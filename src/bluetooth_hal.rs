//! Platform-independent half of the Bluetooth reader.
//!
//! Scanning itself is platform-specific — bluer on Linux, btleplug elsewhere —
//! but everything above the raw advertisement lives here, so it is written once
//! and tested on every platform.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

use futures_util::stream::{Stream, StreamExt};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::BoxError;
use crate::keiser::{KEISER_MANUFACTURER_ID, parse_keiser_data};
use crate::run_status::RunStatus;
use crate::stats::{BikeId, Fleet, KeiserStats, Reading, record_reading};
use std::sync::Arc;

/// One received advertisement, reduced to the only part this bridge reads.
#[derive(Debug, Clone)]
pub struct Advertisement {
    /// Address or id of the sender. For logging only — nothing branches on it.
    pub device: String,
    pub manufacturer_data: HashMap<u16, Vec<u8>>,
}

#[derive(Debug)]
pub enum ScanEvent {
    Advertisement(Advertisement),
    /// The scan cannot continue. The caller tears down and `bridge_loop`
    /// decides whether to retry.
    Error(BoxError),
}

pub type ScanStream = Pin<Box<dyn Stream<Item = ScanEvent> + Send>>;

/// Something that yields BLE advertisements.
///
/// The boundary is deliberately *raw manufacturer data* rather than parsed
/// `KeiserStats`. Pushing the Keiser id match, the parse, the bike-id filter
/// and the logging down into the platform implementations would duplicate the
/// only interesting logic in the crate across a `cfg` split that CI can compile
/// just one half of. Kept here, all of it is covered by tests that run
/// everywhere.
pub trait BleScanner {
    /// Written as a desugared RPITIT rather than `async fn` so the `Send` bound
    /// on the returned future is explicit.
    fn scan(
        &self,
        cancel_token: CancellationToken,
    ) -> impl Future<Output = Result<ScanStream, BoxError>> + Send;
}

/// Reads advertisements until the scan ends, publishing each Keiser reading.
pub async fn run_bridge<S: BleScanner>(
    scanner: &S,
    cancel_token: CancellationToken,
    fleet_tx: watch::Sender<Arc<Fleet>>,
    bike_id_filter: Option<BikeId>,
) -> Result<RunStatus, BoxError> {
    let mut scan = scanner.scan(cancel_token.clone()).await?;

    let mut advertisement_count: u64 = 0;
    while let Some(event) = scan.next().await {
        match event {
            ScanEvent::Advertisement(advertisement) => {
                advertisement_count += 1;
                if advertisement_count.is_multiple_of(100) || advertisement_count < 10 {
                    tracing::trace!(
                        "Received advertisement {}: {:?}",
                        advertisement_count,
                        &advertisement
                    );
                }
                handle_advertisement(&advertisement, &fleet_tx, bike_id_filter);
            }
            ScanEvent::Error(e) => {
                tracing::error!("Bridge error: {}", e);
                return Err(e);
            }
        }
    }

    // Cancellation is "the stream ended while the token is cancelled", which is
    // why there is no dedicated Cancelled event: a scanner signals it by
    // finishing, and every scanner has to handle the token anyway.
    if cancel_token.is_cancelled() {
        tracing::info!("Bridge task cancelled.");
        Ok(RunStatus::Cancelled)
    } else {
        Ok(RunStatus::StreamEnded)
    }
}

/// Stamps the arrival time here, at the boundary with the radio: the parser
/// knows nothing about time, and nothing downstream can tell a live
/// advertisement from bytes replayed out of a cache, so this must only ever
/// be fed freshly received data.
fn handle_advertisement(
    advertisement: &Advertisement,
    fleet_tx: &watch::Sender<Arc<Fleet>>,
    bike_id_filter: Option<BikeId>,
) {
    if let Some(stats) = keiser_stats_from(&advertisement.manufacturer_data, bike_id_filter) {
        log_bike_update(&advertisement.device, &stats);
        record_reading(fleet_tx, Reading::now(stats));
    }
}

/// Picks this bike's reading out of an advertisement's manufacturer-data map.
fn keiser_stats_from(
    manufacturer_data: &HashMap<u16, Vec<u8>>,
    bike_id_filter: Option<BikeId>,
) -> Option<KeiserStats> {
    let data = manufacturer_data.get(&KEISER_MANUFACTURER_ID)?;

    let Some(stats) = parse_keiser_data(data) else {
        // parse_keiser_data already warns about old firmware and imperial
        // units; this covers the length and data-slot rejections, which used
        // to vanish silently. Debug level, so a malformed beacon arriving at
        // 2 Hz cannot flood the journal.
        tracing::debug!(payload = ?data, "unparseable Keiser advertisement");
        return None;
    };

    if let Some(wanted) = bike_id_filter
        && stats.bike_id != wanted
    {
        tracing::debug!(
            seen = %stats.bike_id,
            wanted = %wanted,
            "ignoring an advertisement from another bike"
        );
        return None;
    }

    Some(stats)
}

fn log_bike_update(device: &str, stats: &KeiserStats) {
    let bike_id = stats.bike_id;
    let status = if stats.is_paused { "PAUSED" } else { "LIVE" };

    tracing::info!(
        target: "bike_stats",
        device,
        bike_id = bike_id.0,
        status,
        power = stats.power,
        cadence = %stats.cadence,
        heart_rate = %stats.heart_rate,
        gear = stats.gear,
        distance = %stats.distance,
        energy = stats.energy,
        "Bike {} Update: {}W, {}RPM, Gear {}",
        bike_id.0, stats.power, stats.cadence, stats.gear
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stats::Tenths;
    use hex_literal::hex;

    /// A real live-data capture from `doc/sample-data.md`, bike id 0.
    const LIVE_CAPTURE: [u8; 17] = hex!("0624000034030000340002000100028008");

    fn manufacturer_data(manufacturer_id: u16, data: &[u8]) -> HashMap<u16, Vec<u8>> {
        HashMap::from([(manufacturer_id, data.to_vec())])
    }

    fn advertisement(manufacturer_id: u16, data: &[u8]) -> Advertisement {
        Advertisement {
            device: "00:11:22:33:44:55".to_string(),
            manufacturer_data: manufacturer_data(manufacturer_id, data),
        }
    }

    /// Replays a canned list of events, so `run_bridge` itself is testable on
    /// every platform without a Bluetooth stack. This is the payoff of putting
    /// the trait boundary at raw advertisements rather than at parsed stats.
    struct FakeScanner {
        events: std::sync::Mutex<Option<Vec<ScanEvent>>>,
    }

    impl FakeScanner {
        fn new(events: Vec<ScanEvent>) -> Self {
            Self {
                events: std::sync::Mutex::new(Some(events)),
            }
        }
    }

    impl BleScanner for FakeScanner {
        fn scan(
            &self,
            _cancel_token: CancellationToken,
        ) -> impl Future<Output = Result<ScanStream, BoxError>> + Send {
            let events = self.events.lock().unwrap().take().expect("scanned once");
            async move { Ok(Box::pin(futures_util::stream::iter(events)) as ScanStream) }
        }
    }

    #[tokio::test]
    async fn given_a_keiser_advertisement_when_the_bridge_runs_then_the_reading_is_published() {
        let scanner = FakeScanner::new(vec![ScanEvent::Advertisement(advertisement(
            KEISER_MANUFACTURER_ID,
            &LIVE_CAPTURE,
        ))]);
        let (stats_tx, stats_rx) = crate::stats::fleet_channel();

        let status = run_bridge(&scanner, CancellationToken::new(), stats_tx, None)
            .await
            .expect("a clean stream end is not an error");

        assert_eq!(status, RunStatus::StreamEnded);
        let fleet = stats_rx.borrow();
        assert_eq!(fleet[&BikeId(0)].stats.cadence, Tenths(820));
        assert!(!fleet[&BikeId(0)].is_stale(), "stamped on arrival");
    }

    #[tokio::test]
    async fn given_a_foreign_advertisement_when_the_bridge_runs_then_nothing_is_published() {
        let scanner = FakeScanner::new(vec![ScanEvent::Advertisement(advertisement(
            0x004C,
            &LIVE_CAPTURE,
        ))]);
        let (stats_tx, stats_rx) = crate::stats::fleet_channel();

        run_bridge(&scanner, CancellationToken::new(), stats_tx, None)
            .await
            .unwrap();

        assert!(
            stats_rx.borrow().is_empty(),
            "the channel must be untouched"
        );
    }

    #[tokio::test]
    async fn given_a_scan_error_when_the_bridge_runs_then_it_fails_so_the_loop_retries() {
        let scanner = FakeScanner::new(vec![ScanEvent::Error("adapter went away".into())]);
        let (stats_tx, _stats_rx) = crate::stats::fleet_channel();

        let error = run_bridge(&scanner, CancellationToken::new(), stats_tx, None)
            .await
            .expect_err("a scan error must reach bridge_loop");

        assert!(error.to_string().contains("adapter went away"));
    }

    #[tokio::test]
    async fn given_a_cancelled_token_when_the_scan_ends_then_the_run_reports_cancellation() {
        // How every scanner signals cancellation now: end the stream. Reporting
        // StreamEnded here instead would make bridge_loop retry during shutdown.
        let scanner = FakeScanner::new(Vec::new());
        let (stats_tx, _stats_rx) = crate::stats::fleet_channel();
        let cancel_token = CancellationToken::new();
        cancel_token.cancel();

        let status = run_bridge(&scanner, cancel_token, stats_tx, None)
            .await
            .unwrap();

        assert_eq!(status, RunStatus::Cancelled);
    }

    #[tokio::test]
    async fn given_an_uncancelled_token_when_the_scan_ends_then_the_run_reports_a_stream_end() {
        let scanner = FakeScanner::new(Vec::new());
        let (stats_tx, _stats_rx) = crate::stats::fleet_channel();

        let status = run_bridge(&scanner, CancellationToken::new(), stats_tx, None)
            .await
            .unwrap();

        assert_eq!(status, RunStatus::StreamEnded);
    }

    #[tokio::test]
    async fn given_a_bike_id_filter_when_another_bike_advertises_then_the_bridge_ignores_it() {
        let scanner = FakeScanner::new(vec![ScanEvent::Advertisement(advertisement(
            KEISER_MANUFACTURER_ID,
            &capture_for_bike(9),
        ))]);
        let (stats_tx, stats_rx) = crate::stats::fleet_channel();

        run_bridge(
            &scanner,
            CancellationToken::new(),
            stats_tx,
            Some(BikeId(7)),
        )
        .await
        .unwrap();

        assert!(stats_rx.borrow().is_empty());
    }

    fn capture_for_bike(bike_id: u8) -> [u8; 17] {
        let mut data = LIVE_CAPTURE;
        data[3] = bike_id;
        data
    }

    #[test]
    fn given_the_keiser_manufacturer_id_when_read_then_it_is_the_sig_assigned_value() {
        // 0x0102 is Keiser Corporation in the Bluetooth SIG company_identifiers
        // registry. Pinned because widening this back out is what issue #16 was
        // about: 0x0201 is AR Timing, 0x01AA Geophysical Technology, 0x015E
        // Unikey Technologies.
        assert_eq!(KEISER_MANUFACTURER_ID, 0x0102);
    }

    #[tokio::test]
    async fn given_a_bridge_attempt_ends_when_its_sender_clone_drops_then_the_channel_stays_open() {
        // `bridge_loop` calls `run_bridge` again after every failure, handing
        // each attempt its own clone of the sender. That is only sound because
        // a watch channel closes when the *last* sender drops, not the first —
        // the property the removed `Arc` used to provide. Moving the sender
        // into `run_bridge` instead of cloning would leave every restart
        // publishing into a closed channel, with both consumers already gone.
        let (stats_tx, mut stats_rx) = crate::stats::fleet_channel();

        let attempt = stats_tx.clone();
        drop(attempt);

        let stats = KeiserStats {
            power: 42,
            ..Default::default()
        };
        record_reading(&stats_tx, Reading::now(stats));
        assert!(
            stats_rx.changed().await.is_ok(),
            "the channel must outlive one attempt"
        );
        assert_eq!(stats_rx.borrow()[&BikeId(0)].stats.power, 42);
    }

    #[tokio::test]
    async fn given_a_replayed_payload_when_handled_then_it_is_stamped_as_freshly_received() {
        // Nothing downstream can tell a live advertisement from bytes the
        // Bluetooth stack replayed out of its cache: whatever reaches
        // handle_advertisement is stamped as received now. Any code path that
        // reads cached manufacturer data (BlueZ `properties()`, bluer's
        // `device.manufacturer_data()`) would silently reset the staleness
        // clock, and must not feed it.
        let (stats_tx, stats_rx) = crate::stats::fleet_channel();
        handle_advertisement(
            &advertisement(KEISER_MANUFACTURER_ID, &LIVE_CAPTURE),
            &stats_tx,
            None,
        );
        assert!(
            !stats_rx.borrow()[&BikeId(0)].is_stale(),
            "a handled packet always looks fresh, however old the bytes are"
        );
    }

    #[test]
    fn given_a_keiser_advertisement_when_filtered_then_the_reading_is_returned() {
        let stats = keiser_stats_from(
            &manufacturer_data(KEISER_MANUFACTURER_ID, &LIVE_CAPTURE),
            None,
        )
        .expect("a valid Keiser packet should parse");
        assert_eq!(stats.bike_id, BikeId(0));
        assert_eq!(stats.cadence, Tenths(820));
    }

    #[test]
    fn given_a_foreign_manufacturer_id_when_filtered_then_nothing_is_returned() {
        // The three ids removed by issue #16. A device advertising a
        // structurally plausible payload under any of them must no longer be
        // decoded as a bike — 0x0201 in particular can never occur, because
        // BlueZ decodes the on-air `02 01` prefix to 0x0102 and strips it.
        for manufacturer_id in [0x0201, 0x01AA, 0x015E] {
            assert!(
                keiser_stats_from(&manufacturer_data(manufacturer_id, &LIVE_CAPTURE), None)
                    .is_none(),
                "id {manufacturer_id:#06X} should be ignored"
            );
        }
    }

    #[test]
    fn given_an_advertisement_with_no_keiser_data_when_filtered_then_nothing_is_returned() {
        assert!(keiser_stats_from(&HashMap::new(), None).is_none());
    }

    #[test]
    fn given_an_unparseable_keiser_payload_when_filtered_then_nothing_is_returned() {
        let truncated = &LIVE_CAPTURE[..10];
        assert!(
            keiser_stats_from(&manufacturer_data(KEISER_MANUFACTURER_ID, truncated), None)
                .is_none()
        );
    }

    #[test]
    fn given_no_bike_id_filter_when_any_bike_advertises_then_it_is_accepted() {
        for bike_id in [0, 1, 200] {
            let data = capture_for_bike(bike_id);
            let stats = keiser_stats_from(&manufacturer_data(KEISER_MANUFACTURER_ID, &data), None)
                .expect("every bike is accepted when unfiltered");
            assert_eq!(stats.bike_id, BikeId(bike_id));
        }
    }

    #[test]
    fn given_a_bike_id_filter_when_the_matching_bike_advertises_then_it_is_accepted() {
        let data = capture_for_bike(7);
        let stats = keiser_stats_from(
            &manufacturer_data(KEISER_MANUFACTURER_ID, &data),
            Some(BikeId(7)),
        )
        .expect("the requested bike should be accepted");
        assert_eq!(stats.bike_id, BikeId(7));
    }

    #[test]
    fn given_a_bike_id_filter_when_another_bike_advertises_then_it_is_ignored() {
        let data = capture_for_bike(9);
        assert!(
            keiser_stats_from(
                &manufacturer_data(KEISER_MANUFACTURER_ID, &data),
                Some(BikeId(7))
            )
            .is_none()
        );
    }

    #[test]
    fn given_a_bike_id_filter_of_zero_when_another_bike_advertises_then_it_is_ignored() {
        // Zero is a real ordinal id — it is the deployed bike's — so filtering
        // on 0 must filter, not mean "unset".
        let data = capture_for_bike(1);
        assert!(
            keiser_stats_from(
                &manufacturer_data(KEISER_MANUFACTURER_ID, &data),
                Some(BikeId(0))
            )
            .is_none()
        );
        let data = capture_for_bike(0);
        assert!(
            keiser_stats_from(
                &manufacturer_data(KEISER_MANUFACTURER_ID, &data),
                Some(BikeId(0))
            )
            .is_some()
        );
    }
}
