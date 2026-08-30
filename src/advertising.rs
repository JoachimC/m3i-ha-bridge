//! Advertising policy: which bike the bridge advertises as, when it switches,
//! what the advertisement contains, and whether it fits.
//!
//! This module has no BlueZ/bluer dependency, so unit tests cover all of it
//! on every platform; `gatt_server` supplies the [`Advertiser`] that
//! communicates with BlueZ.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::BoxError;
use crate::stats::{BikeId, Fleet};

/// The 16-bit service UUIDs in the advertising packet.
///
/// The packet must list FTMS (0x1826); discovery after connection is not
/// sufficient. Many clients, for example Zwift's FTMS pairing screen,
/// filter discovery on the advertised UUID. A trainer that omits it never
/// appears, and the pairing flow never reads the service list. FTMS v1.0
/// §3.1 requires it for this reason. The packet lists Heart Rate (0x180D)
/// for the same reason: Zwift's HR pairing screen filters on it
/// identically. The characteristic only notifies while the bike reports a
/// rate. With no strap, clients see the sensor but it stays silent; a worn
/// strap is the usual case here.
pub const ADVERTISED_SERVICE_UUIDS: [u16; 3] = [
    CYCLING_POWER_SERVICE,
    FITNESS_MACHINE_SERVICE,
    HEART_RATE_SERVICE,
];

pub const CYCLING_POWER_SERVICE: u16 = 0x1818;
pub const FITNESS_MACHINE_SERVICE: u16 = 0x1826;
pub const HEART_RATE_SERVICE: u16 = 0x180D;

/// The full 128-bit form of a Bluetooth SIG 16-bit UUID.
pub const fn sig_uuid(short: u16) -> u128 {
    0x0000_0000_0000_1000_8000_0080_5f9b_34fb | ((short as u128) << 96)
}

/// `btmgmt add-adv` arguments for the same advertisement. The bridge uses
/// them as the fallback when D-Bus registration fails. The function derives
/// them from [`ADVERTISED_SERVICE_UUIDS`], so the two paths always list the
/// same services. `-n` advertises the adapter's alias as the local name.
pub fn btmgmt_add_adv_args() -> Vec<String> {
    let mut args = vec!["add-adv".to_string()];
    for uuid in ADVERTISED_SERVICE_UUIDS {
        args.push("-u".to_string());
        args.push(format!("{uuid:04x}"));
    }
    args.extend(["-c", "-g", "-n", "1"].map(str::to_string));
    args
}

/// A legacy (non-extended) BLE advertising packet carries at most 31 bytes
/// of AD structures. If the payload is larger, BlueZ refuses to register
/// the advertisement. The tests check this budget; they do not assume it.
pub const LEGACY_ADVERTISING_CAPACITY: usize = 31;

/// Size in bytes of a legacy advertising payload carrying Flags, a Complete
/// Local Name and the advertised 16-bit Service UUIDs.
///
/// Each AD structure adds a length byte and a type byte to its value (Core
/// Spec CSS Part A §1); BlueZ contributes the 3-byte Flags structure itself
/// for a discoverable advertisement. This is a conservative worst case:
/// BlueZ can move the local name into the scan response, and that frees 16
/// bytes here.
pub fn legacy_advertising_size(local_name: &str, uuid16_count: usize) -> usize {
    ad_structure_size(1) + ad_structure_size(local_name.len()) + ad_structure_size(2 * uuid16_count)
}

/// An empty AD structure has size zero: the packet omits it and does not
/// contain a zero-length structure.
fn ad_structure_size(value_len: usize) -> usize {
    const AD_HEADER: usize = 2; // length byte + type byte
    if value_len == 0 {
        0
    } else {
        AD_HEADER + value_len
    }
}

/// How long the latest bike id must stay the same before the bridge
/// re-registers the advertisement under it.
///
/// Re-registration has a cost: BlueZ removes the old advertisement, and
/// clients that are mid-pairing lose the device. In a room where two bikes
/// alternate packets, the "latest" id changes every few seconds. The hold
/// makes sure that the name only changes when a different bike has really
/// replaced the current one.
pub const ADVERTISED_ID_HOLD: Duration = Duration::from_secs(10);

/// How recent the last packet from a candidate bike must be when the hold
/// elapses. The M3i advertises every ~2 s while a rider pedals it, so the
/// bridge hears a genuine replacement well inside this window. A bike that
/// sent one packet and then stopped is outside it.
pub const CANDIDATE_RECENCY: Duration = Duration::from_secs(5);

/// Decides which bike id the advertisement carries.
///
/// The tracker accepts the first bike immediately; before it, there is
/// nothing to advertise. After that, a different id replaces the advertised
/// one only when the bridge heard it for at least [`ADVERTISED_ID_HOLD`]
/// and still hears it (within [`CANDIDATE_RECENCY`]). That is a bike that
/// replaced the current one, not a bike that sent a stray packet.
#[derive(Debug, Default)]
pub struct AdvertisedIdTracker {
    advertised: Option<BikeId>,
    candidate: Option<Candidate>,
}

#[derive(Debug, Clone, Copy)]
struct Candidate {
    bike_id: BikeId,
    first_seen: Instant,
    last_seen: Instant,
}

impl AdvertisedIdTracker {
    /// Records a sighting of a bike.
    pub fn observe(&mut self, bike_id: BikeId, now: Instant) {
        if self.advertised == Some(bike_id) {
            self.candidate = None;
            return;
        }
        match &mut self.candidate {
            Some(candidate) if candidate.bike_id == bike_id => candidate.last_seen = now,
            _ => {
                self.candidate = Some(Candidate {
                    bike_id,
                    first_seen: now,
                    last_seen: now,
                })
            }
        }
    }

    /// The id the advertisement must switch to now, if any. A call commits
    /// the switch, so the caller must then register it.
    #[must_use = "a due switch is committed; register it"]
    pub fn take_due(&mut self, now: Instant) -> Option<BikeId> {
        let candidate = self.candidate?;
        let due = self.advertised.is_none()
            || (now.duration_since(candidate.first_seen) >= ADVERTISED_ID_HOLD
                && now.duration_since(candidate.last_seen) <= CANDIDATE_RECENCY);
        if !due {
            return None;
        }
        self.candidate = None;
        self.advertised = Some(candidate.bike_id);
        Some(candidate.bike_id)
    }

    /// The time when the current candidate's hold elapses. The caller can
    /// sleep until then instead of polling. `None` when nothing is pending.
    pub fn next_deadline(&self) -> Option<Instant> {
        let candidate = self.candidate?;
        Some(if self.advertised.is_none() {
            candidate.first_seen
        } else {
            candidate.first_seen + ADVERTISED_ID_HOLD
        })
    }

    pub fn advertised(&self) -> Option<BikeId> {
        self.advertised
    }
}

/// Passes only readings that really arrived to the advertising tracker.
///
/// The watch channel delivers its current snapshot again on every poll
/// timeout, so consumers can watch a reading become stale. That behaviour
/// is dangerous for the tracker. A single packet from a nearby bike can
/// arrive again once a second while the advertised bike's rider coasts.
/// The tracker would then see that bike as present for the whole hold, and
/// the bike would take the advertisement. A bike counts as sighted only
/// when its reading's receive timestamp changes.
#[derive(Debug, Default)]
pub struct NewArrivals {
    seen: BTreeMap<BikeId, std::time::Instant>,
}

impl NewArrivals {
    /// The bikes whose reading in `fleet` is new to this filter.
    pub fn new_in(&mut self, fleet: &Fleet) -> Vec<BikeId> {
        fleet
            .iter()
            .filter(|(bike_id, reading)| {
                self.seen.insert(**bike_id, reading.received_at) != Some(reading.received_at)
            })
            .map(|(bike_id, _)| *bike_id)
            .collect()
    }
}

/// Something that can broadcast one advertisement at a time.
///
/// The trait uses desugared RPITITs instead of `async fn`, so the `Send`
/// bounds on the returned futures are explicit.
pub trait Advertiser {
    /// Registers an advertisement under `name`. The caller calls this only
    /// while no advertisement is active: [`track_advertised_bike`] stops the
    /// previous one first.
    fn advertise(&mut self, name: &str) -> impl Future<Output = Result<(), BoxError>> + Send;

    /// Unregisters the current advertisement, if any, and returns once the
    /// stack is ready for the next registration.
    fn stop(&mut self) -> impl Future<Output = ()> + Send;
}

/// Advertises as the bike that the fleet reports as ridden, until
/// cancellation.
///
/// The loop advertises nothing until it hears a bike; the name then carries
/// that bike's id. The loop updates `advertised` only after a registration
/// succeeds, so the DIS serial number and the notify loops never name a
/// bike that is not advertised. A registration that fails, also through the
/// fallback, returns an error: the condition does not recover in this
/// process.
///
/// With `locked_to` set, this bridge instance serves one bike only (the
/// one-bridge-per-bike studio setup). The loop advertises that name
/// immediately, so a rider who pairs before pedalling finds the trainer,
/// and nothing ever changes the name.
pub async fn track_advertised_bike<A: Advertiser>(
    advertiser: &mut A,
    mut fleet_rx: watch::Receiver<Arc<Fleet>>,
    advertised: &watch::Sender<Option<BikeId>>,
    cancel_token: CancellationToken,
    locked_to: Option<BikeId>,
) -> Result<(), BoxError> {
    if let Some(bike_id) = locked_to {
        let name = bike_id.display_name();
        tracing::info!("Locked to bike {bike_id}: advertising as {name:?} from the start");
        let result = advertiser.advertise(&name).await;
        if result.is_ok() {
            let _ = advertised.send(Some(bike_id));
            tracing::info!("BLE broadcasting active as {:?}", name);
            cancel_token.cancelled().await;
        }
        advertiser.stop().await;
        return result;
    }

    let mut tracker = AdvertisedIdTracker::default();
    let mut arrivals = NewArrivals::default();

    for bike_id in arrivals.new_in(&fleet_rx.borrow_and_update()) {
        tracker.observe(bike_id, Instant::now());
    }
    tracing::info!("Waiting for a bike before advertising...");

    let result = loop {
        let was_advertising = tracker.advertised().is_some();
        if let Some(bike_id) = tracker.take_due(Instant::now()) {
            let name = bike_id.display_name();
            if was_advertising {
                tracing::info!("Re-advertising as {:?}", name);
                advertiser.stop().await;
            } else {
                tracing::info!("Advertising as {:?}", name);
            }
            if let Err(e) = advertiser.advertise(&name).await {
                break Err(e);
            }
            let _ = advertised.send(Some(bike_id));
            tracing::info!("BLE broadcasting active as {:?}", name);
        }

        let deadline = tracker.next_deadline();
        tokio::select! {
            _ = cancel_token.cancelled() => break Ok(()),
            changed = fleet_rx.changed() => {
                if changed.is_err() {
                    // The producer is gone. Keep the current advertisement
                    // active until shutdown.
                    cancel_token.cancelled().await;
                    break Ok(());
                }
                let now = Instant::now();
                for bike_id in arrivals.new_in(&fleet_rx.borrow_and_update()) {
                    tracker.observe(bike_id, now);
                }
            }
            _ = tokio::time::sleep_until(deadline.unwrap_or_else(Instant::now)), if deadline.is_some() => {}
        }
    };

    advertiser.stop().await;
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stats::{KeiserStats, Reading, fleet_channel, record_reading};
    use std::sync::Mutex;

    fn reading_from(bike_id: u8, power: u16) -> Reading {
        Reading::now(KeiserStats {
            bike_id: BikeId(bike_id),
            power,
            ..Default::default()
        })
    }

    fn fleet_of(readings: impl IntoIterator<Item = Reading>) -> Fleet {
        readings
            .into_iter()
            .map(|reading| (reading.stats.bike_id, reading))
            .collect()
    }

    #[test]
    fn given_the_advertised_services_when_listed_then_every_pairing_screen_filter_is_present() {
        // Clients filter discovery on the advertised UUID, so they never
        // offer a service that is only discoverable after connection.
        assert!(ADVERTISED_SERVICE_UUIDS.contains(&FITNESS_MACHINE_SERVICE));
        assert!(ADVERTISED_SERVICE_UUIDS.contains(&CYCLING_POWER_SERVICE));
        assert!(ADVERTISED_SERVICE_UUIDS.contains(&HEART_RATE_SERVICE));
    }

    #[test]
    fn given_a_short_uuid_when_expanded_then_it_is_the_sig_base_form() {
        assert_eq!(
            sig_uuid(0x1826),
            0x0000_1826_0000_1000_8000_0080_5f9b_34fb,
            "Fitness Machine"
        );
    }

    #[test]
    fn given_the_btmgmt_fallback_when_built_then_it_lists_the_same_services() {
        assert_eq!(
            btmgmt_add_adv_args(),
            [
                "add-adv", "-u", "1818", "-u", "1826", "-u", "180d", "-c", "-g", "-n", "1"
            ]
        );
    }

    #[test]
    fn given_the_advertised_services_when_sized_then_the_payload_fits_a_legacy_advertisement() {
        // Flags (3) + "Keiser M3i #200" (2 + 15) + three 16-bit UUIDs (2 + 6)
        // = 28. The widest id is three digits, so 200 is the worst case.
        let size =
            legacy_advertising_size(&BikeId(200).display_name(), ADVERTISED_SERVICE_UUIDS.len());
        assert_eq!(size, 28);
        assert!(
            size <= LEGACY_ADVERTISING_CAPACITY,
            "{size} bytes exceeds the {LEGACY_ADVERTISING_CAPACITY}-byte legacy limit; \
             BlueZ would refuse to register the advertisement"
        );
    }

    #[test]
    fn given_a_fourth_advertised_service_when_sized_then_there_is_still_headroom() {
        // The packet lists no other service, but one more service fits.
        // That makes the current list a choice, not a constraint.
        assert!(
            legacy_advertising_size(&BikeId(200).display_name(), 4) <= LEGACY_ADVERTISING_CAPACITY
        );
    }

    #[test]
    fn given_a_name_that_fills_the_packet_when_sized_then_the_limit_is_exceeded() {
        // This test checks the check: a sizing function that never reports
        // an overrun would pass the tests above and prove nothing.
        let long_name = "K".repeat(28);
        assert!(legacy_advertising_size(&long_name, 2) > LEGACY_ADVERTISING_CAPACITY);
    }

    #[test]
    fn given_no_advertised_services_when_sized_then_the_uuid_structure_costs_nothing() {
        assert_eq!(legacy_advertising_size(&BikeId(200).display_name(), 0), 20);
    }

    #[tokio::test(start_paused = true)]
    async fn given_no_advertisement_yet_when_the_first_bike_is_heard_then_it_is_due_at_once() {
        // The bridge advertises nothing until it hears a bike, so there is
        // no unwanted switch to prevent: the first id is due immediately.
        let mut tracker = AdvertisedIdTracker::default();
        let now = Instant::now();
        tracker.observe(BikeId(42), now);
        assert_eq!(tracker.take_due(now), Some(BikeId(42)));
        assert_eq!(tracker.advertised(), Some(BikeId(42)));
        assert_eq!(tracker.take_due(now), None, "committed, not repeated");
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_different_bike_when_it_has_only_just_appeared_then_nothing_is_due() {
        let mut tracker = AdvertisedIdTracker::default();
        let start = Instant::now();
        tracker.observe(BikeId(1), start);
        let _ = tracker.take_due(start);

        tracker.observe(BikeId(2), start);
        assert_eq!(tracker.take_due(start), None);
        assert_eq!(tracker.next_deadline(), Some(start + ADVERTISED_ID_HOLD));
        tokio::time::advance(ADVERTISED_ID_HOLD / 2).await;
        assert_eq!(tracker.take_due(Instant::now()), None);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_lone_packet_from_another_bike_when_the_hold_elapses_then_nothing_is_due() {
        // Bike 7 sent one packet and then stopped. It was "the latest" for
        // the whole hold, but it did not replace bike 1: that rider only
        // coasts.
        let mut tracker = AdvertisedIdTracker::default();
        tracker.observe(BikeId(1), Instant::now());
        let _ = tracker.take_due(Instant::now());

        tracker.observe(BikeId(7), Instant::now());
        tokio::time::advance(ADVERTISED_ID_HOLD + Duration::from_secs(1)).await;
        assert_eq!(tracker.take_due(Instant::now()), None);
        assert_eq!(tracker.advertised(), Some(BikeId(1)));
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_bike_heard_throughout_the_hold_when_it_elapses_then_it_takes_over() {
        // The genuine switch: bike 7 advertises every 2 s while bike 1 is
        // silent.
        let mut tracker = AdvertisedIdTracker::default();
        tracker.observe(BikeId(1), Instant::now());
        let _ = tracker.take_due(Instant::now());

        for _ in 0..6 {
            tracker.observe(BikeId(7), Instant::now());
            tokio::time::advance(Duration::from_secs(2)).await;
        }
        assert_eq!(tracker.take_due(Instant::now()), Some(BikeId(7)));
    }

    #[tokio::test(start_paused = true)]
    async fn given_two_bikes_alternating_when_the_hold_elapses_then_the_name_does_not_flip() {
        // The multi-bike room: packets from bike 1 and bike 2 interleave
        // every few seconds. Bike 2 never holds the "latest" slot for the
        // full hold, so the advertisement stays on bike 1 and does not
        // oscillate.
        let mut tracker = AdvertisedIdTracker::default();
        tracker.observe(BikeId(1), Instant::now());
        let _ = tracker.take_due(Instant::now());

        for _ in 0..10 {
            tracker.observe(BikeId(2), Instant::now());
            tokio::time::advance(Duration::from_secs(2)).await;
            assert_eq!(tracker.take_due(Instant::now()), None);
            tracker.observe(BikeId(1), Instant::now());
            tokio::time::advance(Duration::from_secs(2)).await;
            assert_eq!(tracker.take_due(Instant::now()), None);
        }
        assert_eq!(tracker.advertised(), Some(BikeId(1)));
    }

    #[tokio::test(start_paused = true)]
    async fn given_the_advertised_bike_returns_when_a_candidate_was_pending_then_it_is_dropped() {
        // A new sighting of the advertised bike resets the clock: the next
        // different bike must complete the full hold again.
        let mut tracker = AdvertisedIdTracker::default();
        tracker.observe(BikeId(1), Instant::now());
        let _ = tracker.take_due(Instant::now());

        tracker.observe(BikeId(2), Instant::now());
        tokio::time::advance(ADVERTISED_ID_HOLD - Duration::from_secs(1)).await;
        tracker.observe(BikeId(1), Instant::now());
        assert_eq!(tracker.next_deadline(), None);
        tokio::time::advance(Duration::from_secs(2)).await;
        tracker.observe(BikeId(2), Instant::now());
        assert_eq!(
            tracker.take_due(Instant::now()),
            None,
            "the earlier sighting of bike 2 must not count"
        );
    }

    #[test]
    fn given_an_empty_fleet_when_filtered_then_nothing_is_sighted() {
        let mut arrivals = NewArrivals::default();
        assert!(arrivals.new_in(&Fleet::new()).is_empty());
    }

    #[test]
    fn given_a_snapshot_delivered_again_when_filtered_then_each_reading_counts_once() {
        let mut arrivals = NewArrivals::default();
        let fleet = fleet_of([reading_from(7, 50)]);
        assert_eq!(arrivals.new_in(&fleet), [BikeId(7)]);
        assert!(arrivals.new_in(&fleet).is_empty());
    }

    #[test]
    fn given_a_fresh_reading_for_one_bike_when_filtered_then_only_that_bike_is_sighted() {
        let mut arrivals = NewArrivals::default();
        let bike_42 = reading_from(42, 150);
        let bike_7 = reading_from(7, 50);
        assert_eq!(
            arrivals.new_in(&fleet_of([bike_42.clone(), bike_7.clone()])),
            [BikeId(7), BikeId(42)]
        );

        let bike_42_again = Reading {
            received_at: std::time::Instant::now() + Duration::from_secs(2),
            ..reading_from(42, 160)
        };
        assert_eq!(
            arrivals.new_in(&fleet_of([bike_42_again, bike_7])),
            [BikeId(42)]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_stray_packet_while_the_rider_coasts_when_the_hold_elapses_then_the_advertisement_stays()
     {
        // The bridge advertises bike 42, and its rider stops pedalling. One
        // packet from bike 7 arrives, and the channel delivers the same
        // snapshot again once a second for longer than the hold.
        let mut arrivals = NewArrivals::default();
        let mut tracker = AdvertisedIdTracker::default();
        let bike_42 = reading_from(42, 150);
        for id in arrivals.new_in(&fleet_of([bike_42.clone()])) {
            tracker.observe(id, Instant::now());
        }
        let _ = tracker.take_due(Instant::now());

        let fleet = fleet_of([bike_42, reading_from(7, 50)]);
        for _ in 0..15 {
            for id in arrivals.new_in(&fleet) {
                tracker.observe(id, Instant::now());
            }
            tokio::time::advance(Duration::from_secs(1)).await;
            assert_eq!(tracker.take_due(Instant::now()), None);
        }
        assert_eq!(tracker.advertised(), Some(BikeId(42)));
    }

    /// Records what the loop requested; a test can make the next call fail.
    #[derive(Clone, Default)]
    struct FakeAdvertiser {
        events: Arc<Mutex<Vec<String>>>,
        fail_next: Arc<Mutex<bool>>,
    }

    impl FakeAdvertiser {
        fn events(&self) -> Vec<String> {
            self.events.lock().unwrap().clone()
        }
    }

    impl Advertiser for FakeAdvertiser {
        fn advertise(&mut self, name: &str) -> impl Future<Output = Result<(), BoxError>> + Send {
            let events = self.events.clone();
            let fail = std::mem::take(&mut *self.fail_next.lock().unwrap());
            let name = name.to_string();
            async move {
                events.lock().unwrap().push(format!("advertise {name}"));
                if fail {
                    Err("BlueZ said no".into())
                } else {
                    Ok(())
                }
            }
        }

        fn stop(&mut self) -> impl Future<Output = ()> + Send {
            let events = self.events.clone();
            async move { events.lock().unwrap().push("stop".to_string()) }
        }
    }

    /// Runs the loop in the background against a fake advertiser.
    struct Harness {
        advertiser: FakeAdvertiser,
        fleet_tx: watch::Sender<Arc<Fleet>>,
        advertised_rx: watch::Receiver<Option<BikeId>>,
        cancel: CancellationToken,
        task: tokio::task::JoinHandle<Result<(), BoxError>>,
    }

    impl Harness {
        fn start() -> Self {
            Self::start_with(None)
        }

        fn start_with(locked_to: Option<BikeId>) -> Self {
            let advertiser = FakeAdvertiser::default();
            let (fleet_tx, fleet_rx) = fleet_channel();
            let (advertised_tx, advertised_rx) = watch::channel(None);
            let cancel = CancellationToken::new();
            let mut loop_advertiser = advertiser.clone();
            let loop_cancel = cancel.clone();
            let task = tokio::spawn(async move {
                track_advertised_bike(
                    &mut loop_advertiser,
                    fleet_rx,
                    &advertised_tx,
                    loop_cancel,
                    locked_to,
                )
                .await
            });
            Self {
                advertiser,
                fleet_tx,
                advertised_rx,
                cancel,
                task,
            }
        }

        async fn hear(&self, bike_id: u8) {
            record_reading(&self.fleet_tx, reading_from(bike_id, 100));
            tokio::task::yield_now().await;
        }

        async fn advance(&self, secs: u64) {
            tokio::time::advance(Duration::from_secs(secs)).await;
            tokio::task::yield_now().await;
        }

        async fn finish(self) -> Result<(), BoxError> {
            self.cancel.cancel();
            self.task.await.unwrap()
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_no_bike_heard_when_time_passes_then_nothing_is_advertised() {
        let harness = Harness::start();
        harness.advance(60).await;
        assert!(harness.advertiser.events().is_empty());
        assert_eq!(*harness.advertised_rx.borrow(), None);
        harness.finish().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn given_the_first_bike_when_heard_then_it_is_advertised_at_once() {
        let harness = Harness::start();
        harness.hear(42).await;
        assert_eq!(harness.advertiser.events(), ["advertise Keiser M3i #042"]);
        assert_eq!(*harness.advertised_rx.borrow(), Some(BikeId(42)));
        assert_eq!(
            harness.finish().await.map_err(|e| e.to_string()),
            Ok(()),
            "cancellation is a clean exit"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_another_bike_ridden_through_the_hold_when_it_elapses_then_the_loop_switches() {
        // The important sequence for the hardware: stop, then advertise, in
        // that order. The advertised id changes only after that succeeds.
        let harness = Harness::start();
        harness.hear(1).await;
        for _ in 0..6 {
            harness.hear(2).await;
            harness.advance(2).await;
        }
        assert_eq!(
            harness.advertiser.events(),
            [
                "advertise Keiser M3i #001",
                "stop",
                "advertise Keiser M3i #002",
            ]
        );
        assert_eq!(*harness.advertised_rx.borrow(), Some(BikeId(2)));
        harness.finish().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_candidate_pending_when_no_packet_arrives_then_the_hold_is_still_evaluated() {
        // The loop must wake at the candidate's deadline by itself. Without
        // that, a bike that the bridge hears until almost the full hold, and
        // that then pauses for a moment, waits for the next unrelated packet.
        let harness = Harness::start();
        harness.hear(1).await;
        harness.hear(2).await;
        harness.advance(8).await;
        harness.hear(2).await;
        harness.advance(3).await; // deadline passes with no further packet
        assert_eq!(*harness.advertised_rx.borrow(), Some(BikeId(2)));
        harness.finish().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_registration_that_fails_when_switching_then_the_loop_fails_and_the_id_is_kept()
    {
        let harness = Harness::start();
        harness.hear(1).await;
        *harness.advertiser.fail_next.lock().unwrap() = true;
        for _ in 0..6 {
            harness.hear(2).await;
            harness.advance(2).await;
        }
        assert_eq!(
            *harness.advertised_rx.borrow(),
            Some(BikeId(1)),
            "never names a bike that is not on the air"
        );
        let result = harness.task.await.unwrap();
        assert!(result.is_err(), "fail fast: the process restarts");
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_locked_bike_when_the_loop_starts_then_it_advertises_before_any_packet() {
        // The dedicated-bridge case: the configuration supplies the name, so
        // a rider who pairs before pedalling must already find the trainer.
        let harness = Harness::start_with(Some(BikeId(42)));
        harness.advance(1).await;
        assert_eq!(harness.advertiser.events(), ["advertise Keiser M3i #042"]);
        assert_eq!(*harness.advertised_rx.borrow(), Some(BikeId(42)));
        harness.finish().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_locked_bike_when_another_bike_is_ridden_for_the_hold_then_nothing_switches() {
        // The reader's filter already drops other bikes. This test shows
        // that even a reading that passed the filter cannot move the
        // advertisement.
        let harness = Harness::start_with(Some(BikeId(1)));
        for _ in 0..6 {
            harness.hear(2).await;
            harness.advance(2).await;
        }
        assert_eq!(harness.advertiser.events(), ["advertise Keiser M3i #001"]);
        assert_eq!(*harness.advertised_rx.borrow(), Some(BikeId(1)));
        harness.finish().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_locked_bike_when_its_registration_fails_then_the_loop_fails_at_once() {
        let harness = Harness::start_with(Some(BikeId(1)));
        *harness.advertiser.fail_next.lock().unwrap() = true;
        harness.advance(1).await;
        assert_eq!(*harness.advertised_rx.borrow(), None);
        assert!(harness.task.await.unwrap().is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_running_loop_when_cancelled_then_the_advertisement_is_stopped() {
        let harness = Harness::start();
        harness.hear(1).await;
        let advertiser = harness.advertiser.clone();
        harness.finish().await.unwrap();
        assert_eq!(advertiser.events().last().unwrap(), "stop");
    }
}
