//! Advertising policy: which bike the bridge advertises as, when it switches,
//! what the advertisement contains, and whether it fits.
//!
//! Kept free of any BlueZ/bluer dependency so all of it is unit-tested on
//! every platform; `gatt_server` supplies the [`Advertiser`] that talks to
//! BlueZ.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::BoxError;
use crate::stats::{BikeId, Fleet};

/// The 16-bit service UUIDs listed in the advertising packet.
///
/// FTMS (0x1826) has to be here, not merely discoverable after connecting:
/// many clients — Zwift's FTMS pairing screen among them — filter discovery
/// on the advertised UUID, so a trainer that omits it is simply never
/// offered, and the pairing flow never gets far enough to read the service
/// list. FTMS v1.0 §3.1 requires it for that reason. Heart Rate (0x180D) is
/// advertised for the same reason: Zwift's HR pairing screen filters on it
/// identically. The characteristic only notifies while the bike reports a
/// rate, so with no strap the sensor is offered but silent — the usual case
/// here is a strap being worn.
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

/// `btmgmt add-adv` arguments for the same advertisement, for the fallback
/// used when D-Bus registration fails. Derived from
/// [`ADVERTISED_SERVICE_UUIDS`] so the two can never list different services
/// — a divergence would only ever show up on a Pi that is having a bad day.
/// `-n` advertises the adapter's alias as the local name.
pub fn btmgmt_add_adv_args() -> Vec<String> {
    let mut args = vec!["add-adv".to_string()];
    for uuid in ADVERTISED_SERVICE_UUIDS {
        args.push("-u".to_string());
        args.push(format!("{uuid:04x}"));
    }
    args.extend(["-c", "-g", "-n", "1"].map(str::to_string));
    args
}

/// A legacy (non-extended) BLE advertising packet carries at most 31 bytes of
/// AD structures. Overrunning it makes BlueZ refuse to register the
/// advertisement outright, so the budget is worth checking rather than
/// assuming.
pub const LEGACY_ADVERTISING_CAPACITY: usize = 31;

/// Size in bytes of a legacy advertising payload carrying Flags, a Complete
/// Local Name and the advertised 16-bit Service UUIDs.
///
/// Each AD structure costs a length byte and a type byte on top of its value
/// (Core Spec CSS Part A §1); BlueZ contributes the 3-byte Flags structure
/// itself for a discoverable advertisement. This is a conservative worst case:
/// BlueZ may move the local name into the scan response, which would free 16
/// bytes here.
pub fn legacy_advertising_size(local_name: &str, uuid16_count: usize) -> usize {
    ad_structure_size(1) + ad_structure_size(local_name.len()) + ad_structure_size(2 * uuid16_count)
}

/// An empty AD structure is omitted entirely rather than emitted with a
/// zero-length value.
fn ad_structure_size(value_len: usize) -> usize {
    const AD_HEADER: usize = 2; // length byte + type byte
    if value_len == 0 {
        0
    } else {
        AD_HEADER + value_len
    }
}

/// How long the latest bike id has to stay the same before the advertisement
/// is re-registered under it.
///
/// Re-registering is not free: BlueZ tears the old advertisement down, and
/// clients mid-pairing lose the device. In a room where two bikes alternate
/// packets, the "latest" id flips every couple of seconds; the hold means the
/// name only changes when a different bike has genuinely taken over.
pub const ADVERTISED_ID_HOLD: Duration = Duration::from_secs(10);

/// How recently a candidate bike must have been heard when the hold elapses.
/// The M3i advertises every ~2 s while it is pedalled, so a bike that has
/// genuinely taken over is heard well inside this window; one that sent a
/// single packet and fell silent is not.
pub const CANDIDATE_RECENCY: Duration = Duration::from_secs(5);

/// Decides which bike id the advertisement should carry.
///
/// The first bike heard is advertised at once — before it there is nothing to
/// advertise. After that, a different id replaces the advertised one only when
/// it has been heard for at least [`ADVERTISED_ID_HOLD`] and is still being
/// heard (within [`CANDIDATE_RECENCY`]) — a bike that has taken over, rather
/// than one that sent a stray packet.
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

    /// The id the advertisement should switch to now, if any. Calling this
    /// commits the switch, so the caller must go on to register it.
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

    /// When the current candidate's hold elapses, so the caller can sleep
    /// until then rather than poll. `None` when nothing is pending.
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

/// Lets only readings that actually arrived through to the advertising
/// tracker.
///
/// The watch channel re-delivers its current snapshot on every poll timeout so
/// consumers can watch a reading go stale. For the tracker that is poison: a
/// single packet from a neighbouring bike, re-delivered once a second while
/// the advertised bike's rider coasts, would look like that bike being
/// present for the whole hold and steal the advertisement. A bike counts as
/// sighted only when its reading's receive timestamp has changed.
#[derive(Debug, Default)]
pub struct NewArrivals {
    seen: BTreeMap<BikeId, std::time::Instant>,
}

impl NewArrivals {
    /// The bikes whose reading in `fleet` has not been seen before.
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

/// Something that can put one advertisement on the air at a time.
///
/// Written as desugared RPITITs rather than `async fn` so the `Send` bounds
/// on the returned futures are explicit.
pub trait Advertiser {
    /// Registers an advertisement under `name`. Called only while nothing is
    /// advertised: [`track_advertised_bike`] stops the previous one first.
    fn advertise(&mut self, name: &str) -> impl Future<Output = Result<(), BoxError>> + Send;

    /// Unregisters the current advertisement, if any, and returns once the
    /// stack is ready for the next registration.
    fn stop(&mut self) -> impl Future<Output = ()> + Send;
}

/// Advertises as the bike the fleet says is being ridden, until cancelled.
///
/// Nothing is advertised until a bike has been heard; the name then carries
/// that bike's id. `advertised` is updated only after a registration
/// succeeds, so the DIS serial number and the notify loops never name a bike
/// that is not on the air. A registration that fails even via the fallback
/// is returned as an error: it does not heal in-process.
pub async fn track_advertised_bike<A: Advertiser>(
    advertiser: &mut A,
    mut fleet_rx: watch::Receiver<Arc<Fleet>>,
    advertised: &watch::Sender<Option<BikeId>>,
    cancel_token: CancellationToken,
) -> Result<(), BoxError> {
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
                    // The producer is gone; keep serving what is registered
                    // until shutdown.
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
        // Clients filter discovery on the advertised UUID, so a service that
        // is only discoverable after connecting is never offered.
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
        // Nothing else is advertised today, but knowing one more would fit is
        // what makes that a choice rather than a constraint.
        assert!(
            legacy_advertising_size(&BikeId(200).display_name(), 4) <= LEGACY_ADVERTISING_CAPACITY
        );
    }

    #[test]
    fn given_a_name_that_fills_the_packet_when_sized_then_the_limit_is_exceeded() {
        // Guards the guard: a sizing function that never reports an overrun
        // would pass the tests above while proving nothing.
        let long_name = "K".repeat(28);
        assert!(legacy_advertising_size(&long_name, 2) > LEGACY_ADVERTISING_CAPACITY);
    }

    #[test]
    fn given_no_advertised_services_when_sized_then_the_uuid_structure_costs_nothing() {
        assert_eq!(legacy_advertising_size(&BikeId(200).display_name(), 0), 20);
    }

    #[tokio::test(start_paused = true)]
    async fn given_no_advertisement_yet_when_the_first_bike_is_heard_then_it_is_due_at_once() {
        // Nothing is advertised until a bike is heard, so there is nothing to
        // protect from thrashing: the first id goes out immediately.
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
        // Bike 7 sent one packet and fell silent. It was "the latest" for the
        // whole hold, but it has not taken over — the rider on bike 1 is just
        // coasting.
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
        // The genuine switch: bike 7 keeps advertising every 2 s while bike 1
        // is silent.
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
        // The multi-bike room: packets from bike 1 and bike 2 interleave every
        // couple of seconds. Bike 2 never holds the "latest" slot for the full
        // hold, so the advertisement stays on bike 1 rather than thrashing.
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
        // Seeing the advertised bike again resets the clock: the next time a
        // different bike appears it has to earn the full hold from scratch.
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
        // Bike 42 is advertised, its rider stops pedalling, one packet from
        // bike 7 arrives, and the channel re-delivers the same snapshot once a
        // second for longer than the hold.
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

    /// Records what the loop asked for, and can be told to fail.
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
            let advertiser = FakeAdvertiser::default();
            let (fleet_tx, fleet_rx) = fleet_channel();
            let (advertised_tx, advertised_rx) = watch::channel(None);
            let cancel = CancellationToken::new();
            let mut loop_advertiser = advertiser.clone();
            let loop_cancel = cancel.clone();
            let task = tokio::spawn(async move {
                track_advertised_bike(&mut loop_advertiser, fleet_rx, &advertised_tx, loop_cancel)
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
        // The invariant the hardware cares about: stop, then advertise, in
        // that order, and the advertised id changes only once that succeeded.
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
        // The loop must wake at the candidate's deadline on its own; a bike
        // that is heard right up to the hold and then falls silent for a
        // moment would otherwise wait for the next unrelated packet.
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
    async fn given_a_running_loop_when_cancelled_then_the_advertisement_is_stopped() {
        let harness = Harness::start();
        harness.hear(1).await;
        let advertiser = harness.advertiser.clone();
        harness.finish().await.unwrap();
        assert_eq!(advertiser.events().last().unwrap(), "stop");
    }
}
