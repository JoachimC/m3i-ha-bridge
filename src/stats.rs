use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::watch;

/// How long after the last received advertisement a reading is considered stale.
pub const STALE_AFTER: Duration = Duration::from_secs(20);

/// The latest reading from every bike heard so far, keyed by bike id.
///
/// This is what the watch channel carries. A single reading would be
/// last-writer-wins: with two bikes in range, packets from one would
/// overwrite the other's before a consumer woke, and every consumer would
/// have to rebuild per-bike state from the stream. A snapshot loses nothing,
/// because each write is a complete picture of the fleet. Bikes are never
/// removed; a bike that stops advertising simply goes stale.
pub type Fleet = BTreeMap<u8, Reading>;

/// A new, empty fleet channel: nothing heard yet.
pub fn fleet_channel() -> (watch::Sender<Arc<Fleet>>, watch::Receiver<Arc<Fleet>>) {
    watch::channel(Arc::new(Fleet::new()))
}

/// Records a reading in the fleet, replacing that bike's previous one.
///
/// `Arc::make_mut` copies the map only while a consumer still holds the
/// previous snapshot, so the common case is an in-place insert.
pub fn record_reading(fleet_tx: &watch::Sender<Arc<Fleet>>, reading: Reading) {
    fleet_tx.send_modify(|fleet| {
        Arc::make_mut(fleet).insert(reading.stats.bike_id, reading);
    });
}

/// The snapshot a consumer starts from, marked as seen.
///
/// The marking is the point. `Receiver::clone` copies the *cloned receiver's*
/// version rather than the channel's current one, so a per-subscriber clone
/// can start arbitrarily far behind. Reading it with a plain `borrow` would
/// leave that backlog unseen, and the loop's first [`next_snapshot`] would
/// return the same value again immediately, notifying a new subscriber twice
/// with identical data.
pub fn current_snapshot(rx: &mut watch::Receiver<Arc<Fleet>>) -> Arc<Fleet> {
    rx.borrow_and_update().clone()
}

/// Waits for the next reason to publish: either new data arrived, or
/// `poll_interval` elapsed, so consumers still see values decay once a reading
/// goes stale. Returns `None` once the producer is gone.
///
/// `borrow_and_update` rather than `borrow` again, for a narrower reason here:
/// `timeout` drops the pending `changed()` future when it fires, so a value
/// landing in that same instant is never marked seen. A plain `borrow` would
/// return it and leave the version outstanding, so the next call would return
/// the identical snapshot without waiting.
pub async fn next_snapshot(
    rx: &mut watch::Receiver<Arc<Fleet>>,
    poll_interval: Duration,
) -> Option<Arc<Fleet>> {
    match tokio::time::timeout(poll_interval, rx.changed()).await {
        Ok(Ok(())) => {}
        Ok(Err(_)) => return None, // the producer is gone
        Err(_) => {}               // nothing new; re-publish so staleness shows
    }
    Some(current_snapshot(rx))
}

/// The bike id as it appears in names and topics: zero-padded to three
/// digits, so `#7` and `#42` sort and align with `#200`.
pub fn bike_id_label(bike_id: u8) -> String {
    format!("{bike_id:03}")
}

/// The display name of one bike, shared by the BLE advertisement and the Home
/// Assistant device so a rider sees the same name in Zwift and on the
/// dashboard.
pub fn bike_display_name(bike_id: u8) -> String {
    format!("Keiser M3i #{}", bike_id_label(bike_id))
}

/// One decoded advertisement: exactly what the bike broadcast.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct KeiserStats {
    pub bike_id: u8,
    pub version: String,
    pub power: u16,
    pub cadence: f32,
    pub heart_rate: f32,
    pub is_paused: bool,
    /// Trip distance in km (only metric bikes are supported).
    pub distance: f32,
    /// Accumulated energy in KCal (only metric bikes are supported).
    pub energy: u16,
    pub minutes: u8,
    pub seconds: u8,
    pub gear: u8,
}

impl KeiserStats {
    pub fn elapsed_seconds(&self) -> u16 {
        self.minutes as u16 * 60 + self.seconds as u16
    }
}

/// A reading as received: the bike's data plus when it arrived, which is what
/// staleness is judged against.
#[derive(Debug, Clone, PartialEq)]
pub struct Reading {
    pub stats: KeiserStats,
    pub received_at: Instant,
}

impl Reading {
    pub fn now(stats: KeiserStats) -> Self {
        Self {
            stats,
            received_at: Instant::now(),
        }
    }

    pub fn is_stale(&self) -> bool {
        self.received_at.elapsed() > STALE_AFTER
    }

    /// The data as it should be published: live metrics zeroed when the
    /// reading is stale or the bike is paused, so consumers never report an
    /// outdated reading as current.
    pub fn sanitized(&self) -> KeiserStats {
        let mut stats = self.stats.clone();
        if self.is_stale() || self.stats.is_paused {
            stats.power = 0;
            stats.cadence = 0.0;
            stats.heart_rate = 0.0;
        }
        stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn live_stats() -> KeiserStats {
        KeiserStats {
            power: 150,
            cadence: 85.0,
            heart_rate: 120.0,
            ..Default::default()
        }
    }

    fn live_reading() -> Reading {
        Reading::now(live_stats())
    }

    fn stale_reading() -> Reading {
        Reading {
            stats: live_stats(),
            received_at: Instant::now() - STALE_AFTER * 2,
        }
    }

    #[test]
    fn given_recent_data_when_sanitized_then_metrics_are_kept() {
        let stats = live_reading().sanitized();
        assert_eq!(stats.power, 150);
        assert_eq!(stats.cadence, 85.0);
        assert_eq!(stats.heart_rate, 120.0);
    }

    #[test]
    fn given_a_stale_reading_when_sanitized_then_metrics_are_zeroed() {
        let stats = stale_reading().sanitized();
        assert_eq!(stats.power, 0);
        assert_eq!(stats.cadence, 0.0);
        assert_eq!(stats.heart_rate, 0.0);
    }

    #[test]
    fn given_paused_bike_when_sanitized_then_metrics_are_zeroed() {
        let mut reading = live_reading();
        reading.stats.is_paused = true;
        let stats = reading.sanitized();
        assert_eq!(stats.power, 0);
        assert_eq!(stats.cadence, 0.0);
        assert_eq!(stats.heart_rate, 0.0);
    }

    #[test]
    fn given_a_stale_reading_when_sanitized_then_identity_and_totals_survive() {
        // Staleness zeroes the live metrics; it does not forget which bike or
        // how far it went.
        let mut reading = stale_reading();
        reading.stats.bike_id = 42;
        reading.stats.distance = 4.5;
        let stats = reading.sanitized();
        assert_eq!(stats.bike_id, 42);
        assert_eq!(stats.distance, 4.5);
    }

    #[test]
    fn given_two_bikes_when_both_are_recorded_then_neither_overwrites_the_other() {
        // The point of carrying a snapshot: with one reading on the channel,
        // bike 2's packet would have replaced bike 1's before a consumer woke.
        let (tx, rx) = fleet_channel();
        let mut bike_1 = live_stats();
        bike_1.bike_id = 1;
        let mut bike_2 = live_stats();
        bike_2.bike_id = 2;
        bike_2.power = 200;

        record_reading(&tx, Reading::now(bike_1));
        record_reading(&tx, Reading::now(bike_2));

        let fleet = rx.borrow();
        assert_eq!(fleet.len(), 2);
        assert_eq!(fleet[&1].stats.power, 150);
        assert_eq!(fleet[&2].stats.power, 200);
    }

    #[test]
    fn given_a_bike_heard_again_when_recorded_then_its_reading_is_replaced() {
        let (tx, rx) = fleet_channel();
        let mut first = live_stats();
        first.bike_id = 7;
        let mut second = first.clone();
        second.power = 160;

        record_reading(&tx, Reading::now(first));
        record_reading(&tx, Reading::now(second));

        assert_eq!(rx.borrow().len(), 1);
        assert_eq!(rx.borrow()[&7].stats.power, 160);
    }

    #[test]
    fn given_a_consumer_holding_a_snapshot_when_a_reading_is_recorded_then_its_copy_is_unchanged() {
        // `Arc::make_mut` copies on write, so a consumer mid-tick never sees
        // the map change under it.
        let (tx, mut rx) = fleet_channel();
        let mut bike = live_stats();
        bike.bike_id = 1;
        record_reading(&tx, Reading::now(bike.clone()));
        let held = current_snapshot(&mut rx);

        bike.power = 999;
        record_reading(&tx, Reading::now(bike));

        assert_eq!(held[&1].stats.power, 150);
        assert_eq!(rx.borrow()[&1].stats.power, 999);
    }

    #[test]
    fn given_a_bike_id_when_labelled_then_it_is_zero_padded_to_three_digits() {
        assert_eq!(bike_id_label(0), "000");
        assert_eq!(bike_id_label(7), "007");
        assert_eq!(bike_id_label(42), "042");
        assert_eq!(bike_id_label(200), "200");
    }

    #[test]
    fn given_a_bike_id_when_named_then_the_name_carries_the_padded_id() {
        assert_eq!(bike_display_name(42), "Keiser M3i #042");
    }

    const POLL: Duration = Duration::from_secs(1);

    fn fleet_with(power: u16) -> Arc<Fleet> {
        Arc::new(Fleet::from([(
            0,
            Reading::now(KeiserStats {
                power,
                ..live_stats()
            }),
        )]))
    }

    fn power_of(fleet: &Fleet) -> u16 {
        fleet[&0].stats.power
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_new_subscriber_when_it_reads_then_waits_then_the_snapshot_is_not_repeated() {
        // `Receiver::clone` copies the cloned receiver's version, and the
        // receiver the GATT application is built from never observes
        // anything, so every subscriber's clone starts behind. With a plain
        // `borrow` the first `next_snapshot` returns the same value instantly
        // and the client is notified twice on subscribe.
        let (fleet_tx, build_time_rx) = fleet_channel();
        fleet_tx.send(fleet_with(150)).unwrap();
        let mut rx = build_time_rx.clone();

        assert_eq!(power_of(&current_snapshot(&mut rx)), 150);

        let start = tokio::time::Instant::now();
        let next = next_snapshot(&mut rx, POLL).await.unwrap();
        assert_eq!(power_of(&next), 150);
        assert_eq!(
            start.elapsed(),
            POLL,
            "an already-seen snapshot must wait a full poll interval, not fire at once"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_new_data_when_waiting_for_the_next_snapshot_then_it_arrives_without_delay() {
        let (fleet_tx, mut rx) = fleet_channel();
        let _seen = current_snapshot(&mut rx);
        fleet_tx.send(fleet_with(200)).unwrap();

        let start = tokio::time::Instant::now();
        let next = next_snapshot(&mut rx, POLL).await.unwrap();

        assert_eq!(power_of(&next), 200);
        assert_eq!(start.elapsed(), Duration::ZERO, "new data is not delayed");
    }

    #[tokio::test(start_paused = true)]
    async fn given_no_new_data_when_waiting_for_the_next_snapshot_then_the_poll_interval_elapses() {
        // The re-publish that lets consumers watch a reading decay to zero once
        // it goes stale, rather than sitting on the last live value forever.
        let (_fleet_tx, mut rx) = watch::channel(fleet_with(150));
        let _seen = current_snapshot(&mut rx);

        let start = tokio::time::Instant::now();
        let next = next_snapshot(&mut rx, POLL).await.unwrap();

        assert_eq!(power_of(&next), 150);
        assert_eq!(start.elapsed(), POLL);
    }

    #[tokio::test(start_paused = true)]
    async fn given_the_producer_is_dropped_when_waiting_for_the_next_snapshot_then_none_is_returned()
     {
        let (fleet_tx, mut rx) = watch::channel(fleet_with(150));
        let _seen = current_snapshot(&mut rx);
        drop(fleet_tx);

        assert!(next_snapshot(&mut rx, POLL).await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_snapshot_that_arrived_unobserved_when_it_is_read_then_it_is_not_redelivered() {
        // The state left behind when `timeout` wins the race and drops the
        // pending `changed()`: a value is outstanding, and the loop reads it
        // without `changed()` ever having marked it seen. `mark_changed`
        // reproduces that without having to lose the race on purpose.
        let (_fleet_tx, mut rx) = watch::channel(fleet_with(150));
        rx.mark_changed();

        assert_eq!(power_of(&current_snapshot(&mut rx)), 150);

        let start = tokio::time::Instant::now();
        assert_eq!(power_of(&next_snapshot(&mut rx, POLL).await.unwrap()), 150);
        assert_eq!(
            start.elapsed(),
            POLL,
            "an outstanding snapshot must be consumed by the read, not published twice"
        );
    }

    #[test]
    fn given_minutes_and_seconds_when_elapsed_seconds_then_total_is_returned() {
        let mut stats = live_stats();
        stats.minutes = 2;
        stats.seconds = 30;
        assert_eq!(stats.elapsed_seconds(), 150);
    }
}
