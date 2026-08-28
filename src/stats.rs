use std::time::Duration;

use tokio::sync::watch;

/// How long after the last received advertisement the data is considered stale.
pub const STALE_AFTER: Duration = Duration::from_secs(20);

/// The reading a consumer starts from, marked as seen.
///
/// The marking is the point. `Receiver::clone` copies the *cloned receiver's*
/// version rather than the channel's current one, and the receiver the GATT
/// application is built from never observes anything — so every per-subscriber
/// clone starts arbitrarily far behind. Reading it with a plain `borrow` would
/// leave that backlog unseen, and the loop's first [`next_reading`] would
/// return the same value again immediately, notifying every new subscriber
/// twice with identical data.
pub fn current_reading(rx: &mut watch::Receiver<KeiserStats>) -> KeiserStats {
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
/// the identical reading without waiting.
pub async fn next_reading(
    rx: &mut watch::Receiver<KeiserStats>,
    poll_interval: Duration,
) -> Option<KeiserStats> {
    match tokio::time::timeout(poll_interval, rx.changed()).await {
        Ok(Ok(())) => {}
        Ok(Err(_)) => return None, // the producer is gone
        Err(_) => {}               // nothing new; re-publish so staleness shows
    }
    Some(current_reading(rx))
}

#[derive(Debug, Clone, Default)]
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
    pub last_updated: Option<std::time::Instant>,
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

impl KeiserStats {
    /// The id of the bike this reading came from, or `None` for the channel's
    /// initial value before any packet has been received.
    ///
    /// Zero is a real ordinal id, so "not known yet" cannot be represented in
    /// the field itself; it is the absence of a receive timestamp that says no
    /// packet has ever been parsed.
    pub fn bike_id(&self) -> Option<u8> {
        self.last_updated.map(|_| self.bike_id)
    }

    pub fn is_stale(&self) -> bool {
        self.last_updated.is_none_or(|t| t.elapsed() > STALE_AFTER)
    }

    /// Zeroes the live metrics when the data is stale or the bike is paused,
    /// so consumers never publish outdated readings as current.
    pub fn sanitized(mut self) -> Self {
        if self.is_stale() || self.is_paused {
            self.power = 0;
            self.cadence = 0.0;
            self.heart_rate = 0.0;
        }
        self
    }

    pub fn elapsed_seconds(&self) -> u16 {
        self.minutes as u16 * 60 + self.seconds as u16
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
            last_updated: Some(std::time::Instant::now()),
            ..Default::default()
        }
    }

    #[test]
    fn given_recent_data_when_sanitized_then_metrics_are_kept() {
        let stats = live_stats().sanitized();
        assert_eq!(stats.power, 150);
        assert_eq!(stats.cadence, 85.0);
        assert_eq!(stats.heart_rate, 120.0);
    }

    #[test]
    fn given_no_update_timestamp_when_sanitized_then_metrics_are_zeroed() {
        let mut stats = live_stats();
        stats.last_updated = None;
        let stats = stats.sanitized();
        assert_eq!(stats.power, 0);
        assert_eq!(stats.cadence, 0.0);
        assert_eq!(stats.heart_rate, 0.0);
    }

    #[test]
    fn given_paused_bike_when_sanitized_then_metrics_are_zeroed() {
        let mut stats = live_stats();
        stats.is_paused = true;
        let stats = stats.sanitized();
        assert_eq!(stats.power, 0);
        assert_eq!(stats.cadence, 0.0);
        assert_eq!(stats.heart_rate, 0.0);
    }

    const POLL: Duration = Duration::from_secs(1);

    fn reading(power: u16) -> KeiserStats {
        KeiserStats {
            power,
            ..live_stats()
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_new_subscriber_when_it_reads_then_waits_then_the_reading_is_not_repeated() {
        // The GATT case from issue #17. `Receiver::clone` copies the cloned
        // receiver's version, and the receiver the GATT application is built
        // from never observes anything, so every subscriber's clone starts
        // behind. With a plain `borrow` the first `next_reading` returns the
        // same value instantly and the client is notified twice on subscribe.
        let (stats_tx, build_time_rx) = watch::channel(KeiserStats::default());
        stats_tx.send(reading(150)).unwrap();
        let mut rx = build_time_rx.clone();

        assert_eq!(current_reading(&mut rx).power, 150);

        let start = tokio::time::Instant::now();
        let next = next_reading(&mut rx, POLL).await.unwrap();
        assert_eq!(next.power, 150);
        assert_eq!(
            start.elapsed(),
            POLL,
            "an already-seen reading must wait a full poll interval, not fire at once"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_new_data_when_waiting_for_the_next_reading_then_it_arrives_without_delay() {
        let (stats_tx, mut stats_rx) = watch::channel(KeiserStats::default());
        let _seen = current_reading(&mut stats_rx);
        stats_tx.send(reading(200)).unwrap();

        let start = tokio::time::Instant::now();
        let next = next_reading(&mut stats_rx, POLL).await.unwrap();

        assert_eq!(next.power, 200);
        assert_eq!(start.elapsed(), Duration::ZERO, "new data is not delayed");
    }

    #[tokio::test(start_paused = true)]
    async fn given_no_new_data_when_waiting_for_the_next_reading_then_the_poll_interval_elapses() {
        // The re-publish that lets consumers watch a reading decay to zero once
        // it goes stale, rather than sitting on the last live value forever.
        let (_stats_tx, mut stats_rx) = watch::channel(reading(150));
        let _seen = current_reading(&mut stats_rx);

        let start = tokio::time::Instant::now();
        let next = next_reading(&mut stats_rx, POLL).await.unwrap();

        assert_eq!(next.power, 150);
        assert_eq!(start.elapsed(), POLL);
    }

    #[tokio::test(start_paused = true)]
    async fn given_the_producer_is_dropped_when_waiting_for_the_next_reading_then_none_is_returned()
    {
        let (stats_tx, mut stats_rx) = watch::channel(reading(150));
        let _seen = current_reading(&mut stats_rx);
        drop(stats_tx);

        assert!(next_reading(&mut stats_rx, POLL).await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_reading_that_arrived_unobserved_when_it_is_read_then_it_is_not_redelivered() {
        // The state left behind when `timeout` wins the race and drops the
        // pending `changed()`: a value is outstanding, and the loop reads it
        // without `changed()` ever having marked it seen. `mark_changed`
        // reproduces that without having to lose the race on purpose.
        let (_stats_tx, mut stats_rx) = watch::channel(reading(150));
        stats_rx.mark_changed();

        assert_eq!(current_reading(&mut stats_rx).power, 150);

        let start = tokio::time::Instant::now();
        assert_eq!(next_reading(&mut stats_rx, POLL).await.unwrap().power, 150);
        assert_eq!(
            start.elapsed(),
            POLL,
            "an outstanding reading must be consumed by the read, not published twice"
        );
    }

    #[test]
    fn given_no_packet_received_when_the_bike_id_is_read_then_it_is_unknown() {
        // The channel's initial value has bike_id 0, which is a real id, so a
        // consumer reading the field directly would announce bike #000 before
        // any bike had been heard.
        assert_eq!(KeiserStats::default().bike_id(), None);
    }

    #[test]
    fn given_a_received_packet_when_the_bike_id_is_read_then_it_is_known() {
        let stats = KeiserStats {
            bike_id: 0,
            ..live_stats()
        };
        assert_eq!(stats.bike_id(), Some(0), "zero is a real id once received");
    }

    #[test]
    fn given_a_stale_reading_when_the_bike_id_is_read_then_it_is_still_known() {
        // Staleness zeroes the live metrics; it does not forget which bike.
        let stats = KeiserStats {
            bike_id: 42,
            last_updated: Some(std::time::Instant::now() - STALE_AFTER * 2),
            ..Default::default()
        };
        assert_eq!(stats.bike_id(), Some(42));
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

    #[test]
    fn given_minutes_and_seconds_when_elapsed_seconds_then_total_is_returned() {
        let mut stats = live_stats();
        stats.minutes = 2;
        stats.seconds = 30;
        assert_eq!(stats.elapsed_seconds(), 150);
    }
}
