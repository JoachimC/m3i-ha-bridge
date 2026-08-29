//! Payload serialization for the standard BLE GATT characteristics the bridge
//! exposes: FTMS Indoor Bike Data, Cycling Power Measurement and Heart Rate
//! Measurement.
//!
//! Kept free of any BlueZ/bluer dependency so the wire formats can be
//! unit-tested on every platform, even though the GATT server itself only
//! runs on Linux.

use std::time::Duration;

use tokio::time::Instant;

use crate::stats::{KeiserStats, bike_display_name, bike_id_label};

/// Name the bridge advertises itself under: the bike's display name, so a
/// pairing screen in Zwift or Garmin shows which bike this is.
pub fn local_name(bike_id: u8) -> String {
    bike_display_name(bike_id)
}

/// The Device Information Service's Serial Number String for a bike: the
/// zero-padded id, or empty while no bike has been heard.
pub fn serial_number(bike_id: Option<u8>) -> String {
    bike_id.map(bike_id_label).unwrap_or_default()
}

/// How long the latest bike id has to stay the same before the advertisement
/// is re-registered under it.
///
/// Re-registering is not free: BlueZ tears the old advertisement down, and
/// clients mid-pairing lose the device. In a room where two bikes alternate
/// packets, the "latest" id flips every couple of seconds; the hold means the
/// name only changes when a different bike has genuinely taken over.
pub const ADVERTISED_ID_HOLD: Duration = Duration::from_secs(10);

/// Decides which bike id the advertisement should carry.
///
/// The first bike heard is advertised at once — before it there is nothing to
/// advertise. After that, a different id has to persist for
/// [`ADVERTISED_ID_HOLD`] before it replaces the advertised one.
#[derive(Debug, Default)]
pub struct AdvertisedIdTracker {
    advertised: Option<u8>,
    /// The most recent id that differs from the advertised one, and when it
    /// was first seen.
    candidate: Option<(u8, Instant)>,
}

impl AdvertisedIdTracker {
    /// Records the latest reading's bike id.
    pub fn observe(&mut self, bike_id: u8, now: Instant) {
        if self.advertised == Some(bike_id) {
            self.candidate = None;
            return;
        }
        match self.candidate {
            Some((id, _)) if id == bike_id => {}
            _ => self.candidate = Some((bike_id, now)),
        }
    }

    /// The id the advertisement should switch to now, if any. Calling this
    /// commits the switch, so the caller must go on to register it.
    pub fn take_due(&mut self, now: Instant) -> Option<u8> {
        let (id, since) = self.candidate?;
        let due = self.advertised.is_none() || now.duration_since(since) >= ADVERTISED_ID_HOLD;
        if !due {
            return None;
        }
        self.candidate = None;
        self.advertised = Some(id);
        Some(id)
    }

    #[cfg(test)]
    pub fn advertised(&self) -> Option<u8> {
        self.advertised
    }
}

/// Picks the reading a notify loop should send: the one from the bike the
/// advertisement names, or nothing.
///
/// The watch channel carries every bike in range, so in a multi-bike room a
/// client paired to "Keiser M3i #042" would otherwise be notified with #007's
/// power interleaved. A reading from another bike neither goes out nor
/// replaces `kept`; the advertised bike's last reading is re-sent instead, so
/// it still decays to zero on staleness rather than freezing at its last live
/// value while another bike holds the channel.
pub fn reading_for_advertised_bike(
    kept: Option<KeiserStats>,
    incoming: KeiserStats,
    advertised: Option<u8>,
) -> Option<KeiserStats> {
    let advertised = advertised?;
    if incoming.bike_id() == Some(advertised) {
        return Some(incoming);
    }
    kept.filter(|kept| kept.bike_id() == Some(advertised))
}

/// A legacy (non-extended) BLE advertising packet carries at most 31 bytes of
/// AD structures. Overrunning it makes BlueZ refuse to register the
/// advertisement outright, so the budget is worth checking rather than
/// assuming.
pub const LEGACY_ADVERTISING_CAPACITY: usize = 31;

/// Size in bytes of a legacy advertising payload carrying Flags, a Complete
/// Local Name and a Complete List of 16-bit Service UUIDs.
///
/// Each AD structure costs a length byte and a type byte on top of its value
/// (Core Spec CSS Part A §1); BlueZ contributes the 3-byte Flags structure
/// itself for a discoverable advertisement. This is a conservative worst case:
/// BlueZ may move the local name into the scan response, which would free 16
/// bytes here.
pub fn legacy_advertising_size(local_name: &str, uuid16_count: usize) -> usize {
    const AD_HEADER: usize = 2; // length byte + type byte
    let flags = AD_HEADER + 1;
    let name = AD_HEADER + local_name.len();
    let uuids = match uuid16_count {
        0 => 0,
        count => AD_HEADER + 2 * count,
    };
    flags + name + uuids
}

/// FTMS Fitness Machine Feature (0x2ACC): Fitness Machine Features (uint32,
/// LSO..MSO) then Target Setting Features (uint32, LSO..MSO) — FTMS v1.0 §4.3,
/// Table 4.2.
///
/// Features = 0x0000_5486: Cadence (bit 1), Total Distance (bit 2), Resistance
/// Level (bit 7), Heart Rate Measurement (bit 10), Elapsed Time (bit 12) and
/// Power Measurement (bit 14) — exactly the optional fields [`serialize_ftms`]
/// sends, per the flag-to-feature mapping in FTMS v1.0 Table 4.10.
///
/// Target Setting Features = 0: this is a read-only bridge with no Fitness
/// Machine Control Point, so no Supported \*Range characteristics are required
/// (Table 4.1, conditions C.1–C.5).
///
/// Lives here rather than beside the characteristic in `gatt_server.rs` because
/// that module is `cfg(target_os = "linux")` — a test next to it would silently
/// never run on a macOS dev machine.
pub const FTMS_FEATURE_VALUE: [u8; 8] = [0x86, 0x54, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];

/// FTMS Indoor Bike Data (0x2AD2).
///
/// Flags 0x0A74: Cadence (bit 2), Total Distance (bit 4), Resistance (bit 5),
/// Power (bit 6), Heart Rate (bit 9) and Elapsed Time (bit 11) present.
/// Bit 0 ("More Data") clear means Instantaneous Speed is also present.
pub fn serialize_ftms(stats: &KeiserStats) -> Vec<u8> {
    let flags: u16 = 0x0A74;
    let mut data = Vec::with_capacity(16);
    data.extend_from_slice(&flags.to_le_bytes());

    // Instantaneous Speed: u16, 0.01 km/h
    let speed_u16 = (calculate_speed_from_power(stats.power) * 100.0) as u16;
    data.extend_from_slice(&speed_u16.to_le_bytes());

    // Instantaneous Cadence: u16, 0.5 RPM
    let cadence_u16 = (stats.cadence * 2.0) as u16;
    data.extend_from_slice(&cadence_u16.to_le_bytes());

    // Total Distance: u24, meters (stats.distance is km)
    let distance_m = (stats.distance * 1000.0) as u32;
    data.extend_from_slice(&distance_m.to_le_bytes()[0..3]);

    // Resistance Level: i16 (the M3i's gear)
    data.extend_from_slice(&(stats.gear as i16).to_le_bytes());

    // Instantaneous Power: i16, Watts
    data.extend_from_slice(&(stats.power as i16).to_le_bytes());

    // Heart Rate: u8, BPM
    data.push(stats.heart_rate as u8);

    // Elapsed Time: u16, seconds
    data.extend_from_slice(&stats.elapsed_seconds().to_le_bytes());

    data
}

/// Cycling Power Measurement (0x2A63).
///
/// Flags 0x0020: Crank Revolution Data present; Instantaneous Power is always
/// present per the spec.
pub fn serialize_cps(stats: &KeiserStats, revolutions: u16, event_time: u16) -> Vec<u8> {
    let flags: u16 = 0x0020;
    let mut data = Vec::with_capacity(8);
    data.extend_from_slice(&flags.to_le_bytes());

    // Instantaneous Power: i16, Watts
    data.extend_from_slice(&(stats.power as i16).to_le_bytes());

    // Cumulative Crank Revolutions: u16 (rolls over)
    data.extend_from_slice(&revolutions.to_le_bytes());

    // Last Crank Event Time: u16, 1/1024 s (rolls over)
    data.extend_from_slice(&event_time.to_le_bytes());

    data
}

/// Heart Rate Measurement (0x2A37). Flags 0x00: u8 value, no extra fields.
pub fn serialize_hrs(stats: &KeiserStats) -> Vec<u8> {
    vec![0x00, stats.heart_rate as u8]
}

/// The first payload to send a client that has just subscribed to a notify
/// characteristic, or `None` when that characteristic has nothing worth
/// reporting yet.
///
/// The order here is the point: the stats are sanitized *before* `has_value`
/// inspects them, not just before serialization. Predicates like `power > 0`
/// evaluated on the raw watch value fire precisely in the stale case — a client
/// subscribing an hour after a ride would be told the last live power, cadence
/// and heart rate, and for Cycling Power that reading would also accrue crank
/// revolutions across the whole idle gap.
///
/// `serialize` is therefore only invoked when something is actually being sent,
/// which keeps the stateful CPS serializer from advancing on a skipped send.
pub fn initial_notification(
    stats: &KeiserStats,
    has_value: fn(&KeiserStats) -> bool,
    serialize: &mut impl FnMut(&KeiserStats) -> Vec<u8>,
) -> Option<Vec<u8>> {
    let stats = stats.clone().sanitized();
    has_value(&stats).then(|| serialize(&stats))
}

/// Whether Indoor Bike Data has anything worth reporting to a new subscriber.
pub fn ftms_has_value(stats: &KeiserStats) -> bool {
    stats.power > 0 || stats.cadence > 0.0
}

/// Whether Cycling Power Measurement has anything worth reporting.
pub fn cps_has_value(stats: &KeiserStats) -> bool {
    stats.power > 0
}

/// Whether Heart Rate Measurement has anything worth reporting.
pub fn hrs_has_value(stats: &KeiserStats) -> bool {
    stats.heart_rate > 0.0
}

/// Converts an accumulating counter to the wrapping u16 the CPS spec expects.
/// A plain `as u16` cast saturates at 65535 (freezing the value — for the
/// crank event time that happens after only 64 seconds at 1024 ticks/s),
/// whereas clients rely on modulo-65536 rollover to compute deltas.
pub fn wrap_u16(value: f64) -> u16 {
    (value.max(0.0) % 65536.0) as u16
}

/// Estimates road speed from power using P = 0.196 * v^3 (v in m/s), a simple
/// flat-road aero model, returning km/h.
pub fn calculate_speed_from_power(power: u16) -> f32 {
    if power == 0 {
        return 0.0;
    }
    let v_ms = (power as f64 / 0.196).powf(1.0 / 3.0);
    (v_ms * 3.6) as f32
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stats() -> KeiserStats {
        KeiserStats {
            power: 150,
            cadence: 85.0,
            heart_rate: 120.0,
            distance: 4.5,
            gear: 12,
            minutes: 2,
            seconds: 5,
            last_updated: Some(std::time::Instant::now()),
            ..Default::default()
        }
    }

    fn le_u16(data: &[u8], offset: usize) -> u16 {
        u16::from_le_bytes([data[offset], data[offset + 1]])
    }

    #[test]
    fn given_stats_when_ftms_is_serialized_then_layout_matches_the_spec() {
        let data = serialize_ftms(&stats());
        assert_eq!(data.len(), 16);
        assert_eq!(le_u16(&data, 0), 0x0A74, "flags");
        assert!(le_u16(&data, 2) > 0, "speed should be non-zero at 150W");
        assert_eq!(le_u16(&data, 4), 170, "cadence in 0.5 RPM units");
        let distance_m = u32::from_le_bytes([data[6], data[7], data[8], 0]);
        assert_eq!(distance_m, 4500, "distance in meters");
        assert_eq!(le_u16(&data, 9), 12, "resistance = gear");
        assert_eq!(le_u16(&data, 11), 150, "power in watts");
        assert_eq!(data[13], 120, "heart rate");
        assert_eq!(le_u16(&data, 14), 125, "elapsed seconds");
    }

    #[test]
    fn given_a_bike_id_when_the_local_name_is_built_then_it_is_the_bikes_display_name() {
        // Issue #6: the one place a rider can tell bikes apart in a pairing
        // list is the advertised name, so it carries the padded id.
        assert_eq!(local_name(42), "Keiser M3i #042");
    }

    #[test]
    fn given_the_advertised_services_when_sized_then_the_payload_fits_a_legacy_advertisement() {
        // Flags (3) + "Keiser M3i #200" (2 + 15) + two 16-bit UUIDs (2 + 4) = 26.
        // The widest id is three digits, so 200 is the worst case.
        let size = legacy_advertising_size(&local_name(200), 2);
        assert_eq!(size, 26);
        assert!(
            size <= LEGACY_ADVERTISING_CAPACITY,
            "{size} bytes exceeds the {LEGACY_ADVERTISING_CAPACITY}-byte legacy limit; \
             BlueZ would refuse to register the advertisement"
        );
    }

    #[test]
    fn given_a_third_advertised_service_when_sized_then_there_is_still_headroom() {
        // Heart Rate is not advertised today, but knowing it would still fit is
        // what makes that a choice rather than a constraint.
        assert!(legacy_advertising_size(&local_name(200), 3) <= LEGACY_ADVERTISING_CAPACITY);
    }

    fn reading_from(bike_id: u8, power: u16) -> KeiserStats {
        KeiserStats {
            bike_id,
            power,
            ..stats()
        }
    }

    #[test]
    fn given_nothing_advertised_when_a_reading_arrives_then_nothing_is_sent() {
        // No advertisement means no client can have paired to a named bike,
        // so there is no bike whose data it would be correct to send.
        assert!(reading_for_advertised_bike(None, reading_from(1, 100), None).is_none());
    }

    #[test]
    fn given_the_advertised_bikes_reading_when_it_arrives_then_it_is_sent() {
        let sent = reading_for_advertised_bike(None, reading_from(42, 150), Some(42)).unwrap();
        assert_eq!(sent.power, 150);
    }

    #[test]
    fn given_another_bikes_reading_when_it_arrives_then_the_advertised_bikes_last_is_resent() {
        // The multi-bike room: a client paired to #042 must never see #007's
        // power, and must keep seeing #042's last reading so the staleness
        // clock (its own last_updated) can zero it in due course.
        let kept = Some(reading_from(42, 150));
        let sent = reading_for_advertised_bike(kept, reading_from(7, 999), Some(42)).unwrap();
        assert_eq!(sent.bike_id, 42);
        assert_eq!(sent.power, 150);
    }

    #[test]
    fn given_another_bikes_reading_and_nothing_kept_when_it_arrives_then_nothing_is_sent() {
        assert!(reading_for_advertised_bike(None, reading_from(7, 999), Some(42)).is_none());
    }

    #[test]
    fn given_the_advertisement_switched_bikes_when_the_old_bikes_reading_is_kept_then_it_is_dropped()
     {
        // After the advertisement moves to #007, #042's remembered reading is
        // the wrong bike too — a #007 client must not be fed #042's tail.
        let kept = Some(reading_from(42, 150));
        assert!(reading_for_advertised_bike(kept, reading_from(3, 50), Some(7)).is_none());
    }

    #[test]
    fn given_the_initial_channel_value_when_it_arrives_then_it_is_not_mistaken_for_bike_zero() {
        // The default reading has bike_id 0 but no timestamp; with #000
        // advertised it still must not be sent as that bike's data.
        assert!(reading_for_advertised_bike(None, KeiserStats::default(), Some(0)).is_none());
    }

    #[test]
    fn given_no_bike_heard_when_the_serial_number_is_read_then_it_is_empty() {
        assert_eq!(serial_number(None), "");
        assert_eq!(serial_number(Some(7)), "007");
    }

    #[tokio::test(start_paused = true)]
    async fn given_no_advertisement_yet_when_the_first_bike_is_heard_then_it_is_due_at_once() {
        // Nothing is advertised until a bike is heard, so there is nothing to
        // protect from thrashing: the first id goes out immediately.
        let mut tracker = AdvertisedIdTracker::default();
        let now = Instant::now();
        tracker.observe(42, now);
        assert_eq!(tracker.take_due(now), Some(42));
        assert_eq!(tracker.advertised(), Some(42));
        assert_eq!(tracker.take_due(now), None, "committed, not repeated");
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_different_bike_when_it_has_only_just_appeared_then_nothing_is_due() {
        let mut tracker = AdvertisedIdTracker::default();
        let start = Instant::now();
        tracker.observe(1, start);
        tracker.take_due(start);

        tracker.observe(2, start);
        assert_eq!(tracker.take_due(start), None);
        tokio::time::advance(ADVERTISED_ID_HOLD / 2).await;
        assert_eq!(tracker.take_due(Instant::now()), None);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_different_bike_when_it_has_persisted_for_the_hold_then_it_is_due() {
        let mut tracker = AdvertisedIdTracker::default();
        let start = Instant::now();
        tracker.observe(1, start);
        tracker.take_due(start);

        tracker.observe(2, start);
        tokio::time::advance(ADVERTISED_ID_HOLD).await;
        tracker.observe(2, Instant::now());
        assert_eq!(tracker.take_due(Instant::now()), Some(2));
        assert_eq!(tracker.advertised(), Some(2));
    }

    #[tokio::test(start_paused = true)]
    async fn given_two_bikes_alternating_when_the_hold_elapses_then_the_name_does_not_flip() {
        // The multi-bike room: packets from bike 1 and bike 2 interleave every
        // couple of seconds. Bike 2 never holds the "latest" slot for the full
        // hold, so the advertisement stays on bike 1 rather than thrashing.
        let mut tracker = AdvertisedIdTracker::default();
        tracker.observe(1, Instant::now());
        tracker.take_due(Instant::now());

        for _ in 0..10 {
            tracker.observe(2, Instant::now());
            tokio::time::advance(Duration::from_secs(2)).await;
            assert_eq!(tracker.take_due(Instant::now()), None);
            tracker.observe(1, Instant::now());
            tokio::time::advance(Duration::from_secs(2)).await;
            assert_eq!(tracker.take_due(Instant::now()), None);
        }
        assert_eq!(tracker.advertised(), Some(1));
    }

    #[tokio::test(start_paused = true)]
    async fn given_the_advertised_bike_returns_when_a_candidate_was_pending_then_it_is_dropped() {
        // Seeing the advertised bike again resets the clock: the next time a
        // different bike appears it has to earn the full hold from scratch.
        let mut tracker = AdvertisedIdTracker::default();
        tracker.observe(1, Instant::now());
        tracker.take_due(Instant::now());

        tracker.observe(2, Instant::now());
        tokio::time::advance(ADVERTISED_ID_HOLD - Duration::from_secs(1)).await;
        tracker.observe(1, Instant::now());
        tokio::time::advance(Duration::from_secs(2)).await;
        tracker.observe(2, Instant::now());
        assert_eq!(
            tracker.take_due(Instant::now()),
            None,
            "the earlier sighting of bike 2 must not count"
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
        // An empty AD structure is omitted entirely rather than emitted with a
        // zero-length value.
        assert_eq!(legacy_advertising_size(&local_name(200), 0), 20);
    }

    #[test]
    fn given_the_ftms_feature_value_when_decoded_then_it_declares_exactly_the_fields_sent() {
        // FTMS v1.0 §4.3, Table 4.2: Fitness Machine Features (uint32,
        // LSO..MSO) followed by Target Setting Features (uint32, LSO..MSO).
        let machine = u32::from_le_bytes(FTMS_FEATURE_VALUE[0..4].try_into().unwrap());
        let targets = u32::from_le_bytes(FTMS_FEATURE_VALUE[4..8].try_into().unwrap());
        assert_eq!(machine, 0x0000_5486, "Fitness Machine Features");
        assert_eq!(
            targets, 0,
            "no Control Point, so no Target Setting Features"
        );

        // FTMS v1.0 §4.9.1, Table 4.10: Indoor Bike Data flag bit -> the
        // Fitness Machine Feature bit that must be set to be allowed to send
        // that field. Bit 0 (More Data) has no corresponding feature bit, which
        // is why sending the mandatory Instantaneous Speed needs none.
        const FLAG_TO_FEATURE: [(u32, u32); 12] = [
            (1, 0),   // Average Speed Present    -> Average Speed Supported
            (2, 1),   // Instantaneous Cadence    -> Cadence Supported
            (3, 1),   // Average Cadence Present  -> Cadence Supported
            (4, 2),   // Total Distance Present   -> Total Distance Supported
            (5, 7),   // Resistance Level Present -> Resistance Level Supported
            (6, 14),  // Instantaneous Power      -> Power Measurement Supported
            (7, 14),  // Average Power Present    -> Power Measurement Supported
            (8, 9),   // Expended Energy Present  -> Expended Energy Supported
            (9, 10),  // Heart Rate Present       -> Heart Rate Measurement Supported
            (10, 11), // Metabolic Equivalent     -> Metabolic Equivalent Supported
            (11, 12), // Elapsed Time Present     -> Elapsed Time Supported
            (12, 13), // Remaining Time Present   -> Remaining Time Supported
        ];

        // Derived from the flags the serializer actually emits rather than
        // restated as a literal, so the two can never drift apart: adding a
        // field to serialize_ftms without declaring it fails here, and so does
        // declaring a feature that is never sent.
        let data = serialize_ftms(&stats());
        let flags = le_u16(&data, 0) as u32;
        let required = FLAG_TO_FEATURE
            .iter()
            .filter(|(flag_bit, _)| flags & (1 << flag_bit) != 0)
            .fold(0u32, |acc, (_, feature_bit)| acc | (1 << feature_bit));

        assert_eq!(
            machine, required,
            "Fitness Machine Features 0x{machine:08X} must declare exactly the optional \
             fields Indoor Bike Data flags 0x{flags:04X} say are present (0x{required:08X})"
        );
    }

    #[test]
    fn given_stats_when_cps_is_serialized_then_layout_matches_the_spec() {
        let data = serialize_cps(&stats(), 1234, 5678);
        assert_eq!(data.len(), 8);
        assert_eq!(le_u16(&data, 0), 0x0020, "flags");
        assert_eq!(le_u16(&data, 2), 150, "power");
        assert_eq!(le_u16(&data, 4), 1234, "crank revolutions");
        assert_eq!(le_u16(&data, 6), 5678, "crank event time");
    }

    #[test]
    fn given_stats_when_hrs_is_serialized_then_layout_matches_the_spec() {
        let data = serialize_hrs(&stats());
        assert_eq!(data, vec![0x00, 120]);
    }

    /// The state a bridge sits in between rides: real values were received,
    /// but long enough ago that they must not be reported as current.
    fn stale_stats() -> KeiserStats {
        KeiserStats {
            last_updated: None,
            ..stats()
        }
    }

    #[test]
    fn given_live_stats_when_the_initial_notification_is_built_then_it_carries_them() {
        let mut serialize = serialize_ftms;
        let payload = initial_notification(&stats(), ftms_has_value, &mut serialize)
            .expect("live stats are worth sending");
        assert_eq!(le_u16(&payload, 11), 150, "power");
        assert_eq!(le_u16(&payload, 4), 170, "cadence in 0.5 RPM units");
    }

    #[test]
    fn given_stale_stats_when_the_initial_notification_is_built_then_nothing_is_sent() {
        // The defect in issue #4: the predicate used to see the raw watch
        // value, so `power > 0` fired *precisely* when the reading was too old
        // to send, and a client subscribing an hour after a ride was handed the
        // last live power, cadence and heart rate.
        let mut serialize = serialize_ftms;
        assert!(initial_notification(&stale_stats(), ftms_has_value, &mut serialize).is_none());
    }

    #[test]
    fn given_a_paused_bike_when_the_initial_notification_is_built_then_nothing_is_sent() {
        let paused = KeiserStats {
            is_paused: true,
            ..stats()
        };
        let mut serialize = serialize_ftms;
        assert!(initial_notification(&paused, ftms_has_value, &mut serialize).is_none());
    }

    #[test]
    fn given_stale_stats_when_the_initial_notification_is_built_then_the_serializer_is_not_called()
    {
        // The Cycling Power serializer is stateful. Calling it on a skipped
        // send would advance cumulative crank revolutions across the whole idle
        // gap, so the decision has to come before serialization, not after.
        let mut calls = 0;
        let mut serialize = |stats: &KeiserStats| {
            calls += 1;
            serialize_cps(stats, 0, 0)
        };
        assert!(initial_notification(&stale_stats(), cps_has_value, &mut serialize).is_none());
        assert_eq!(calls, 0, "nothing to send means nothing to serialize");
    }

    #[test]
    fn given_stale_stats_when_any_characteristic_decides_then_none_reports_a_value() {
        // All three characteristics gate on a live metric, and `sanitized`
        // zeroes exactly those three, so staleness silences every one of them.
        let stale = stale_stats().sanitized();
        assert!(!ftms_has_value(&stale), "Indoor Bike Data");
        assert!(!cps_has_value(&stale), "Cycling Power");
        assert!(!hrs_has_value(&stale), "Heart Rate");
    }

    #[test]
    fn given_live_stats_when_each_characteristic_decides_then_all_report_a_value() {
        let live = stats().sanitized();
        assert!(ftms_has_value(&live), "Indoor Bike Data");
        assert!(cps_has_value(&live), "Cycling Power");
        assert!(hrs_has_value(&live), "Heart Rate");
    }

    #[test]
    fn given_counter_beyond_u16_range_when_wrapped_then_it_rolls_over() {
        assert_eq!(wrap_u16(0.0), 0);
        assert_eq!(wrap_u16(65535.0), 65535);
        assert_eq!(wrap_u16(65536.0), 0);
        assert_eq!(wrap_u16(65536.0 + 100.0), 100);
        // A 30-minute ride at 1024 ticks/s must keep rolling over, not freeze.
        assert_eq!(
            wrap_u16(30.0 * 60.0 * 1024.0),
            ((30_u32 * 60 * 1024) % 65536) as u16
        );
    }

    #[test]
    fn given_zero_power_when_speed_is_calculated_then_it_is_zero() {
        assert_eq!(calculate_speed_from_power(0), 0.0);
    }

    #[test]
    fn given_power_when_speed_is_calculated_then_it_is_plausible() {
        let speed = calculate_speed_from_power(150);
        // ~9.15 m/s => ~33 km/h on the flat-road model
        assert!((32.0..34.0).contains(&speed), "got {speed}");
    }
}
