//! Payload serialization for the standard BLE GATT characteristics the bridge
//! exposes: FTMS Indoor Bike Data, Cycling Power Measurement and Heart Rate
//! Measurement.
//!
//! Kept free of any BlueZ/bluer dependency so the wire formats can be
//! unit-tested on every platform, even though the GATT server itself only
//! runs on Linux.

use crate::stats::KeiserStats;

/// Name the bridge advertises itself under.
pub const LOCAL_NAME: &str = "Keiser M3i BLE";

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
    fn given_the_advertised_services_when_sized_then_the_payload_fits_a_legacy_advertisement() {
        // Flags (3) + "Keiser M3i BLE" (2 + 14) + two 16-bit UUIDs (2 + 4) = 25.
        let size = legacy_advertising_size(LOCAL_NAME, 2);
        assert_eq!(size, 25);
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
        assert!(legacy_advertising_size(LOCAL_NAME, 3) <= LEGACY_ADVERTISING_CAPACITY);
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
        assert_eq!(legacy_advertising_size(LOCAL_NAME, 0), 19);
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
