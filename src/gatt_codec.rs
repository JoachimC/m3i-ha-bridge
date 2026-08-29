//! Payload serialization for the standard BLE GATT characteristics the bridge
//! exposes: FTMS Indoor Bike Data, Cycling Power Measurement and Heart Rate
//! Measurement.
//!
//! Kept free of any BlueZ/bluer dependency so the wire formats can be
//! unit-tested on every platform, even though the GATT server itself only
//! runs on Linux.

use tokio::time::Instant;

use crate::stats::{Sanitized, Tenths};

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

/// Indoor Bike Data flags (FTMS v1.0 §4.9.1): Cadence (bit 2), Total Distance
/// (bit 4), Resistance (bit 5), Power (bit 6), Heart Rate (bit 9) and Elapsed
/// Time (bit 11) present. Bit 0 ("More Data") clear means Instantaneous Speed
/// is also present.
const INDOOR_BIKE_DATA_FLAGS: u16 = 0x0A74;

/// Cycling Power Measurement flags (CPS v1.1 §3.2): Crank Revolution Data
/// present. Instantaneous Power is always present.
const CPS_MEASUREMENT_FLAGS: u16 = 0x0020;

/// Heart Rate Measurement flags (HRS v1.0 §3.1): a `u8` value, no extra fields.
const HRS_MEASUREMENT_FLAGS: u8 = 0x00;

/// FTMS Indoor Bike Data (0x2AD2), fields in the order the flags declare.
pub fn serialize_ftms(stats: &Sanitized) -> Vec<u8> {
    let mut data = Vec::with_capacity(16);
    data.extend_from_slice(&INDOOR_BIKE_DATA_FLAGS.to_le_bytes());
    data.extend_from_slice(&speed_hundredths_kmh(stats.power).to_le_bytes());
    data.extend_from_slice(&cadence_half_rpm(stats.cadence).to_le_bytes());
    data.extend_from_slice(&distance_metres_u24(stats.distance));
    data.extend_from_slice(&resistance_level(stats.gear).to_le_bytes());
    data.extend_from_slice(&power_watts(stats.power).to_le_bytes());
    data.push(heart_rate_bpm(stats.heart_rate));
    data.extend_from_slice(&stats.elapsed_seconds().to_le_bytes());
    data
}

/// Cycling Power Measurement (0x2A63): power, then the crank revolution data
/// the flags declare. Both counters roll over at u16, per spec.
pub fn serialize_cps(stats: &Sanitized, revolutions: u16, event_time: u16) -> Vec<u8> {
    let mut data = Vec::with_capacity(8);
    data.extend_from_slice(&CPS_MEASUREMENT_FLAGS.to_le_bytes());
    data.extend_from_slice(&power_watts(stats.power).to_le_bytes());
    data.extend_from_slice(&revolutions.to_le_bytes());
    data.extend_from_slice(&event_time.to_le_bytes());
    data
}

/// Heart Rate Measurement (0x2A37).
pub fn serialize_hrs(stats: &Sanitized) -> Vec<u8> {
    vec![HRS_MEASUREMENT_FLAGS, heart_rate_bpm(stats.heart_rate)]
}

/// Instantaneous Speed: `u16`, 0.01 km/h.
fn speed_hundredths_kmh(power: u16) -> u16 {
    (calculate_speed_from_power(power) * 100.0) as u16
}

/// Instantaneous Cadence: `u16`, 0.5 rpm — two units per rpm, so tenths / 5.
fn cadence_half_rpm(cadence: Tenths) -> u16 {
    cadence.0 / 5
}

/// Total Distance: `u24`, metres, little-endian — tenths of a km × 100.
fn distance_metres_u24(distance: Tenths) -> [u8; 3] {
    let metres = u32::from(distance.0) * 100;
    let [b0, b1, b2, _] = metres.to_le_bytes();
    [b0, b1, b2]
}

/// Resistance Level: `i16`; the M3i's gear.
fn resistance_level(gear: u8) -> i16 {
    i16::from(gear)
}

/// Instantaneous Power: `i16`, watts.
fn power_watts(power: u16) -> i16 {
    power as i16
}

/// Heart Rate: `u8`, bpm.
fn heart_rate_bpm(heart_rate: Tenths) -> u8 {
    heart_rate.whole() as u8
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
    reading: &crate::stats::Reading,
    has_value: fn(&Sanitized) -> bool,
    serialize: &mut impl FnMut(&Sanitized) -> Vec<u8>,
) -> Option<Vec<u8>> {
    let stats = reading.sanitized();
    has_value(&stats).then(|| serialize(&stats))
}

// Whether each characteristic has anything worth sending a new subscriber.
// All three gate on a live metric, which is exactly what `sanitized` zeroes,
// so staleness silences every one of them.

pub fn ftms_has_value(stats: &Sanitized) -> bool {
    stats.power > 0 || stats.cadence.is_positive()
}

pub fn cps_has_value(stats: &Sanitized) -> bool {
    stats.power > 0
}

pub fn hrs_has_value(stats: &Sanitized) -> bool {
    stats.heart_rate.is_positive()
}

/// Cycling Power is stateful: it reports cumulative crank revolutions and the
/// time of the last crank event in 1/1024 s, both wrapping at u16 per spec.
/// One accumulator per subscriber, advanced on every notification.
#[derive(Debug)]
pub struct CrankAccumulator {
    revolutions: f64,
    event_ticks: f64,
    last_update: Instant,
}

const CRANK_EVENT_TICKS_PER_SEC: f64 = 1024.0;

impl CrankAccumulator {
    pub fn new(now: Instant) -> Self {
        Self {
            revolutions: 0.0,
            event_ticks: 0.0,
            last_update: now,
        }
    }

    /// Advances by the time since the last call at `cadence`, returning the
    /// wrapped (revolutions, last event time) pair for the measurement.
    pub fn advance(&mut self, cadence: Tenths, now: Instant) -> (u16, u16) {
        let delta_t = now.duration_since(self.last_update).as_secs_f64();
        self.last_update = now;
        if cadence.is_positive() {
            self.revolutions += (cadence.as_f64() / 60.0) * delta_t;
            self.event_ticks += delta_t * CRANK_EVENT_TICKS_PER_SEC;
        }
        (wrap_u16(self.revolutions), wrap_u16(self.event_ticks))
    }
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
    use crate::stats::{KeiserStats, Reading};
    use std::time::Duration;

    fn stats() -> KeiserStats {
        KeiserStats {
            power: 150,
            cadence: Tenths(850),
            heart_rate: Tenths(1200),
            distance: Tenths(45),
            gear: 12,
            minutes: 2,
            seconds: 5,
            ..Default::default()
        }
    }

    fn reading() -> Reading {
        Reading::now(stats())
    }

    fn sanitized() -> Sanitized {
        reading().sanitized()
    }

    fn le_u16(data: &[u8], offset: usize) -> u16 {
        u16::from_le_bytes([data[offset], data[offset + 1]])
    }

    #[test]
    fn given_bike_units_when_encoded_for_the_wire_then_each_field_uses_the_spec_unit() {
        assert_eq!(
            cadence_half_rpm(Tenths(850)),
            170,
            "85 rpm in half-rpm units"
        );
        assert_eq!(cadence_half_rpm(Tenths(855)), 171, "85.5 rpm");
        assert_eq!(
            distance_metres_u24(Tenths(45)),
            [0x94, 0x11, 0x00],
            "4.5 km = 4500 m"
        );
        assert_eq!(
            distance_metres_u24(Tenths(u16::MAX)),
            6_553_500u32.to_le_bytes()[..3],
            "the largest distance still fits three bytes"
        );
        assert_eq!(heart_rate_bpm(Tenths(1205)), 120, "120.5 bpm truncates");
        assert_eq!(speed_hundredths_kmh(0), 0);
    }

    #[test]
    fn given_stats_when_ftms_is_serialized_then_layout_matches_the_spec() {
        let data = serialize_ftms(&sanitized());
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
        let data = serialize_ftms(&sanitized());
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
        let data = serialize_cps(&sanitized(), 1234, 5678);
        assert_eq!(data.len(), 8);
        assert_eq!(le_u16(&data, 0), 0x0020, "flags");
        assert_eq!(le_u16(&data, 2), 150, "power");
        assert_eq!(le_u16(&data, 4), 1234, "crank revolutions");
        assert_eq!(le_u16(&data, 6), 5678, "crank event time");
    }

    #[test]
    fn given_stats_when_hrs_is_serialized_then_layout_matches_the_spec() {
        let data = serialize_hrs(&sanitized());
        assert_eq!(data, vec![0x00, 120]);
    }

    /// The state a bridge sits in between rides: real values were received,
    /// but long enough ago that they must not be reported as current.
    fn stale_reading() -> Reading {
        Reading {
            stats: stats(),
            received_at: std::time::Instant::now() - crate::stats::STALE_AFTER * 2,
        }
    }

    #[test]
    fn given_live_stats_when_the_initial_notification_is_built_then_it_carries_them() {
        let mut serialize = serialize_ftms;
        let payload = initial_notification(&reading(), ftms_has_value, &mut serialize)
            .expect("live stats are worth sending");
        assert_eq!(le_u16(&payload, 11), 150, "power");
        assert_eq!(le_u16(&payload, 4), 170, "cadence in 0.5 RPM units");
    }

    #[test]
    fn given_stale_stats_when_the_initial_notification_is_built_then_nothing_is_sent() {
        // Evaluated on the raw reading, `power > 0` fires *precisely* when it
        // is too old to send: a client subscribing an hour after a ride would
        // be handed the last live power, cadence and heart rate.
        let mut serialize = serialize_ftms;
        assert!(initial_notification(&stale_reading(), ftms_has_value, &mut serialize).is_none());
    }

    #[test]
    fn given_a_paused_bike_when_the_initial_notification_is_built_then_nothing_is_sent() {
        let paused = Reading::now(KeiserStats {
            is_paused: true,
            ..stats()
        });
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
        let mut serialize = |stats: &Sanitized| {
            calls += 1;
            serialize_cps(stats, 0, 0)
        };
        assert!(initial_notification(&stale_reading(), cps_has_value, &mut serialize).is_none());
        assert_eq!(calls, 0, "nothing to send means nothing to serialize");
    }

    #[test]
    fn given_stale_stats_when_any_characteristic_decides_then_none_reports_a_value() {
        // All three characteristics gate on a live metric, and `sanitized`
        // zeroes exactly those three, so staleness silences every one of them.
        let stale = stale_reading().sanitized();
        assert!(!ftms_has_value(&stale), "Indoor Bike Data");
        assert!(!cps_has_value(&stale), "Cycling Power");
        assert!(!hrs_has_value(&stale), "Heart Rate");
    }

    #[test]
    fn given_live_stats_when_each_characteristic_decides_then_all_report_a_value() {
        let live = reading().sanitized();
        assert!(ftms_has_value(&live), "Indoor Bike Data");
        assert!(cps_has_value(&live), "Cycling Power");
        assert!(hrs_has_value(&live), "Heart Rate");
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_steady_cadence_when_a_second_passes_then_revolutions_and_ticks_advance() {
        let start = Instant::now();
        let mut crank = CrankAccumulator::new(start);
        tokio::time::advance(Duration::from_secs(60)).await;

        let (revolutions, ticks) = crank.advance(Tenths(850), Instant::now());

        assert_eq!(revolutions, 85, "85 rpm for a minute");
        assert_eq!(ticks, 60 * 1024, "ticks at 1024/s, under the u16 wrap");
    }

    #[tokio::test(start_paused = true)]
    async fn given_no_cadence_when_time_passes_then_the_crank_does_not_turn() {
        let mut crank = CrankAccumulator::new(Instant::now());
        tokio::time::advance(Duration::from_secs(60)).await;
        assert_eq!(crank.advance(Tenths::ZERO, Instant::now()), (0, 0));
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
