//! Parser for the Keiser M3i BLE advertising protocol.
//!
//! The bike broadcasts its state in the Manufacturer Data field of BLE
//! advertisements; see `doc/bluetooth-protocol.md` for the packet layout.
//! This module is deliberately free of any Bluetooth stack dependency so the
//! protocol logic can be unit-tested on any platform.
//!
//! Only new firmware (6.21+, 17-byte payload) and metric units are supported;
//! anything else is logged and ignored.

use crate::stats::KeiserStats;

/// Keiser Corporation's Bluetooth SIG assigned company identifier — the only
/// id an M3i advertises under.
///
/// On air the two prefix bytes are `02 01` (Core Spec CSS Part A §1.4: the
/// company id is the first two octets of AD type 0xFF, little-endian). BlueZ
/// decodes that to `0x0102` and strips it from the payload before btleplug
/// exposes it (`src/eir.c`, `eir_parse_msd`), which is why this parser's
/// offsets start at the firmware major byte rather than at the prefix. So
/// there is no "byte-swapped 0x0201" variant to accept: 0x0201 is AR Timing,
/// 0x01AA is Geophysical Technology and 0x015E is Unikey Technologies, none of
/// which have any connection to Keiser.
pub const KEISER_MANUFACTURER_ID: u16 = 0x0102;

/// Reads the optional `KEISER_BIKE_ID` filter: the ordinal id (packet byte 3)
/// of the one bike to accept, or `None` to accept every M3i in range.
///
/// This guards against *other Keiser bikes*, not against foreign devices —
/// every M-Series unit shares [`KEISER_MANUFACTURER_ID`] and the packet
/// format, so in a multi-bike room they all race on the same watch channel and
/// the last writer wins. Note that `0` is a real bike id (it is the deployed
/// bike's), so "unset" has to be `None` rather than zero.
pub fn bike_id_filter(lookup: impl Fn(&str) -> Option<String>) -> Option<u8> {
    let raw = lookup("KEISER_BIKE_ID").filter(|v| !v.is_empty())?;
    match raw.parse() {
        Ok(bike_id) => Some(bike_id),
        Err(_) => {
            tracing::warn!("ignoring invalid KEISER_BIKE_ID {raw:?}; accepting every bike");
            None
        }
    }
}

const PACKET_LEN: usize = 17;
/// Firmware 6.21 is the first build to append the trailing Gear byte, which is
/// what makes the payload 17 bytes; Keiser's own parser gates the gear field on
/// the same `>= 21`. See [`decode_version_byte`] for the byte encoding.
const MIN_SUPPORTED_VERSION: (u8, u8) = (6, 21);
/// Byte 2 is a data-slot index. `0x00` carries live real-time data and `0xFF`
/// means the ride is paused; every other value indexes one of the review /
/// summary records the bike broadcasts after a ride, which must not be
/// republished as current readings.
const REALTIME_DATA_SLOT: u8 = 0x00;
const PAUSED_DATA_SLOT: u8 = 0xFF;
const METRIC_FLAG: u16 = 0x8000;

/// Decodes one of the two firmware-version bytes.
///
/// The version bytes are **BCD**: each nibble is one decimal digit of the
/// version segment, so `0x24` is version 24 (not 36) and `0x21` is version 21
/// (not 33). Every other field in the packet is plain binary — the quirk is
/// unique to these two bytes, because the firmware writes the decimal digits
/// into the byte without converting to hex first.
///
/// Authority: Keiser's official parser does the same conversion literally —
/// render the byte as hex, then read that text back as decimal
/// (<https://github.com/KeiserCorp/Keiser.MSeries.BLE-Parser>,
/// `Keiser.M3i.BLE-Parser/Parser.cs`, `BuildValueConvert`):
///
/// ```csharp
/// // ** Note: Build values are not converted to hex prior to broadcast
/// //          so they arrive in a mutated form.
/// Int32.TryParse(value.ToString("X"), out converted);
/// ```
///
/// Corroborated by Keiser's parse example at <https://dev.keiser.com/mseries/direct/>
/// (byte `30` = version 30, i.e. firmware 6.30) and by Keiser's own M3i
/// simulator, which broadcasts `MAJOR="06" MINOR="24"` as raw hex bytes.
///
/// Returns `None` if either nibble is not a decimal digit; Keiser's parser
/// likewise fails to convert such a byte (`TryParse` leaves it 0).
fn decode_version_byte(byte: u8) -> Option<u8> {
    let (tens, units) = (byte >> 4, byte & 0x0F);
    (tens <= 9 && units <= 9).then_some(tens * 10 + units)
}

/// Parses a Keiser M3i manufacturer-data payload. Returns `None` for
/// payloads that are not supported M3i status packets.
pub fn parse_keiser_data(data: &[u8]) -> Option<KeiserStats> {
    tracing::trace!("Parsing data of length {}: {:02X?}", data.len(), data);

    if data.len() < 2 {
        return None;
    }

    let (Some(major), Some(minor)) = (decode_version_byte(data[0]), decode_version_byte(data[1]))
    else {
        tracing::warn!(
            "unrecognised firmware version encoding ({:02X}.{:02X})",
            data[0],
            data[1]
        );
        return None;
    };
    if (major, minor) < MIN_SUPPORTED_VERSION {
        tracing::warn!("old firmware not supported (version {major:02}.{minor:02})");
        return None;
    }

    if data.len() < PACKET_LEN {
        return None;
    }

    let data_slot = data[2];
    if data_slot != REALTIME_DATA_SLOT && data_slot != PAUSED_DATA_SLOT {
        tracing::debug!("ignoring review-record slot {:#04X}", data_slot);
        return None;
    }

    let le_u16 = |i: usize| u16::from_le_bytes([data[i], data[i + 1]]);

    let dist_raw = le_u16(14);
    if dist_raw & METRIC_FLAG == 0 {
        tracing::warn!("imperial not supported");
        return None;
    }

    Some(KeiserStats {
        bike_id: data[3],
        version: format!("{major:02}.{minor:02}"),
        power: le_u16(8),
        cadence: le_u16(4) as f32 / 10.0,
        heart_rate: le_u16(6) as f32 / 10.0,
        is_paused: data_slot == PAUSED_DATA_SLOT,
        distance: (dist_raw & 0x7FFF) as f32 / 10.0,
        energy: le_u16(10),
        minutes: data[12],
        seconds: data[13],
        gear: data[16],
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use hex_literal::hex;

    // Real captures from doc/sample-data.md (btmon on the target hardware).
    // Typing these as [u8; PACKET_LEN] makes the capture length a compile-time
    // assertion: a mistyped vector fails the build rather than a test.
    const PAUSED_CAPTURE: [u8; PACKET_LEN] = hex!("0624ff00f60100001b0002000033018008");
    const LIVE_CAPTURE: [u8; PACKET_LEN] = hex!("0624000034030000340002000100028008");

    #[test]
    fn given_real_paused_capture_when_parsed_then_all_fields_are_decoded() {
        let stats = parse_keiser_data(&PAUSED_CAPTURE).unwrap();
        assert!(stats.is_paused);
        assert_eq!(stats.version, "06.24");
        assert_eq!(stats.bike_id, 0);
        assert_eq!(stats.cadence, 50.2);
        assert_eq!(stats.heart_rate, 0.0);
        assert_eq!(stats.power, 27);
        assert_eq!(stats.energy, 2);
        assert_eq!(stats.minutes, 0);
        assert_eq!(stats.seconds, 0x33);
        assert_eq!(stats.distance, 0.1);
        assert_eq!(stats.gear, 8);
    }

    #[test]
    fn given_real_live_capture_when_parsed_then_all_fields_are_decoded() {
        let stats = parse_keiser_data(&LIVE_CAPTURE).unwrap();
        assert!(!stats.is_paused);
        assert_eq!(stats.cadence, 82.0);
        assert_eq!(stats.power, 0x34);
        assert_eq!(stats.minutes, 1);
        assert_eq!(stats.seconds, 0);
        assert_eq!(stats.distance, 0.2);
        assert_eq!(stats.gear, 8);
    }

    #[test]
    fn given_old_firmware_version_when_parsed_then_packet_is_rejected() {
        let mut data = PAUSED_CAPTURE;
        data[1] = 0x20; // minor below the supported threshold
        assert!(parse_keiser_data(&data).is_none());
    }

    #[test]
    fn given_bcd_version_bytes_when_decoded_then_each_nibble_is_a_decimal_digit() {
        // The load-bearing assertion for issue #14: the version bytes are BCD,
        // NOT plain binary. Under a binary reading 0x24 would be 36 and 0x30
        // would be 48, so any "fix" in that direction fails here.
        assert_eq!(decode_version_byte(0x06), Some(6));
        assert_eq!(decode_version_byte(0x21), Some(21), "the gate, not 33");
        assert_eq!(
            decode_version_byte(0x24),
            Some(24),
            "the deployed bike, not 36"
        );
        assert_eq!(
            decode_version_byte(0x30),
            Some(30),
            "Keiser's example, not 48"
        );
        assert_eq!(
            decode_version_byte(0x99),
            Some(99),
            "the largest valid byte"
        );
    }

    #[test]
    fn given_a_byte_with_a_non_decimal_nibble_when_decoded_then_it_is_rejected() {
        // Keiser's BuildValueConvert renders the byte as hex and parses that as
        // decimal, so a nibble above 9 fails to convert there too.
        for byte in [0x0A, 0xA0, 0x2A, 0xFF] {
            assert_eq!(decode_version_byte(byte), None, "byte {byte:#04X}");
        }
    }

    #[test]
    fn given_every_valid_bcd_byte_when_decoded_then_ordering_is_preserved() {
        // Why `MIN_SUPPORTED_VERSION` can be compared either way round: for
        // valid BCD, raw-byte order equals decoded-decimal order. That
        // equivalence is what made the pre-BCD `(6, 0x21)` constant correct,
        // and it is worth pinning so the two readings can never disagree.
        let decoded: Vec<(u8, u8)> = (0..=0xFFu8)
            .filter_map(|byte| decode_version_byte(byte).map(|value| (byte, value)))
            .collect();
        assert_eq!(decoded.len(), 100, "exactly 100 valid BCD bytes");
        assert!(
            decoded
                .windows(2)
                .all(|w| (w[0].0 < w[1].0) == (w[0].1 < w[1].1)),
            "raw-byte ordering must match decoded ordering"
        );
    }

    #[test]
    fn given_the_minimum_supported_firmware_when_parsed_then_the_packet_is_accepted() {
        let mut data = PAUSED_CAPTURE;
        data[1] = 0x21; // firmware 6.21 — the first build with a gear byte
        assert_eq!(parse_keiser_data(&data).unwrap().version, "06.21");
    }

    #[test]
    fn given_a_non_bcd_version_byte_when_parsed_then_the_packet_is_rejected() {
        let mut data = PAUSED_CAPTURE;
        data[1] = 0x2A; // not a valid BCD pair
        assert!(parse_keiser_data(&data).is_none());
    }

    #[test]
    fn given_firmware_six_thirty_when_parsed_then_the_version_reads_as_decimal() {
        // 0x30 is the discriminating case: 30 under BCD, 48 under a binary
        // reading. Keiser's own docs use it as their parse example.
        let mut data = PAUSED_CAPTURE;
        data[1] = 0x30;
        assert_eq!(parse_keiser_data(&data).unwrap().version, "06.30");
    }

    #[test]
    fn given_review_record_slot_when_parsed_then_packet_is_rejected() {
        // Slots other than 0x00 (real-time) and 0xFF (paused) index the
        // review/summary records broadcast after a ride; republishing those
        // as live readings would report a finished ride as current.
        for slot in [0x01, 0x02, 0x7F, 0xFE] {
            let mut data = LIVE_CAPTURE;
            data[2] = slot;
            assert!(
                parse_keiser_data(&data).is_none(),
                "slot {slot:#04X} should be ignored"
            );
        }
    }

    #[test]
    fn given_paused_and_realtime_slots_when_parsed_then_packets_are_accepted() {
        assert!(
            parse_keiser_data(&LIVE_CAPTURE).is_some(),
            "real-time slot 0x00"
        );
        assert!(
            parse_keiser_data(&PAUSED_CAPTURE).is_some(),
            "paused slot 0xFF"
        );
    }

    #[test]
    fn given_imperial_distance_flag_when_parsed_then_packet_is_rejected() {
        let mut data = PAUSED_CAPTURE;
        data[15] &= 0x7F; // clear the metric bit
        assert!(parse_keiser_data(&data).is_none());
    }

    #[test]
    fn given_truncated_packet_when_parsed_then_packet_is_rejected() {
        assert!(parse_keiser_data(&PAUSED_CAPTURE[..PACKET_LEN - 1]).is_none());
    }

    #[test]
    fn given_empty_packet_when_parsed_then_packet_is_rejected() {
        assert!(parse_keiser_data(&[]).is_none());
    }

    fn filter_for(value: Option<&str>) -> Option<u8> {
        bike_id_filter(|key| {
            assert_eq!(key, "KEISER_BIKE_ID");
            value.map(str::to_string)
        })
    }

    #[test]
    fn given_no_bike_id_is_configured_when_the_filter_is_read_then_every_bike_is_accepted() {
        assert_eq!(filter_for(None), None);
    }

    #[test]
    fn given_an_empty_bike_id_when_the_filter_is_read_then_every_bike_is_accepted() {
        // Commenting the line out of /etc/default/m3i-ha-bridge and blanking
        // its value behave the same way, as for the MQTT credentials.
        assert_eq!(filter_for(Some("")), None);
    }

    #[test]
    fn given_a_bike_id_of_zero_when_the_filter_is_read_then_it_is_a_real_filter() {
        // The deployed bike reports ordinal id 0, so 0 must not collapse into
        // "unset" — otherwise the one id that matters cannot be selected.
        assert_eq!(filter_for(Some("0")), Some(0));
    }

    #[test]
    fn given_a_valid_bike_id_when_the_filter_is_read_then_it_is_used() {
        assert_eq!(filter_for(Some("200")), Some(200));
    }

    #[test]
    fn given_an_unparseable_bike_id_when_the_filter_is_read_then_every_bike_is_accepted() {
        // Out of u8 range, negative and non-numeric all fall back to
        // accept-all rather than silently filtering everything out.
        for raw in ["256", "-1", "banana", "7.0"] {
            assert_eq!(filter_for(Some(raw)), None, "KEISER_BIKE_ID={raw:?}");
        }
    }
}
