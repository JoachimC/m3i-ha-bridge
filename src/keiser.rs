//! Parser for the Keiser M3i BLE advertising protocol.
//!
//! The bike broadcasts its state in the Manufacturer Data field of BLE
//! advertisements; see `doc/bluetooth-protocol.md` for the packet layout.
//! This module is deliberately free of any Bluetooth stack dependency so the
//! protocol logic can be unit-tested on any platform, and it does no logging:
//! it says *why* a payload was rejected and leaves the reporting to the
//! caller, which knows how often the same beacon will be seen again.
//!
//! Only new firmware (6.21+, 17-byte payload) and metric units are supported.

use std::fmt;

use crate::stats::{BikeId, KeiserStats, Tenths, Version};

/// Keiser Corporation's Bluetooth SIG assigned company identifier — the only
/// id an M3i advertises under.
///
/// On air the two prefix bytes are `02 01` (Core Spec CSS Part A §1.4: the
/// company id is the first two octets of AD type 0xFF, little-endian). BlueZ
/// decodes that to `0x0102` and strips it from the payload (`src/eir.c`,
/// `eir_parse_msd`), which is why this parser's offsets start at the firmware
/// major byte rather than at the prefix. 0x0201, 0x01AA and 0x015E are AR
/// Timing, Geophysical Technology and Unikey Technologies — unrelated to
/// Keiser, and 0x0201 can never reach this parser.
pub const MANUFACTURER_ID: u16 = 0x0102;

/// Length of the manufacturer-data payload after BlueZ strips the company id.
/// Firmware 6.21 is the first build to append the trailing Gear byte, which is
/// what makes it 17.
pub const PAYLOAD_LEN: usize = 17;

/// Keiser's own parser gates the gear field on the same `>= 21`. See
/// [`decode_version_byte`] for the byte encoding.
const MIN_SUPPORTED_VERSION: Version = Version {
    major: 6,
    minor: 21,
};

/// Byte 2 is a data-slot index. `0x00` carries live real-time data and `0xFF`
/// means the ride is paused; every other value indexes one of the review /
/// summary records the bike broadcasts after a ride, which must not be
/// republished as current readings.
const REALTIME_DATA_SLOT: u8 = 0x00;
const PAUSED_DATA_SLOT: u8 = 0xFF;

/// Bit 15 of the distance word: `1` metric, `0` imperial.
const METRIC_FLAG: u16 = 0x8000;
const DISTANCE_VALUE_MASK: u16 = 0x7FFF;

/// Why a manufacturer-data payload was not decoded as a reading.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Rejection {
    /// Shorter than [`PAYLOAD_LEN`].
    TooShort(usize),
    /// A version byte that is not BCD; the raw bytes are reported.
    UnrecognisedVersion(u8, u8),
    /// Firmware before 6.21 has no gear byte and a different layout.
    OldFirmware(Version),
    /// One of the post-ride review/summary records.
    ReviewRecord(u8),
    /// The bike is set to imperial units.
    Imperial,
}

impl Rejection {
    /// Whether this says something about the bike (persistently unsupported)
    /// rather than about one packet. The caller reports these more loudly.
    pub fn is_unsupported_bike(&self) -> bool {
        matches!(
            self,
            Rejection::UnrecognisedVersion(..) | Rejection::OldFirmware(_) | Rejection::Imperial
        )
    }
}

impl fmt::Display for Rejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Rejection::TooShort(len) => write!(f, "payload is {len} bytes, need {PAYLOAD_LEN}"),
            Rejection::UnrecognisedVersion(major, minor) => {
                write!(
                    f,
                    "unrecognised firmware version encoding ({major:02X}.{minor:02X})"
                )
            }
            Rejection::OldFirmware(version) => {
                write!(f, "old firmware not supported (version {version})")
            }
            Rejection::ReviewRecord(slot) => write!(f, "review-record slot {slot:#04X}"),
            Rejection::Imperial => write!(f, "imperial units not supported"),
        }
    }
}

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

/// A full-length payload, with the fields at their documented offsets.
struct Packet<'a>(&'a [u8; PAYLOAD_LEN]);

impl Packet<'_> {
    fn version(&self) -> Result<Version, Rejection> {
        let (major, minor) = (self.0[0], self.0[1]);
        match (decode_version_byte(major), decode_version_byte(minor)) {
            (Some(major), Some(minor)) => Ok(Version { major, minor }),
            _ => Err(Rejection::UnrecognisedVersion(major, minor)),
        }
    }

    fn data_slot(&self) -> u8 {
        self.0[2]
    }

    fn bike_id(&self) -> BikeId {
        BikeId(self.0[3])
    }

    fn cadence(&self) -> Tenths {
        Tenths(self.u16_at(4))
    }

    fn heart_rate(&self) -> Tenths {
        Tenths(self.u16_at(6))
    }

    fn power(&self) -> u16 {
        self.u16_at(8)
    }

    fn energy(&self) -> u16 {
        self.u16_at(10)
    }

    fn minutes(&self) -> u8 {
        self.0[12]
    }

    fn seconds(&self) -> u8 {
        self.0[13]
    }

    fn distance_word(&self) -> u16 {
        self.u16_at(14)
    }

    fn gear(&self) -> u8 {
        self.0[16]
    }

    fn u16_at(&self, offset: usize) -> u16 {
        u16::from_le_bytes([self.0[offset], self.0[offset + 1]])
    }
}

/// Decodes a Keiser M3i manufacturer-data payload.
pub fn parse(data: &[u8]) -> Result<KeiserStats, Rejection> {
    let packet = Packet(
        data.try_into()
            .map_err(|_| Rejection::TooShort(data.len()))?,
    );

    let version = packet.version()?;
    if (version.major, version.minor) < (MIN_SUPPORTED_VERSION.major, MIN_SUPPORTED_VERSION.minor) {
        return Err(Rejection::OldFirmware(version));
    }

    let data_slot = packet.data_slot();
    if data_slot != REALTIME_DATA_SLOT && data_slot != PAUSED_DATA_SLOT {
        return Err(Rejection::ReviewRecord(data_slot));
    }

    let distance_word = packet.distance_word();
    if distance_word & METRIC_FLAG == 0 {
        return Err(Rejection::Imperial);
    }

    Ok(KeiserStats {
        bike_id: packet.bike_id(),
        version,
        power: packet.power(),
        cadence: packet.cadence(),
        heart_rate: packet.heart_rate(),
        is_paused: data_slot == PAUSED_DATA_SLOT,
        distance: Tenths(distance_word & DISTANCE_VALUE_MASK),
        energy: packet.energy(),
        minutes: packet.minutes(),
        seconds: packet.seconds(),
        gear: packet.gear(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use hex_literal::hex;

    // Real captures from doc/sample-data.md (btmon on the target hardware).
    // Typing these as [u8; PAYLOAD_LEN] makes the capture length a compile-time
    // assertion: a mistyped vector fails the build rather than a test.
    const PAUSED_CAPTURE: [u8; PAYLOAD_LEN] = hex!("0624ff00f60100001b0002000033018008");
    const LIVE_CAPTURE: [u8; PAYLOAD_LEN] = hex!("0624000034030000340002000100028008");

    #[test]
    fn given_real_paused_capture_when_parsed_then_all_fields_are_decoded() {
        let stats = parse(&PAUSED_CAPTURE).unwrap();
        assert!(stats.is_paused);
        assert_eq!(stats.version.to_string(), "06.24");
        assert_eq!(stats.bike_id, BikeId(0));
        assert_eq!(stats.cadence, Tenths(502), "50.2 rpm");
        assert_eq!(stats.heart_rate, Tenths::ZERO);
        assert_eq!(stats.power, 27);
        assert_eq!(stats.energy, 2);
        assert_eq!(stats.minutes, 0);
        assert_eq!(stats.seconds, 0x33);
        assert_eq!(stats.distance, Tenths(1), "0.1 km");
        assert_eq!(stats.gear, 8);
    }

    #[test]
    fn given_real_live_capture_when_parsed_then_all_fields_are_decoded() {
        let stats = parse(&LIVE_CAPTURE).unwrap();
        assert!(!stats.is_paused);
        assert_eq!(stats.cadence, Tenths(820), "82.0 rpm");
        assert_eq!(stats.power, 0x34);
        assert_eq!(stats.minutes, 1);
        assert_eq!(stats.seconds, 0);
        assert_eq!(stats.distance, Tenths(2), "0.2 km");
        assert_eq!(stats.gear, 8);
    }

    #[test]
    fn given_old_firmware_version_when_parsed_then_the_rejection_names_the_version() {
        let mut data = PAUSED_CAPTURE;
        data[1] = 0x20; // minor below the supported threshold
        assert_eq!(
            parse(&data),
            Err(Rejection::OldFirmware(Version {
                major: 6,
                minor: 20
            }))
        );
    }

    #[test]
    fn given_bcd_version_bytes_when_decoded_then_each_nibble_is_a_decimal_digit() {
        // The version bytes are BCD, NOT plain binary. Under a binary reading
        // 0x24 would be 36 and 0x30 would be 48, so any "fix" in that
        // direction fails here.
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
        // Why the minimum version can be compared as decoded digits: for
        // valid BCD, raw-byte order equals decoded-decimal order, so the two
        // readings can never disagree.
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
        assert_eq!(parse(&data).unwrap().version.to_string(), "06.21");
    }

    #[test]
    fn given_a_non_bcd_version_byte_when_parsed_then_the_rejection_carries_the_raw_bytes() {
        let mut data = PAUSED_CAPTURE;
        data[1] = 0x2A; // not a valid BCD pair
        assert_eq!(
            parse(&data),
            Err(Rejection::UnrecognisedVersion(0x06, 0x2A))
        );
    }

    #[test]
    fn given_firmware_six_thirty_when_parsed_then_the_version_reads_as_decimal() {
        // 0x30 is the discriminating case: 30 under BCD, 48 under a binary
        // reading. Keiser's own docs use it as their parse example.
        let mut data = PAUSED_CAPTURE;
        data[1] = 0x30;
        assert_eq!(parse(&data).unwrap().version.to_string(), "06.30");
    }

    #[test]
    fn given_review_record_slot_when_parsed_then_the_rejection_names_the_slot() {
        // Slots other than 0x00 (real-time) and 0xFF (paused) index the
        // review/summary records broadcast after a ride; republishing those
        // as live readings would report a finished ride as current.
        for slot in [0x01, 0x02, 0x7F, 0xFE] {
            let mut data = LIVE_CAPTURE;
            data[2] = slot;
            assert_eq!(parse(&data), Err(Rejection::ReviewRecord(slot)));
        }
    }

    #[test]
    fn given_paused_and_realtime_slots_when_parsed_then_packets_are_accepted() {
        assert!(parse(&LIVE_CAPTURE).is_ok(), "real-time slot 0x00");
        assert!(parse(&PAUSED_CAPTURE).is_ok(), "paused slot 0xFF");
    }

    #[test]
    fn given_imperial_distance_flag_when_parsed_then_the_packet_is_rejected() {
        let mut data = PAUSED_CAPTURE;
        data[15] &= 0x7F; // clear the metric bit
        assert_eq!(parse(&data), Err(Rejection::Imperial));
    }

    #[test]
    fn given_a_truncated_packet_when_parsed_then_the_rejection_reports_its_length() {
        assert_eq!(
            parse(&PAUSED_CAPTURE[..PAYLOAD_LEN - 1]),
            Err(Rejection::TooShort(PAYLOAD_LEN - 1))
        );
        assert_eq!(parse(&[]), Err(Rejection::TooShort(0)));
    }

    #[test]
    fn given_each_rejection_when_classified_then_only_bike_level_ones_are_notable() {
        // What the caller logs at warn: conditions that will recur for as long
        // as that bike is in range. A short or review-record packet is one
        // beacon, not a bike.
        assert!(Rejection::Imperial.is_unsupported_bike());
        assert!(Rejection::OldFirmware(Version { major: 6, minor: 0 }).is_unsupported_bike());
        assert!(Rejection::UnrecognisedVersion(0xAA, 0).is_unsupported_bike());
        assert!(!Rejection::TooShort(3).is_unsupported_bike());
        assert!(!Rejection::ReviewRecord(1).is_unsupported_bike());
    }
}
