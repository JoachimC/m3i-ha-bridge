//! Process-wide settings that the bridge reads from the environment. Broker
//! settings are in `mqtt::config`, next to the code that they configure.

use crate::stats::BikeId;

/// Reads the optional `KEISER_BIKE_ID` filter. The value is the ordinal id
/// of the one bike to accept; `None` accepts every M3i in range.
///
/// Every M-Series unit shares the same company id and packet format, so this
/// filter is the only way to exclude a neighbouring bike from the outputs.
/// With no filter, each bike that the bridge hears becomes its own Home
/// Assistant device, and the BLE advertisement follows the active bike. That
/// is correct for a studio dashboard and wrong for a rider who wants only
/// their own bike. `0` is a real bike id, so "unset" must be `None` and not
/// zero.
pub fn bike_id_filter(lookup: impl Fn(&str) -> Option<String>) -> Option<BikeId> {
    let raw = lookup("KEISER_BIKE_ID").filter(|v| !v.is_empty())?;
    match raw.parse() {
        Ok(bike_id) => Some(BikeId(bike_id)),
        Err(_) => {
            tracing::warn!("ignoring invalid KEISER_BIKE_ID {raw:?}; accepting every bike");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn filter_for(value: Option<&str>) -> Option<BikeId> {
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
        // Zero is a real ordinal id, so 0 must not become "unset". The user
        // could then not select that bike.
        assert_eq!(filter_for(Some("0")), Some(BikeId(0)));
    }

    #[test]
    fn given_a_valid_bike_id_when_the_filter_is_read_then_it_is_used() {
        assert_eq!(filter_for(Some("200")), Some(BikeId(200)));
    }

    #[test]
    fn given_an_unparseable_bike_id_when_the_filter_is_read_then_every_bike_is_accepted() {
        // Values out of u8 range, negative values, and non-numeric values
        // all give accept-all. A silent filter that rejects every bike would
        // be worse.
        for raw in ["256", "-1", "banana", "7.0"] {
            assert_eq!(filter_for(Some(raw)), None, "KEISER_BIKE_ID={raw:?}");
        }
    }
}
