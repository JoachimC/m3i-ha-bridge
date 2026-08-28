//! Where things go on the broker: topic and id naming for the bridge and
//! for each bike.

use crate::stats::BikeId;

/// The naming policy, separate from connection settings so discovery and
/// publishing can be tested with a `Topics` alone.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Topics {
    pub prefix: String,
    pub discovery_prefix: String,
}

impl Topics {
    /// Where one bike's readings go: `<prefix>/<id>/state`.
    pub fn state(&self, bike_id: BikeId) -> String {
        format!("{}/{bike_id}/state", self.prefix)
    }

    /// Whether the *bridge* is running: the last will lives here, so a crash
    /// takes every bike's entities offline at once.
    pub fn bridge_availability(&self) -> String {
        format!("{}/availability", self.prefix)
    }

    /// Whether one *bike* is being heard: `offline` once its readings go
    /// stale, so a bike that has been switched off greys out in Home Assistant
    /// while the bridge, and the other bikes, stay online.
    pub fn bike_availability(&self, bike_id: BikeId) -> String {
        format!("{}/{bike_id}/availability", self.prefix)
    }

    /// Node id used in Home Assistant discovery topics; must not contain '/'.
    ///
    /// Deliberately independent of the topic prefix: the same physical bike
    /// heard by two bridges on one broker is one device, not two.
    pub fn node_id(&self, bike_id: BikeId) -> String {
        format!("m3i-ha-bridge-{bike_id}")
    }

    /// The device-discovery config topic for one bike.
    pub fn device_config(&self, bike_id: BikeId) -> String {
        format!(
            "{}/device/{}/config",
            self.discovery_prefix,
            self.node_id(bike_id)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topics() -> Topics {
        Topics {
            prefix: "fitness/m3i".into(),
            discovery_prefix: "ha-discovery".into(),
        }
    }

    #[test]
    fn given_a_bike_when_topics_are_built_then_the_padded_id_is_a_path_segment() {
        assert_eq!(topics().state(BikeId(42)), "fitness/m3i/042/state");
        assert_eq!(
            topics().bike_availability(BikeId(42)),
            "fitness/m3i/042/availability"
        );
        assert_eq!(topics().bridge_availability(), "fitness/m3i/availability");
    }

    #[test]
    fn given_a_topic_prefix_with_slashes_when_the_discovery_topic_is_built_then_the_node_id_is_unaffected()
     {
        // A device discovery topic is exactly <prefix>/device/<node_id>/config;
        // the node id is fixed, so the topic prefix cannot leak a slash into it.
        assert_eq!(
            topics().device_config(BikeId(42)),
            "ha-discovery/device/m3i-ha-bridge-042/config"
        );
        assert_eq!(topics().node_id(BikeId(7)), "m3i-ha-bridge-007");
    }
}
