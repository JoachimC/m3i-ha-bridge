//! What the bridge tells Home Assistant about a bike: the device-discovery
//! config and the state payload its templates read.

use serde_json::json;

use super::topics::Topics;
use crate::stats::{BikeId, Sanitized};

/// Home Assistant marks sensors unavailable if no state arrives within this window.
pub(super) const EXPIRE_AFTER_SECS: u32 = 120;

pub(super) fn state_payload(stats: &Sanitized) -> serde_json::Value {
    json!({
        "bike_id": stats.bike_id.0,
        "version": stats.version.to_string(),
        "power": stats.power,
        "cadence": stats.cadence.as_f64(),
        "heart_rate": stats.heart_rate.as_f64(),
        "gear": stats.gear,
        "distance": stats.distance.as_f64(),
        "distance_unit": "Km",
        "energy": stats.energy,
        "energy_unit": "KCal",
        "elapsed_seconds": stats.elapsed_seconds(),
        "is_paused": stats.is_paused,
    })
}

/// Everything that varies between the sensors announced to Home Assistant.
///
/// A struct rather than positional arguments: a closure would take nine
/// parameters, most of them `Option<&str>`, and every call site would be an
/// unreadable run of `None`s.
pub(super) struct SensorSpec {
    object_id: &'static str,
    name: &'static str,
    value_template: &'static str,
    unit: Option<&'static str>,
    device_class: Option<&'static str>,
    /// Drives Home Assistant's long-term statistics. Without it, Home
    /// Assistant records a sensor in history but never aggregates it.
    state_class: Option<&'static str>,
    /// Display only — the payload builder already rounds the state to the
    /// bike's resolution.
    precision: Option<u8>,
    icon: Option<&'static str>,
    /// `diagnostic` moves an entity out of the device's main sensor list into
    /// its Diagnostic section — right for identity, wrong for a reading.
    entity_category: Option<&'static str>,
}

impl SensorSpec {
    /// This sensor's entry in the device discovery `components` map.
    fn component(&self, node_id: &str) -> serde_json::Value {
        let optional = [
            ("unit_of_measurement", self.unit.map(|v| json!(v))),
            ("device_class", self.device_class.map(|v| json!(v))),
            ("state_class", self.state_class.map(|v| json!(v))),
            (
                "suggested_display_precision",
                self.precision.map(|v| json!(v)),
            ),
            ("icon", self.icon.map(|v| json!(v))),
            ("entity_category", self.entity_category.map(|v| json!(v))),
        ];
        let mut component = json!({
            "platform": "sensor",
            "name": self.name,
            "unique_id": format!("{}_{}", node_id, self.object_id),
            "value_template": self.value_template,
            "expire_after": EXPIRE_AFTER_SECS,
        });
        let obj = component.as_object_mut().expect("a JSON object");
        for (key, value) in optional {
            if let Some(value) = value {
                obj.insert(key.into(), value);
            }
        }
        component
    }
}

/// Home Assistant metadata for each published sensor.
///
/// Home Assistant constrains the device-class choices: `power` accepts only
/// `measurement`, and `energy` only `total`/`total_increasing`. Where Home
/// Assistant allows a choice, the choice is semantic. Distance and energy
/// use `total_increasing`: they accumulate through a ride and reset to zero
/// on the next one, which is exactly the case that state class exists for.
/// Elapsed time uses `measurement`: the useful statistic is the live value,
/// not a lifetime sum of seconds.
///
/// Heart rate and cadence have no device class in Home Assistant; it
/// accepts `bpm` and `rpm` as free-form units.
pub(super) const SENSORS: &[SensorSpec] = &[
    SensorSpec {
        object_id: "power",
        name: "Power",
        value_template: "{{ value_json.power }}",
        unit: Some("W"),
        device_class: Some("power"),
        state_class: Some("measurement"),
        precision: Some(0),
        icon: None, // the device class supplies one
        entity_category: None,
    },
    SensorSpec {
        object_id: "cadence",
        name: "Cadence",
        value_template: "{{ value_json.cadence }}",
        unit: Some("rpm"),
        device_class: None,
        state_class: Some("measurement"),
        precision: Some(0),
        icon: Some("mdi:rotate-right"),
        entity_category: None,
    },
    SensorSpec {
        object_id: "heart_rate",
        name: "Heart Rate",
        value_template: "{{ value_json.heart_rate }}",
        unit: Some("bpm"),
        device_class: None,
        state_class: Some("measurement"),
        precision: Some(0),
        icon: Some("mdi:heart-pulse"),
        entity_category: None,
    },
    SensorSpec {
        object_id: "gear",
        name: "Gear",
        value_template: "{{ value_json.gear }}",
        unit: None,
        device_class: None,
        state_class: Some("measurement"),
        precision: Some(0),
        icon: Some("mdi:cog"),
        entity_category: None,
    },
    SensorSpec {
        object_id: "distance",
        name: "Distance",
        value_template: "{{ value_json.distance }}",
        unit: Some("km"),
        device_class: Some("distance"),
        state_class: Some("total_increasing"),
        // The bike transmits distance to 0.1 km, so two decimals would show a
        // precision the reading does not have.
        precision: Some(1),
        icon: Some("mdi:map-marker-distance"),
        entity_category: None,
    },
    SensorSpec {
        object_id: "energy",
        name: "Energy",
        value_template: "{{ value_json.energy }}",
        unit: Some("kcal"),
        // Home Assistant 2024.10 and later accepts cal/kcal/Mcal/Gcal for
        // the energy device class, which makes this valid. An older Home
        // Assistant rejects the entity outright. To support one, remove the
        // device class; the unit and state class alone still give long-term
        // statistics.
        device_class: Some("energy"),
        state_class: Some("total_increasing"),
        precision: Some(0),
        icon: Some("mdi:fire"),
        entity_category: None,
    },
    SensorSpec {
        object_id: "elapsed_time",
        name: "Elapsed Time",
        value_template: "{{ value_json.elapsed_seconds }}",
        unit: Some("s"),
        device_class: Some("duration"),
        state_class: Some("measurement"),
        precision: Some(0),
        icon: None,
        entity_category: None,
    },
    // Identity rather than a measurement: no unit, no state class (there is
    // nothing to aggregate), and in the Diagnostic category so it does not
    // sit between Power and Cadence on the dashboard.
    SensorSpec {
        object_id: "bike_id",
        name: "Bike ID",
        value_template: "{{ value_json.bike_id }}",
        unit: None,
        device_class: None,
        state_class: None,
        precision: None,
        icon: Some("mdi:identifier"),
        entity_category: Some("diagnostic"),
    },
];

/// Home Assistant device-based MQTT discovery: one retained config per bike
/// carries every entity, so sensors appear automatically without any YAML
/// on the HA side.
///
/// The bridge uses device discovery (`<prefix>/device/<node_id>/config`
/// with a `components` map; Home Assistant 2024.11+) rather than one
/// retained topic per entity.
///
/// The root holds `state_topic`, `availability` and `availability_mode`,
/// and every component inherits them. `expire_after` is a per-entity
/// option, so each component repeats it. `device` and `origin` are
/// mandatory here, not merely recommended.
///
/// # Entity naming
///
/// Each component announces a short `name` ("Power") plus the shared `device`
/// block, and Home Assistant composes the two: friendly name "Keiser M3i #042
/// Power", entity id `sensor.keiser_m3i_042_power`. No bare `sensor.power`
/// exists to collide with anything else on the instance, and two bikes
/// never collide with each other.
///
/// Do **not** add `"has_entity_name": true` to these payloads. It is not an
/// MQTT discovery option: the MQTT integration (Home Assistant 2023.8 and
/// later) sets `_attr_has_entity_name` unconditionally on every entity, and
/// the discovery schema uses `extra=vol.REMOVE_EXTRA`, so it discards the
/// key silently. The key would read as configuration while it changes
/// nothing.
pub(super) fn discovery_message(topics: &Topics, bike_id: BikeId) -> (String, serde_json::Value) {
    let node_id = topics.node_id(bike_id);
    let device = json!({
        "identifiers": [format!("m3i_ha_bridge_{bike_id}")],
        "name": bike_id.display_name(),
        "manufacturer": "Keiser",
        "model": "M3i",
    });
    // Both must say online: the bridge's topic carries the last will, so a
    // crash sets every bike offline; the bike's own topic goes offline when
    // its readings go stale, so a powered-off bike shows unavailable on its
    // own.
    let availability = json!([
        { "topic": topics.bridge_availability() },
        { "topic": topics.bike_availability(bike_id) },
    ]);
    let origin = json!({
        "name": env!("CARGO_PKG_NAME"),
        "sw_version": env!("CARGO_PKG_VERSION"),
        "support_url": "https://github.com/JoachimC/m3i-ha-bridge",
    });

    let mut components = serde_json::Map::new();
    for spec in SENSORS {
        components.insert(spec.object_id.to_string(), spec.component(&node_id));
    }
    components.insert(
        "paused".to_string(),
        json!({
            "platform": "binary_sensor",
            "name": "Paused",
            "unique_id": format!("{}_paused", node_id),
            "value_template": "{{ 'ON' if value_json.is_paused else 'OFF' }}",
            // Same expiry as the sensors, so the device cannot contradict
            // itself about reachability.
            "expire_after": EXPIRE_AFTER_SECS,
        }),
    );

    (
        topics.device_config(bike_id),
        json!({
            "device": device,
            "origin": origin,
            "state_topic": topics.state(bike_id),
            "availability": availability,
            "availability_mode": "all",
            "components": components,
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::super::test_support::{BIKE, test_topics};
    use super::*;
    use crate::stats::{KeiserStats, Reading, Tenths};
    use hex_literal::hex;

    fn device_discovery() -> (String, serde_json::Value) {
        discovery_message(&test_topics(), BIKE)
    }

    /// One entity's component of the device discovery payload.
    fn discovery_for(object_id: &str) -> serde_json::Value {
        let (_, payload) = device_discovery();
        payload["components"]
            .get(object_id)
            .cloned()
            .unwrap_or_else(|| panic!("no discovery component for {object_id}"))
    }

    fn payload_of(stats: KeiserStats) -> serde_json::Value {
        state_payload(&Reading::now(stats).sanitized())
    }

    #[test]
    fn given_stats_when_state_payload_is_built_then_all_fields_are_present() {
        let payload = payload_of(KeiserStats {
            bike_id: BikeId(3),
            power: 150,
            cadence: Tenths(855),
            heart_rate: Tenths(1200),
            gear: 12,
            distance: Tenths(42),
            energy: 55,
            minutes: 2,
            seconds: 5,
            is_paused: false,
            ..Default::default()
        });
        assert_eq!(payload["bike_id"], 3);
        assert_eq!(payload["power"], 150);
        assert_eq!(payload["gear"], 12);
        assert_eq!(payload["distance_unit"], "Km");
        assert_eq!(payload["elapsed_seconds"], 125);
        assert_eq!(payload["is_paused"], false);
    }

    #[test]
    fn given_the_real_capture_when_the_state_payload_is_serialized_then_it_carries_no_float_noise()
    {
        // End to end from the bytes that doc/sample-data.md captured,
        // because the state is a string Home Assistant renders verbatim.
        // Cadence 502 -> 50.2 rpm and distance 1 -> 0.1 km would otherwise
        // serialize as 50.20000076293945 and 0.10000000149011612.
        let mut stats = crate::keiser::parse(&hex!("0624ff00f60100001b0002000033018008"))
            .expect("the captured packet should parse");
        stats.is_paused = false; // so sanitizing keeps the 50.2 rpm this test is about
        let payload = payload_of(stats).to_string();

        assert!(
            payload.contains("\"cadence\":50.2"),
            "expected a clean cadence in {payload}"
        );
        assert!(
            payload.contains("\"distance\":0.1"),
            "expected a clean distance in {payload}"
        );
        assert!(
            !payload.contains("0000000"),
            "no field should carry float noise: {payload}"
        );
    }

    #[test]
    fn given_a_heart_rate_when_the_state_payload_is_built_then_it_is_rounded_too() {
        let payload = payload_of(KeiserStats {
            heart_rate: Tenths(1205),
            ..Default::default()
        });
        assert!(payload.to_string().contains("\"heart_rate\":120.5"));
    }

    /// The bike every discovery test announces.
    #[test]
    fn given_config_when_the_discovery_message_is_built_then_it_is_one_device_topic_per_bike() {
        // One retained topic per bike carrying every entity; the node id,
        // topics and unique ids all carry the padded bike id.
        let (topic, payload) = device_discovery();

        assert_eq!(topic, "homeassistant/device/m3i-ha-bridge-042/config");
        assert_eq!(payload["state_topic"], "m3i/042/state");
        let components = payload["components"].as_object().unwrap();
        assert_eq!(components.len(), SENSORS.len() + 1);
        assert_eq!(components["power"]["platform"], "sensor");
        assert_eq!(components["power"]["unique_id"], "m3i-ha-bridge-042_power");
        assert_eq!(components["paused"]["platform"], "binary_sensor");
        assert_eq!(
            components["paused"]["unique_id"],
            "m3i-ha-bridge-042_paused"
        );
    }

    #[test]
    fn given_the_discovery_message_when_built_then_the_shared_options_are_at_the_root_only() {
        // Device discovery inherits state_topic and availability from the
        // root. To repeat them per component would be redundant, and the
        // schema forbids device and origin there.
        let (_, payload) = device_discovery();
        for (object_id, component) in payload["components"].as_object().unwrap() {
            for shared in [
                "state_topic",
                "availability",
                "availability_mode",
                "device",
                "origin",
            ] {
                assert!(
                    component.get(shared).is_none(),
                    "{object_id} repeats the shared option {shared}"
                );
            }
            assert!(
                component.get("platform").is_some(),
                "{object_id} must name its platform"
            );
            assert_eq!(
                component["expire_after"], EXPIRE_AFTER_SECS,
                "{object_id}: expire_after is per entity, not shared"
            );
        }
    }

    #[test]
    fn given_the_discovery_message_when_built_then_it_requires_both_bridge_and_bike_online() {
        // The bridge topic carries the last will; the bike topic goes offline
        // when that bike's readings go stale. An entity is only available when
        // both say so, otherwise a dead bridge would leave a bike "online".
        let (_, payload) = device_discovery();
        assert_eq!(
            payload["availability"],
            json!([
                { "topic": "m3i/availability" },
                { "topic": "m3i/042/availability" },
            ])
        );
        assert_eq!(payload["availability_mode"], "all");
        assert!(
            payload.get("availability_topic").is_none(),
            "availability_topic and availability are mutually exclusive"
        );
    }

    #[test]
    fn given_the_bike_id_sensor_when_announced_then_it_is_a_diagnostic_integer() {
        let payload = discovery_for("bike_id");
        assert_eq!(payload["name"], "Bike ID");
        assert_eq!(payload["entity_category"], "diagnostic");
        assert!(payload.get("unit_of_measurement").is_none());
        assert!(payload.get("state_class").is_none(), "nothing to aggregate");
        assert_eq!(payload["value_template"], "{{ value_json.bike_id }}");

        let stats = KeiserStats {
            bike_id: BIKE,
            ..Default::default()
        };
        assert_eq!(
            payload_of(stats)["bike_id"],
            json!(42),
            "the console shows 42, not \"042\""
        );
    }

    #[test]
    fn given_the_paused_binary_sensor_when_announced_then_it_expires_with_the_others() {
        // Without it the binary sensor stays live while every sensor expires,
        // and the device contradicts itself about reachability.
        assert_eq!(discovery_for("paused")["expire_after"], EXPIRE_AFTER_SECS);
    }

    #[test]
    fn given_the_sensors_when_announced_then_their_units_and_classes_match_the_table() {
        // Every reading declares a state_class: without it Home Assistant
        // records history but computes no long-term statistics. Only the
        // identity sensor is diagnostic, and it alone has nothing to aggregate.
        let expected = [
            // object_id, unit, device_class, state_class, entity_category
            ("power", Some("W"), Some("power"), Some("measurement"), None),
            ("cadence", Some("rpm"), None, Some("measurement"), None),
            ("heart_rate", Some("bpm"), None, Some("measurement"), None),
            ("gear", None, None, Some("measurement"), None),
            (
                "distance",
                Some("km"),
                Some("distance"),
                Some("total_increasing"),
                None,
            ),
            (
                "energy",
                Some("kcal"),
                Some("energy"),
                Some("total_increasing"),
                None,
            ),
            (
                "elapsed_time",
                Some("s"),
                Some("duration"),
                Some("measurement"),
                None,
            ),
            ("bike_id", None, None, None, Some("diagnostic")),
        ];
        assert_eq!(
            expected.len(),
            SENSORS.len(),
            "a sensor is missing from this table"
        );

        for (object_id, unit, device_class, state_class, entity_category) in expected {
            let payload = discovery_for(object_id);
            assert_eq!(
                payload["unit_of_measurement"].as_str(),
                unit,
                "{object_id} unit"
            );
            assert_eq!(
                payload["device_class"].as_str(),
                device_class,
                "{object_id} device_class"
            );
            assert_eq!(
                payload["state_class"].as_str(),
                state_class,
                "{object_id} state_class"
            );
            assert_eq!(
                payload["entity_category"].as_str(),
                entity_category,
                "{object_id} entity_category"
            );
        }
    }

    #[test]
    fn given_a_device_class_when_announced_then_its_unit_and_state_class_are_ones_ha_accepts() {
        // Home Assistant rejects an invalid device_class/unit pair at
        // discovery and never creates the entity: a silent loss with only a
        // log line. It warns about an impossible device_class/state_class
        // pair. These tables copy its own, so a wrong combination fails
        // here rather than on the running system.
        const DEVICE_CLASS_RULES: &[(&str, &[&str], &[&str])] = &[
            (
                "power",
                &["mW", "W", "kW", "MW", "GW", "TW"],
                &["measurement"],
            ),
            (
                "energy",
                &[
                    "J", "kJ", "MJ", "GJ", "mWh", "Wh", "kWh", "MWh", "GWh", "TWh", "cal", "kcal",
                    "Mcal", "Gcal",
                ],
                &["total", "total_increasing"],
            ),
            (
                "distance",
                &["mm", "cm", "m", "km", "in", "ft", "yd", "mi", "nmi"],
                &[
                    "measurement",
                    "measurement_angle",
                    "total",
                    "total_increasing",
                ],
            ),
            (
                "duration",
                &["d", "h", "min", "s", "ms", "µs"],
                &[
                    "measurement",
                    "measurement_angle",
                    "total",
                    "total_increasing",
                ],
            ),
        ];

        for spec in SENSORS {
            let Some(device_class) = spec.device_class else {
                continue;
            };
            let (_, units, state_classes) = DEVICE_CLASS_RULES
                .iter()
                .find(|(name, _, _)| *name == device_class)
                .unwrap_or_else(|| panic!("no rule recorded for device class {device_class}"));

            let unit = spec
                .unit
                .unwrap_or_else(|| panic!("{} has a device class but no unit", spec.object_id));
            assert!(
                units.contains(&unit),
                "{}: unit {unit:?} is not valid for device class {device_class:?} — Home \
                 Assistant would reject the discovery config and never create the entity",
                spec.object_id
            );
            let state_class = spec.state_class.expect("checked separately");
            assert!(
                state_classes.contains(&state_class),
                "{}: state class {state_class:?} is impossible for device class {device_class:?}",
                spec.object_id
            );
        }
    }

    #[test]
    fn given_the_sensors_when_announced_then_unique_ids_are_unchanged_and_distinct() {
        // unique_id is what ties a discovery config to an existing entity, so
        // changing one would silently orphan the old entity and its history.
        let ids: Vec<String> = SENSORS
            .iter()
            .map(|spec| {
                discovery_for(spec.object_id)["unique_id"]
                    .as_str()
                    .unwrap()
                    .to_string()
            })
            .collect();
        assert_eq!(
            ids,
            [
                "m3i-ha-bridge-042_power",
                "m3i-ha-bridge-042_cadence",
                "m3i-ha-bridge-042_heart_rate",
                "m3i-ha-bridge-042_gear",
                "m3i-ha-bridge-042_distance",
                "m3i-ha-bridge-042_energy",
                "m3i-ha-bridge-042_elapsed_time",
                "m3i-ha-bridge-042_bike_id",
            ]
        );
    }

    #[test]
    fn given_a_discovery_message_when_announced_then_it_carries_a_short_name_and_a_device() {
        // This pairing is what makes Home Assistant derive
        // sensor.keiser_m3i_042_power rather than a collision-prone
        // sensor.power: it prefixes the entity name with the device name. A
        // long name like "Keiser M3i Power" here would produce
        // sensor.keiser_m3i_042_keiser_m3i_power instead.
        let (_, payload) = device_discovery();
        assert_eq!(payload["device"]["name"], "Keiser M3i #042");
        assert_eq!(
            payload["device"]["identifiers"],
            json!(["m3i_ha_bridge_042"])
        );
        for spec in SENSORS {
            let component = discovery_for(spec.object_id);
            assert!(
                !component["name"].as_str().unwrap().contains("Keiser"),
                "{} repeats the device name in its entity name",
                spec.object_id
            );
        }
    }

    #[test]
    fn given_a_discovery_message_when_announced_then_it_does_not_set_has_entity_name() {
        // `has_entity_name` is not an MQTT discovery option: the integration
        // hardcodes it True on every entity, and the discovery schema drops
        // unknown keys silently (extra=vol.REMOVE_EXTRA). To publish it
        // would read as configuration while it does nothing, so its absence
        // is deliberate and this test pins it.
        let mut payloads: Vec<serde_json::Value> = SENSORS
            .iter()
            .map(|spec| discovery_for(spec.object_id))
            .collect();
        payloads.push(discovery_for("paused"));

        for payload in payloads {
            assert!(
                payload.get("has_entity_name").is_none(),
                "has_entity_name is a no-op in MQTT discovery and should not be published"
            );
        }
    }

    #[test]
    fn given_a_discovery_message_when_announced_then_it_names_its_origin() {
        // Mandatory for device discovery, not merely recommended.
        let (_, payload) = device_discovery();
        assert_eq!(payload["origin"]["name"], "m3i-ha-bridge");
        assert_eq!(payload["origin"]["sw_version"], env!("CARGO_PKG_VERSION"));
    }

    #[test]
    fn given_the_state_payload_when_read_by_the_templates_then_every_sensor_finds_its_field() {
        // Different code builds the discovery configs and the state
        // payload, so this test checks that the state payload publishes the
        // JSON key that each value_template reads.
        let payload = payload_of(KeiserStats::default());

        for spec in SENSORS {
            let field = spec
                .value_template
                .trim_start_matches("{{ value_json.")
                .trim_end_matches(" }}");
            assert!(
                !payload[field].is_null(),
                "{} reads value_json.{field}, which the state payload does not publish",
                spec.object_id
            );
        }
    }
}
