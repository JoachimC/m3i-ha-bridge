//! Builders shared by the MQTT submodules' tests.

use std::collections::HashMap;

use rumqttc::AsyncClient;

use super::config::MqttConfig;
use super::topics::Topics;
use crate::stats::{BikeId, Fleet, KeiserStats, Reading, Tenths};

/// The bike most tests announce.
pub(super) const BIKE: BikeId = BikeId(42);

pub(super) fn reading_from(bike_id: impl Into<BikeId>, power: u16) -> Reading {
    Reading::now(KeiserStats {
        bike_id: bike_id.into(),
        power,
        cadence: Tenths(800),
        ..Default::default()
    })
}

pub(super) fn stale_reading_from(bike_id: impl Into<BikeId>, power: u16) -> Reading {
    Reading {
        received_at: std::time::Instant::now() - crate::stats::STALE_AFTER * 2,
        ..reading_from(bike_id, power)
    }
}

pub(super) fn fleet_of(readings: impl IntoIterator<Item = Reading>) -> Fleet {
    readings
        .into_iter()
        .map(|reading| (reading.stats.bike_id, reading))
        .collect()
}

pub(super) fn test_config() -> MqttConfig {
    let vars = HashMap::from([("MQTT_HOST", "broker.local")]);
    MqttConfig::from_lookup(
        |key| vars.get(key).map(|v| v.to_string()),
        |_path| Err(std::io::Error::from(std::io::ErrorKind::NotFound)),
    )
    .unwrap()
}

pub(super) fn test_topics() -> Topics {
    test_config().topics
}

/// Stands in for the event loop: `AsyncClient` is only a handle on
/// rumqttc's request channel, so a plain `flume` receiver sees exactly what
/// a publish enqueued, with no broker and no network.
pub(super) fn test_client(capacity: usize) -> (AsyncClient, flume::Receiver<rumqttc::Request>) {
    let (tx, rx) = flume::bounded(capacity);
    (AsyncClient::from_senders(tx), rx)
}

pub(super) fn queued_publishes(
    rx: &flume::Receiver<rumqttc::Request>,
) -> Vec<rumqttc::mqttbytes::v4::Publish> {
    rx.drain()
        .filter_map(|request| match request {
            rumqttc::Request::Publish(publish) => Some(publish),
            _ => None,
        })
        .collect()
}
