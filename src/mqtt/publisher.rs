//! Per-bike publishing: what to send for each bike heard, and when.

use std::collections::BTreeMap;
use std::time::Duration;

use tokio::time::Instant;

use super::connection::OutgoingMessage;
use super::discovery::{EXPIRE_AFTER_SECS, discovery_message, state_payload};
use super::topics::Topics;
use crate::stats::{BikeId, Fleet};

/// How often the publisher repeats the current state even when nothing
/// changes.
///
/// Half of [`EXPIRE_AFTER_SECS`], so a single dropped publish still leaves
/// a second heartbeat before Home Assistant would expire the entity.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(EXPIRE_AFTER_SECS as u64 / 2);

/// Decides when a state payload is worth publishing.
///
/// To repeat identical JSON every second would only be noise, so the gate
/// normally suppresses an unchanged payload — but not indefinitely. Every
/// sensor's discovery config carries `expire_after`, so Home Assistant
/// marks a sensor unavailable when no state arrives inside that window. An
/// idle bike produces exactly that case: after `sanitized()` zeroes the
/// live metrics, the payload stops changing and dedup suppresses every
/// publish. After `expire_after`, all sensors go unavailable while the
/// bridge is healthy and its availability topic still says `online`.
///
/// A heartbeat therefore bounds dedup: the gate republishes an unchanged
/// payload once per [`HEARTBEAT_INTERVAL`]. The design keeps `expire_after`
/// because its meaning stays exact: "no state arrived", which only occurs
/// when the bridge really stopped publishing.
struct PublishGate {
    last: Option<(String, Instant)>,
    heartbeat: Duration,
}

impl PublishGate {
    fn new(heartbeat: Duration) -> Self {
        Self {
            last: None,
            heartbeat,
        }
    }

    fn should_publish(&self, payload: &str, now: Instant) -> bool {
        match &self.last {
            None => true,
            Some((published, at)) => {
                published != payload || now.duration_since(*at) >= self.heartbeat
            }
        }
    }

    fn record_published(&mut self, payload: String, now: Instant) {
        self.last = Some((payload, now));
    }
}

/// What the publisher knows about one bike.
struct BikeChannel {
    gate: PublishGate,
    /// The availability that the publisher last sent for this bike, so it
    /// sends availability again only on a change.
    published_online: Option<bool>,
}

/// Converts fleet snapshots into per-bike state, availability and discovery
/// messages — one Home Assistant device per bike heard.
///
/// Pure with respect to the connection: it decides *what* to send, and `run`
/// sends it. That is what makes the per-bike behaviour testable without a
/// broker.
pub(super) struct BikePublisher {
    topics: Topics,
    bikes: BTreeMap<BikeId, BikeChannel>,
}

impl BikePublisher {
    pub(super) fn new(topics: Topics) -> Self {
        Self {
            topics,
            bikes: BTreeMap::new(),
        }
    }

    /// Announces any bike in `fleet` seen for the first time, so its device
    /// exists before its first state arrives.
    pub(super) fn observe(&mut self, fleet: &Fleet) -> Vec<OutgoingMessage> {
        let new_bikes: Vec<BikeId> = fleet
            .keys()
            .filter(|bike_id| !self.bikes.contains_key(bike_id))
            .copied()
            .collect();
        new_bikes
            .into_iter()
            .map(|bike_id| {
                tracing::info!("Bike {} heard for the first time; announcing it", bike_id);
                self.bikes.insert(
                    bike_id,
                    BikeChannel {
                        gate: PublishGate::new(HEARTBEAT_INTERVAL),
                        published_online: None,
                    },
                );
                let (topic, payload) = discovery_message(&self.topics, bike_id);
                OutgoingMessage::retained(topic, payload.to_string())
            })
            .collect()
    }

    /// Publishes every bike's availability and state through `publish`,
    /// which reports whether the channel queued the message. The gate
    /// records only a queued state, so the next tick retries a rejected
    /// state instead of suppressing it.
    pub(super) fn tick(
        &mut self,
        fleet: &Fleet,
        now: Instant,
        mut publish: impl FnMut(&OutgoingMessage) -> bool,
    ) {
        for (&bike_id, bike) in &mut self.bikes {
            let Some(reading) = fleet.get(&bike_id) else {
                continue; // nothing removes a bike from the fleet
            };
            let online = !reading.is_stale();
            if bike.published_online != Some(online)
                && publish(&availability_message(&self.topics, bike_id, online))
            {
                bike.published_online = Some(online);
            }

            let message = OutgoingMessage::state(
                self.topics.state(bike_id),
                state_payload(&reading.sanitized()).to_string(),
            );
            if bike.gate.should_publish(&message.payload, now) && publish(&message) {
                bike.gate.record_published(message.payload, now);
            }
        }
    }

    /// Forgets the published state, so the next tick sends every bike's
    /// availability and state again.
    ///
    /// The state loop calls this on every (re)connect. The broker can lose
    /// its retained messages (a restart without persistence, a migration).
    /// The driver announces discovery and the bridge availability again,
    /// but it knows nothing about per-bike liveness. Without this reset, a
    /// bike's `online` — sent only on a transition — would stay missing
    /// until the bike went stale and returned, and all its entities would
    /// stay unavailable.
    pub(super) fn reconnected(&mut self) {
        for bike in self.bikes.values_mut() {
            bike.published_online = None;
            bike.gate = PublishGate::new(HEARTBEAT_INTERVAL);
        }
    }

    /// The retained `offline` for every bike. The shutdown sends these
    /// before the bridge's own, so a Home Assistant that returns later sees
    /// each device as gone.
    pub(super) fn offline_messages(&self) -> Vec<OutgoingMessage> {
        self.bikes
            .keys()
            .map(|&bike_id| availability_message(&self.topics, bike_id, false))
            .collect()
    }
}

fn availability_message(topics: &Topics, bike_id: BikeId, online: bool) -> OutgoingMessage {
    OutgoingMessage::retained(
        topics.bike_availability(bike_id),
        if online { "online" } else { "offline" },
    )
}

#[cfg(test)]
mod tests {
    use super::super::test_support::*;
    use super::*;
    use rumqttc::QoS;

    fn publisher() -> BikePublisher {
        BikePublisher::new(test_topics())
    }

    /// Announces new bikes and runs one tick, accepting every message, and
    /// returns what the tick sent.
    fn tick_all(publisher: &mut BikePublisher, fleet: &Fleet) -> Vec<OutgoingMessage> {
        publisher.observe(fleet);
        let mut sent = Vec::new();
        publisher.tick(fleet, Instant::now(), |message| {
            sent.push(message.clone());
            true
        });
        sent
    }

    #[test]
    fn given_an_empty_fleet_when_observed_then_nothing_is_announced() {
        let mut publisher = publisher();
        assert!(publisher.observe(&Fleet::new()).is_empty());
        assert!(tick_all(&mut publisher, &Fleet::new()).is_empty());
    }

    #[test]
    fn given_a_bike_heard_for_the_first_time_when_observed_then_its_discovery_is_sent() {
        let mut publisher = publisher();
        let messages = publisher.observe(&fleet_of([reading_from(BIKE, 150)]));

        assert_eq!(messages.len(), 1, "one device-discovery message per bike");
        assert!(messages[0].retain);
        assert_eq!(
            messages[0].topic, "homeassistant/device/m3i-ha-bridge-042/config",
            "the config belongs to this bike's node"
        );

        assert!(
            publisher
                .observe(&fleet_of([reading_from(BIKE, 160)]))
                .is_empty(),
            "discovery is sent once per bike, not per reading"
        );
    }

    #[test]
    fn given_a_live_bike_when_ticked_then_it_is_online_and_its_state_is_on_its_own_topic() {
        let mut publisher = publisher();

        let sent = tick_all(&mut publisher, &fleet_of([reading_from(BIKE, 150)]));

        assert_eq!(
            sent[0],
            OutgoingMessage::retained("m3i/042/availability".into(), "online"),
            "availability precedes state, so HA never sees state for an offline bike"
        );
        assert_eq!(sent[1].topic, "m3i/042/state");
        assert!(!sent[1].retain);
        let state: serde_json::Value = serde_json::from_str(&sent[1].payload).unwrap();
        assert_eq!(state["bike_id"], 42);
        assert_eq!(state["power"], 150);
    }

    #[test]
    fn given_two_bikes_when_ticked_then_each_has_its_own_device_and_topics() {
        // The multi-bike room: readings alternate, and each bike keeps its own
        // last reading rather than the two overwriting each other.
        let mut publisher = publisher();
        let fleet = fleet_of([reading_from(1, 110), reading_from(2, 200)]);

        let sent = tick_all(&mut publisher, &fleet);
        let state_of = |topic: &str| -> serde_json::Value {
            let m = sent.iter().find(|m| m.topic == topic).unwrap();
            serde_json::from_str(&m.payload).unwrap()
        };
        assert_eq!(state_of("m3i/001/state")["power"], 110);
        assert_eq!(state_of("m3i/002/state")["power"], 200);
    }

    #[test]
    fn given_an_unchanged_reading_when_ticked_again_then_nothing_is_resent() {
        let mut publisher = publisher();
        let fleet = fleet_of([reading_from(BIKE, 150)]);
        tick_all(&mut publisher, &fleet);

        assert!(
            tick_all(&mut publisher, &fleet).is_empty(),
            "availability and state are both unchanged"
        );
    }

    #[test]
    fn given_a_bike_that_went_stale_when_ticked_then_it_goes_offline_and_its_metrics_zero() {
        let mut publisher = publisher();
        tick_all(&mut publisher, &fleet_of([reading_from(BIKE, 150)]));

        let sent = tick_all(&mut publisher, &fleet_of([stale_reading_from(BIKE, 150)]));

        assert_eq!(sent[0].topic, "m3i/042/availability");
        assert_eq!(sent[0].payload, "offline");
        let state: serde_json::Value = serde_json::from_str(&sent[1].payload).unwrap();
        assert_eq!(state["power"], 0);
        assert_eq!(state["bike_id"], 42, "identity survives staleness");
    }

    #[test]
    fn given_a_stale_bike_when_it_is_heard_again_then_it_comes_back_online() {
        let mut publisher = publisher();
        tick_all(&mut publisher, &fleet_of([reading_from(BIKE, 150)]));
        tick_all(&mut publisher, &fleet_of([stale_reading_from(BIKE, 150)]));

        let sent = tick_all(&mut publisher, &fleet_of([reading_from(BIKE, 90)]));

        assert_eq!(sent[0].payload, "online");
        assert!(sent[0].retain);
    }

    #[test]
    fn given_a_rejected_publish_when_ticked_again_then_it_is_retried() {
        // try_publish can refuse when the request channel is full. The gate
        // must not record the rejection as sent, or the reading stays lost
        // until the heartbeat.
        let mut publisher = publisher();
        let fleet = fleet_of([reading_from(BIKE, 150)]);
        publisher.observe(&fleet);
        publisher.tick(&fleet, Instant::now(), |_| false);

        let sent = tick_all(&mut publisher, &fleet);
        assert_eq!(sent.len(), 2, "availability and state both retried");
    }

    #[test]
    fn given_a_reconnect_when_ticked_then_every_bikes_availability_and_state_are_resent() {
        // The broker can lose its retained messages while the bridge is
        // away. The publisher sends a bike's `online` only on a transition,
        // so a reconnect has to forget the published state.
        let mut publisher = publisher();
        let fleet = fleet_of([reading_from(1, 100), reading_from(2, 200)]);
        tick_all(&mut publisher, &fleet);
        assert!(tick_all(&mut publisher, &fleet).is_empty(), "steady state");

        publisher.reconnected();
        let sent = tick_all(&mut publisher, &fleet);

        let topics: Vec<&str> = sent.iter().map(|m| m.topic.as_str()).collect();
        assert_eq!(
            topics,
            [
                "m3i/001/availability",
                "m3i/001/state",
                "m3i/002/availability",
                "m3i/002/state",
            ]
        );
        assert!(
            sent.iter()
                .all(|m| !m.topic.ends_with("availability") || m.payload == "online")
        );
    }

    #[test]
    fn given_known_bikes_when_shutting_down_then_each_gets_a_retained_offline() {
        let mut publisher = publisher();
        publisher.observe(&fleet_of([reading_from(1, 100), reading_from(2, 200)]));

        let offline = publisher.offline_messages();
        assert_eq!(
            offline,
            vec![
                OutgoingMessage::retained("m3i/001/availability".into(), "offline"),
                OutgoingMessage::retained("m3i/002/availability".into(), "offline"),
            ]
        );
    }

    #[test]
    fn given_the_messages_the_state_loop_sends_when_inspected_then_retained_ones_are_qos1() {
        // Retained discovery and availability must survive a loss on the
        // wire; the next reading supersedes state within seconds, so state
        // stays QoS 0.
        let mut publisher = publisher();
        let fleet = fleet_of([reading_from(BIKE, 150)]);
        let discovery = publisher.observe(&fleet);
        assert_eq!(discovery[0].qos, QoS::AtLeastOnce);
        let sent = tick_all(&mut publisher, &fleet);
        assert_eq!(sent[0].qos, QoS::AtLeastOnce, "availability");
        assert_eq!(sent[1].qos, QoS::AtMostOnce, "state");
        assert!(
            publisher
                .offline_messages()
                .iter()
                .all(|m| m.qos == QoS::AtLeastOnce)
        );
    }

    #[test]
    fn given_a_changed_payload_when_the_gate_is_asked_then_it_publishes_at_once() {
        let mut gate = PublishGate::new(HEARTBEAT_INTERVAL);
        let now = Instant::now();
        gate.record_published("{\"power\":0}".to_string(), now);
        assert!(gate.should_publish("{\"power\":150}", now));
    }

    #[tokio::test(start_paused = true)]
    async fn given_an_unchanged_payload_when_asked_again_soon_then_it_is_suppressed() {
        let mut gate = PublishGate::new(HEARTBEAT_INTERVAL);
        let published_at = Instant::now();
        gate.record_published("{\"power\":0}".to_string(), published_at);

        tokio::time::advance(HEARTBEAT_INTERVAL - Duration::from_secs(1)).await;
        assert!(
            !gate.should_publish("{\"power\":0}", Instant::now()),
            "an idle bike must not republish identical JSON every second"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_an_idle_bike_when_the_heartbeat_elapses_then_the_state_is_republished() {
        // After sanitized() zeroes the live metrics, the payload stops
        // changing. Without the heartbeat, dedup suppresses every publish,
        // and Home Assistant expires every sensor while the bridge is
        // healthy and still reports itself online.
        let mut gate = PublishGate::new(HEARTBEAT_INTERVAL);
        let idle = "{\"power\":0,\"cadence\":0.0}";
        gate.record_published(idle.to_string(), Instant::now());

        tokio::time::advance(HEARTBEAT_INTERVAL).await;

        assert!(gate.should_publish(idle, Instant::now()));
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_heartbeat_republish_when_it_succeeds_then_the_next_one_waits_again() {
        let mut gate = PublishGate::new(HEARTBEAT_INTERVAL);
        let idle = "{\"power\":0}";
        gate.record_published(idle.to_string(), Instant::now());

        tokio::time::advance(HEARTBEAT_INTERVAL).await;
        gate.record_published(idle.to_string(), Instant::now());

        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(
            !gate.should_publish(idle, Instant::now()),
            "the heartbeat should reset, not latch on"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_publish_that_failed_when_asked_again_then_it_is_retried() {
        // The gate only advances on a successful publish, so the next tick
        // offers a payload again after a full request channel rejects it,
        // instead of suppressing and losing it.
        let gate = PublishGate::new(HEARTBEAT_INTERVAL);
        let payload = "{\"power\":150}";
        assert!(gate.should_publish(payload, Instant::now()));
        // ... the publish fails, so nothing calls record_published ...
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(gate.should_publish(payload, Instant::now()));
    }

    #[test]
    fn given_the_heartbeat_when_compared_to_expiry_then_a_dropped_publish_still_has_a_margin() {
        // The invariant that makes the heartbeat sufficient: at least two
        // heartbeats must fit inside expire_after, so one lost heartbeat
        // does not expire the entities.
        let expire_after = Duration::from_secs(EXPIRE_AFTER_SECS as u64);
        assert!(
            HEARTBEAT_INTERVAL * 2 <= expire_after,
            "heartbeat {HEARTBEAT_INTERVAL:?} leaves no margin inside {expire_after:?}"
        );
    }
}
