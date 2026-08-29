//! The broker connection: the event-loop driver, what is announced on each
//! connect, and the shutdown handshake.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use rumqttc::{AsyncClient, Event, EventLoop, Outgoing, Packet, QoS};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use super::discovery::discovery_message;
use super::topics::Topics;
use crate::stats::{BikeId, Fleet};

const RECONNECT_DELAY: Duration = Duration::from_secs(5);
/// Capacity of rumqttc's request channel — `AsyncClient::new`'s `cap` is
/// literally the size of the `flume::bounded` queue between the client handles
/// and the event loop.
///
/// It has to absorb a whole [`announce`] burst (availability plus one retained
/// discovery config per entity, per bike heard) alongside the state publishes
/// that queue up while the event loop is busy reconnecting. Beyond that
/// `try_publish` rejects rather than blocks, and a dropped retained discovery
/// config stays missing until the next reconnect. Zero would be a rendezvous
/// channel, where `try_publish` only succeeds if the event loop happens to be
/// parked at that exact instant.
///
/// Sized for two bursts at the worst case — every ordinal id 0–200 heard,
/// one device-discovery message each — because a flapping connection
/// announces twice in quick succession, plus one state publish per bike that
/// may be queued in between. flume grows its queue on demand, so the bound
/// costs nothing until it is used.
pub(super) const REQUEST_CHANNEL_CAPACITY: usize =
    BURSTS_IN_FLIGHT * ANNOUNCE_BURST + STATE_PUBLISHES_IN_FLIGHT;
/// Ordinal ids run 0–200 (see `doc/bluetooth-protocol.md`).
const MAX_BIKES: usize = 201;
/// One announce: the bridge availability plus one device config per bike.
const ANNOUNCE_BURST: usize = 1 + MAX_BIKES;
/// A flapping connection announces twice in quick succession.
const BURSTS_IN_FLIGHT: usize = 2;
/// One state publish per bike may be queued between the bursts.
const STATE_PUBLISHES_IN_FLIGHT: usize = MAX_BIKES;
/// Hard bound on the shutdown handshake: queue the retained `offline` message,
/// wait for the broker to acknowledge it, then DISCONNECT.
///
/// Well under systemd's default `TimeoutStopSec`, and it replaces an
/// unconditional 700 ms delay — so an ordinary shutdown now takes one round
/// trip rather than a fixed wait.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(3);

/// A message to send. Retained messages (discovery, availability) go out at
/// QoS 1 so a loss on the wire is retried; state is QoS 0 because the next
/// reading supersedes it within seconds anyway.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OutgoingMessage {
    pub topic: String,
    pub payload: String,
    pub retain: bool,
    pub qos: QoS,
}

impl OutgoingMessage {
    pub(super) fn retained(topic: String, payload: impl Into<String>) -> Self {
        Self {
            topic,
            payload: payload.into(),
            retain: true,
            qos: QoS::AtLeastOnce,
        }
    }

    pub(super) fn state(topic: String, payload: String) -> Self {
        Self {
            topic,
            payload,
            retain: false,
            qos: QoS::AtMostOnce,
        }
    }
}

/// Accounts for every QoS 1 publish queued for the event loop, from whichever
/// task queued it, so the driver can tell the shutdown's `offline` message
/// apart from the rest.
///
/// rumqttc assigns packet ids inside the event loop, so the offline message's
/// id can only be learned by watching the outgoing events. The request channel
/// is FIFO and ids are assigned in dequeue order, so the first outgoing QoS 1
/// publish that nobody has accounted for is the offline one — the one publish
/// that is deliberately not recorded here. Without the ledger, a SIGTERM
/// arriving while retained configs were still in flight would latch onto the
/// wrong packet id and wait for an acknowledgement that had already been and
/// gone.
#[derive(Debug, Clone, Default)]
pub(super) struct Qos1Ledger(Arc<AtomicUsize>);

impl Qos1Ledger {
    /// Records a QoS 1 publish about to be queued. Recorded *before* queuing:
    /// the driver may dequeue it before the caller runs again.
    fn queued(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    /// Undoes [`queued`](Self::queued) for a publish the channel rejected.
    fn not_queued(&self) {
        let _ = self
            .0
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1));
    }

    /// A connection error discards whatever was queued for the old
    /// connection, so their outgoing events will never arrive.
    fn forget_pending(&self) {
        self.0.store(0, Ordering::SeqCst);
    }

    /// Whether an outgoing QoS 1 publish is the shutdown's `offline` message
    /// rather than one someone recorded here.
    fn is_offline_publish(&self, shutting_down: bool) -> bool {
        let accounted_for = self
            .0
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok();
        !accounted_for && shutting_down
    }
}

/// Queues a message, reporting whether the channel accepted it.
pub(super) fn try_send(
    client: &AsyncClient,
    ledger: &Qos1Ledger,
    message: &OutgoingMessage,
) -> bool {
    let qos1 = message.qos != QoS::AtMostOnce;
    if qos1 {
        ledger.queued();
    }
    match client.try_publish(
        &message.topic,
        message.qos,
        message.retain,
        message.payload.clone(),
    ) {
        Ok(()) => true,
        Err(e) => {
            if qos1 {
                ledger.not_queued();
            }
            tracing::debug!("MQTT publish to {} skipped: {}", message.topic, e);
            false
        }
    }
}

/// Queues the retained `offline` message and waits for the driver to complete
/// the handshake it triggers.
///
/// The wait is on the driver task rather than on the connection, because the
/// driver owns the event loop and is the only thing that can poll it. Bounded
/// by [`SHUTDOWN_TIMEOUT`]: if the driver is stuck — mid-reconnect-sleep, say —
/// dropping the connection is better than hanging, and dropping it makes the
/// broker publish the last will, which carries the same retained `offline`
/// payload.
pub(super) async fn shutdown(
    client: &AsyncClient,
    topics: &Topics,
    mut driver: tokio::task::JoinHandle<()>,
) {
    // Queued here rather than inside the driver so it is strictly ordered after
    // the last state publish. Queuing it is also what wakes the event loop and
    // starts the handshake.
    if let Err(e) = client.try_publish(
        topics.bridge_availability(),
        QoS::AtLeastOnce,
        true,
        "offline",
    ) {
        // Nothing was queued, so there is nothing to confirm. Dropping the
        // connection instead lets the broker publish the will — the same
        // retained "offline" payload — which is the safer end state.
        tracing::warn!("Could not queue MQTT offline message ({e}); relying on the last will");
        driver.abort();
        return;
    }

    if tokio::time::timeout(SHUTDOWN_TIMEOUT, &mut driver)
        .await
        .is_err()
    {
        tracing::warn!(
            "MQTT shutdown handshake did not finish within {SHUTDOWN_TIMEOUT:?}; \
             dropping the connection"
        );
        driver.abort();
    }
}

/// Polls the event loop until cancellation, re-publishing availability and
/// discovery on every (re)connect, then runs the shutdown handshake.
///
/// Nothing here ever `select!`s against `eventloop.poll()`. That future is not
/// cancel-safe: dropping it mid-flush leaves a partially written packet in a
/// buffer that is never cleared. The offline publish queued by [`shutdown`]
/// wakes the parked poll by itself, so there is no need to race it.
pub(super) async fn drive_connection(
    mut eventloop: EventLoop,
    client: AsyncClient,
    topics: Topics,
    fleet_rx: watch::Receiver<Arc<Fleet>>,
    ledger: Qos1Ledger,
    connected: watch::Sender<u64>,
    cancel_token: CancellationToken,
) {
    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Packet::ConnAck(_))) if !cancel_token.is_cancelled() => {
                tracing::info!("Connected to MQTT broker");
                let bikes: BTreeSet<BikeId> = fleet_rx.borrow().keys().copied().collect();
                announce(&client, &ledger, &topics, &bikes);
                connected.send_modify(|generation| *generation += 1);
            }
            // A non-zero packet id means a QoS 1 publish reached the socket;
            // QoS 0 publishes always report packet id 0.
            Ok(Event::Outgoing(Outgoing::Publish(pkid))) if pkid != 0 => {
                if ledger.is_offline_publish(cancel_token.is_cancelled()) {
                    finish_shutdown(&mut eventloop, &client, pkid).await;
                    return;
                }
            }
            Ok(_) => {}
            Err(_) if cancel_token.is_cancelled() => return,
            Err(e) => {
                // Anything queued for the old connection is gone; the next
                // ConnAck will announce again.
                ledger.forget_pending();
                tracing::warn!("MQTT connection error: {e}. Retrying in {RECONNECT_DELAY:?}...");
                tokio::time::sleep(RECONNECT_DELAY).await;
            }
        }
    }
}

/// Waits for the broker to acknowledge the retained `offline` publish, then
/// sends DISCONNECT and waits for it to reach the socket.
///
/// The order matters. On receiving DISCONNECT the broker discards the last will
/// without publishing it (MQTT 3.1.1 [MQTT-3.14.4-3]), so disconnecting before
/// the offline message is acknowledged would suppress the safety net *and* lose
/// the message it was standing in for, leaving availability stuck at `online`
/// forever. On any error this just returns; dropping the connection instead
/// lets the will fire.
async fn finish_shutdown(eventloop: &mut EventLoop, client: &AsyncClient, offline_pkid: u16) {
    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Packet::PubAck(ack))) if ack.pkid == offline_pkid => break,
            Ok(_) => {}
            Err(e) => {
                tracing::warn!("MQTT offline message was not acknowledged: {e}");
                return;
            }
        }
    }

    if let Err(e) = client.try_disconnect() {
        tracing::warn!("Could not queue MQTT disconnect: {e}");
        return;
    }

    loop {
        match eventloop.poll().await {
            Ok(Event::Outgoing(Outgoing::Disconnect)) => {
                tracing::info!("Disconnected from MQTT broker");
                return;
            }
            Ok(_) => {}
            Err(e) => {
                tracing::warn!("MQTT disconnect was not sent: {e}");
                return;
            }
        }
    }
}

/// Publishes the bridge's availability and the Home Assistant device-discovery
/// config of every bike heard so far, returning how many messages were queued.
///
/// A bike's config is first published by the state loop the moment it is
/// heard; this repeats it on every reconnect, because anything queued for the
/// old connection is gone and a retained config the broker never received
/// stays missing.
///
/// Runs inside the connection driver, which is the only task polling the event
/// loop, so it must never await a publish. `client.publish(..).await` parks
/// until the request channel has room, and the only consumer of that channel is
/// the event loop's `next_request` — reachable only from `poll()`, i.e. from
/// the very task that would now be parked. That is a permanent deadlock, not a
/// slow path. `try_publish` never blocks, and [`REQUEST_CHANNEL_CAPACITY`] is
/// sized to hold a whole burst.
///
/// For the same reason `announce` is not spawned: a spawned task could
/// interleave its retained configs with a second reconnect's, and with adequate
/// capacity there is nothing for spawning to buy.
fn announce(
    client: &AsyncClient,
    ledger: &Qos1Ledger,
    topics: &Topics,
    bikes: &BTreeSet<BikeId>,
) -> usize {
    let mut messages = vec![OutgoingMessage::retained(
        topics.bridge_availability(),
        "online",
    )];
    messages.extend(bikes.iter().map(|&bike_id| {
        let (topic, payload) = discovery_message(topics, bike_id);
        OutgoingMessage::retained(topic, payload.to_string())
    }));

    let queued = messages
        .iter()
        .filter(|message| try_send(client, ledger, message))
        .count();
    if queued < messages.len() {
        tracing::warn!(
            "Only {} of {} MQTT announce messages could be queued",
            queued,
            messages.len()
        );
    }
    queued
}

#[cfg(test)]
mod tests {
    use super::super::publisher::BikePublisher;
    use super::super::test_support::*;
    use super::*;
    use tokio::time::Instant;

    #[test]
    fn given_a_reconnect_when_announcing_then_availability_and_every_discovery_config_are_queued() {
        let topics = test_topics();
        let (client, rx) = test_client(REQUEST_CHANNEL_CAPACITY);

        let queued = announce(
            &client,
            &Qos1Ledger::default(),
            &topics,
            &BTreeSet::from([BIKE]),
        );

        let expected = 2; // availability + one device config
        assert_eq!(queued, expected, "announce should report what it queued");
        let publishes = queued_publishes(&rx);
        assert_eq!(publishes.len(), expected);
        assert!(
            publishes.iter().all(|publish| publish.retain),
            "availability and discovery configs must be retained"
        );
    }

    #[test]
    fn given_the_request_channel_when_a_whole_announce_burst_arrives_then_none_is_dropped() {
        // The defect in issue #12: announce runs inside the poll task, so the
        // event loop is not draining while the burst is queued. With a channel
        // this burst can overflow, a dropped retained discovery config stays
        // missing until the next reconnect -- and try_publish only logs.
        let topics = test_topics();
        let (client, rx) = test_client(REQUEST_CHANNEL_CAPACITY);

        // The worst case: every ordinal id 0–200 has been heard. With per-bike
        // devices (issue #6) the burst scales with the bikes in range, and the
        // old capacity of 64 overflowed at four bikes.
        let bikes: BTreeSet<BikeId> = (0..=200).map(BikeId).collect();
        assert_eq!(
            bikes.len(),
            MAX_BIKES,
            "the constant must track the id range"
        );
        let burst = 1 + bikes.len();
        assert!(
            burst <= REQUEST_CHANNEL_CAPACITY,
            "an announce burst of {burst} cannot fit a channel of {REQUEST_CHANNEL_CAPACITY}"
        );

        // Two bursts back to back, as a flapping connection produces, still fit
        // in the channel.
        let ledger = Qos1Ledger::default();
        assert_eq!(announce(&client, &ledger, &topics, &bikes), burst);
        assert_eq!(announce(&client, &ledger, &topics, &bikes), burst);
        assert_eq!(queued_publishes(&rx).len(), burst * 2);
    }

    #[test]
    fn given_a_full_request_channel_when_announcing_then_it_reports_what_it_dropped() {
        // try_publish never blocks -- which is exactly why it is used inside
        // the poll task -- so an overflowing burst must be visible in the
        // return value rather than silently lost.
        let topics = test_topics();
        let (client, _rx) = test_client(3);

        assert_eq!(
            announce(
                &client,
                &Qos1Ledger::default(),
                &topics,
                &BTreeSet::from([BikeId(1), BikeId(2), BikeId(3), BikeId(4)])
            ),
            3,
            "only what fits is queued"
        );
    }

    #[tokio::test]
    async fn given_a_shutdown_when_it_runs_then_it_queues_one_retained_offline_message() {
        // The message the whole handshake exists to deliver: retained, so a
        // Home Assistant restarting later still learns the bridge is gone, and
        // QoS 1, so there is an acknowledgement to wait for.
        let topics = test_topics();
        let (client, rx) = test_client(REQUEST_CHANNEL_CAPACITY);
        let driver = tokio::spawn(async {});

        shutdown(&client, &topics, driver).await;

        let publishes = queued_publishes(&rx);
        assert_eq!(publishes.len(), 1);
        assert_eq!(publishes[0].topic, "m3i/availability");
        assert_eq!(publishes[0].payload, "offline");
        assert!(publishes[0].retain, "a later HA restart must still see it");
        assert_eq!(publishes[0].qos, QoS::AtLeastOnce, "needed for the PubAck");
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_driver_that_never_finishes_when_shutting_down_then_it_gives_up_and_aborts() {
        // A driver parked in its reconnect sleep never sees the offline message,
        // so the handshake cannot complete. Giving up and dropping the
        // connection is the right outcome: the broker then publishes the last
        // will, which carries the same retained "offline" payload.
        let topics = test_topics();
        let (client, _rx) = test_client(REQUEST_CHANNEL_CAPACITY);
        let driver = tokio::spawn(std::future::pending::<()>());
        let handle = driver.abort_handle();

        let start = Instant::now();
        shutdown(&client, &topics, driver).await;

        assert_eq!(start.elapsed(), SHUTDOWN_TIMEOUT, "bounded, not indefinite");
        // abort() only schedules the cancellation; let the runtime apply it.
        tokio::task::yield_now().await;
        assert!(
            handle.is_finished(),
            "the connection must be dropped, not left hanging"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_the_offline_message_cannot_be_queued_when_shutting_down_then_the_will_takes_over()
     {
        let topics = test_topics();
        // A rendezvous channel with no receiver waiting: try_publish fails.
        let (client, _rx) = test_client(0);
        let driver = tokio::spawn(std::future::pending::<()>());
        let handle = driver.abort_handle();

        let start = Instant::now();
        shutdown(&client, &topics, driver).await;

        assert_eq!(
            start.elapsed(),
            Duration::ZERO,
            "with nothing queued there is nothing to wait for"
        );
        tokio::task::yield_now().await;
        assert!(
            handle.is_finished(),
            "drop the connection so the broker publishes the will instead"
        );
    }

    #[test]
    fn given_publishes_still_in_flight_when_shutting_down_then_they_are_not_mistaken_for_offline() {
        // Packet ids are assigned inside the event loop, so the offline
        // message can only be identified by counting. A SIGTERM arriving while
        // retained configs are still in flight must not latch onto one of
        // their ids -- the handshake would then wait for an acknowledgement
        // that had already been and gone.
        let ledger = Qos1Ledger::default();
        for _ in 0..8 {
            ledger.queued();
        }
        for remaining in (1..=8).rev() {
            assert!(
                !ledger.is_offline_publish(true),
                "an accounted-for publish is not the offline message ({remaining} left)"
            );
        }
        assert!(ledger.is_offline_publish(true), "this one is");
    }

    #[test]
    fn given_no_shutdown_in_progress_when_a_qos1_publish_goes_out_then_it_is_not_the_offline_message()
     {
        assert!(!Qos1Ledger::default().is_offline_publish(false));
    }

    #[test]
    fn given_a_connection_error_when_pending_publishes_are_forgotten_then_attribution_recovers() {
        // A connection error discards whatever was queued for the old
        // connection. Otherwise the driver would skip that many outgoing
        // publishes on the new connection and sail past the offline message.
        let ledger = Qos1Ledger::default();
        for _ in 0..8 {
            ledger.queued();
        }
        assert!(!ledger.is_offline_publish(true));

        ledger.forget_pending();
        assert!(ledger.is_offline_publish(true));
    }

    #[test]
    fn given_qos1_publishes_from_the_state_loop_when_shutting_down_then_the_offline_is_still_found()
    {
        // The reason the ledger is shared: the state loop queues retained
        // discovery and availability at QoS 1 too, so the driver alone cannot
        // count what is ahead of the offline message.
        let (client, _rx) = test_client(REQUEST_CHANNEL_CAPACITY);
        let ledger = Qos1Ledger::default();
        let topics = test_topics();

        announce(&client, &ledger, &topics, &BTreeSet::from([BikeId(1)]));
        let mut publisher = BikePublisher::new(topics);
        let fleet = fleet_of([reading_from(2, 100)]);
        for message in publisher.observe(&fleet) {
            try_send(&client, &ledger, &message);
        }
        publisher.tick(&fleet, Instant::now(), |message| {
            try_send(&client, &ledger, message)
        });
        // bridge availability + 1 config from announce, 1 config + 1
        // availability from the state loop; the state publish is QoS 0.
        for _ in 0..4 {
            assert!(!ledger.is_offline_publish(true));
        }
        assert!(ledger.is_offline_publish(true));
    }

    #[test]
    fn given_a_rejected_qos1_publish_when_sent_then_it_is_not_counted() {
        let (client, _rx) = test_client(0);
        let ledger = Qos1Ledger::default();
        let message = OutgoingMessage::retained("t".into(), "p");

        assert!(!try_send(&client, &ledger, &message));
        assert!(ledger.is_offline_publish(true), "nothing is accounted for");
    }
}
