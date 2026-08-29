use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rumqttc::{AsyncClient, Event, EventLoop, LastWill, MqttOptions, Outgoing, Packet, QoS};
use serde_json::json;
use tokio::sync::watch;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::stats::{KeiserStats, bike_display_name, bike_id_label, next_reading};

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
const REQUEST_CHANNEL_CAPACITY: usize = 2 * (1 + MAX_BIKES) + MAX_BIKES;
/// Ordinal ids run 0–200 (see `doc/bluetooth-protocol.md`).
const MAX_BIKES: usize = 201;
/// Hard bound on the shutdown handshake: queue the retained `offline` message,
/// wait for the broker to acknowledge it, then DISCONNECT.
///
/// Well under systemd's default `TimeoutStopSec`, and it replaces an
/// unconditional 700 ms delay — so an ordinary shutdown now takes one round
/// trip rather than a fixed wait.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(3);
/// How often the state loop re-evaluates even when no new advertisement
/// arrived, so a reading going stale is published rather than waited on.
const STATE_POLL_INTERVAL: Duration = Duration::from_secs(1);
/// Home Assistant marks sensors unavailable if no state arrives within this window.
const EXPIRE_AFTER_SECS: u32 = 120;
/// How often the current state is republished even when nothing has changed.
///
/// Half of [`EXPIRE_AFTER_SECS`], so a single dropped publish still leaves a
/// second heartbeat before Home Assistant would expire the entity.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(EXPIRE_AFTER_SECS as u64 / 2);
/// Credential name declared by `LoadCredential=mqtt-password` in the systemd
/// unit. systemd copies the file into a private ramfs and points
/// `$CREDENTIALS_DIRECTORY` at it, so the password never enters the environment.
const PASSWORD_CREDENTIAL: &str = "mqtt-password";

#[derive(Debug, Clone, PartialEq)]
pub struct MqttConfig {
    pub host: String,
    pub port: u16,
    pub username: Option<String>,
    pub password: Option<String>,
    pub client_id: String,
    pub topic_prefix: String,
    pub discovery_prefix: String,
}

impl MqttConfig {
    /// Reads configuration from the environment. Returns `None` when `MQTT_HOST`
    /// is unset, which disables MQTT publishing entirely.
    pub fn from_env() -> Option<Self> {
        Self::from_lookup(
            |key| std::env::var(key).ok(),
            |path: &Path| std::fs::read_to_string(path),
        )
    }

    /// The environment lookup and the file reads are both injected so tests
    /// stay hermetic: no process environment, no disk.
    pub fn from_lookup(
        lookup: impl Fn(&str) -> Option<String>,
        read_file: impl Fn(&Path) -> io::Result<String>,
    ) -> Option<Self> {
        let host = lookup("MQTT_HOST").filter(|v| !v.is_empty())?;
        let port = lookup("MQTT_PORT")
            .and_then(|v| v.parse().ok())
            .unwrap_or(1883);
        Some(MqttConfig {
            host,
            port,
            // An empty value and an unset one are deliberately equivalent for
            // every optional credential: commenting a line out of
            // /etc/default/m3i-ha-bridge and blanking its value must behave
            // the same way.
            username: lookup("MQTT_USERNAME").filter(|v| !v.is_empty()),
            password: resolve_password(&lookup, &read_file),
            client_id: lookup("MQTT_CLIENT_ID").unwrap_or_else(|| "m3i-ha-bridge".to_string()),
            topic_prefix: lookup("MQTT_TOPIC_PREFIX").unwrap_or_else(|| "m3i".to_string()),
            discovery_prefix: lookup("MQTT_DISCOVERY_PREFIX")
                .unwrap_or_else(|| "homeassistant".to_string()),
        })
    }

    /// Where one bike's readings go: `<prefix>/<id>/state`.
    pub fn state_topic(&self, bike_id: u8) -> String {
        format!("{}/{}/state", self.topic_prefix, bike_id_label(bike_id))
    }

    /// Whether the *bridge* is running: the last will lives here, so a crash
    /// takes every bike's entities offline at once.
    pub fn bridge_availability_topic(&self) -> String {
        format!("{}/availability", self.topic_prefix)
    }

    /// Whether one *bike* is being heard: `offline` once its readings go
    /// stale, so a bike that has been switched off greys out in Home Assistant
    /// while the bridge, and the other bikes, stay online.
    pub fn bike_availability_topic(&self, bike_id: u8) -> String {
        format!(
            "{}/{}/availability",
            self.topic_prefix,
            bike_id_label(bike_id)
        )
    }

    /// Node id used in Home Assistant discovery topics; must not contain '/'.
    ///
    /// Deliberately independent of the topic prefix: the same physical bike
    /// heard by two bridges on one broker is one device, not two.
    fn node_id(bike_id: u8) -> String {
        format!("m3i-ha-bridge-{}", bike_id_label(bike_id))
    }
}

/// A message to send. Retained messages (discovery, availability) go out at
/// QoS 1 so a loss on the wire is retried; state is QoS 0 because the next
/// reading supersedes it within seconds anyway.
#[derive(Debug, Clone, PartialEq)]
pub struct OutgoingMessage {
    pub topic: String,
    pub payload: String,
    pub retain: bool,
    pub qos: QoS,
}

impl OutgoingMessage {
    fn retained(topic: String, payload: impl Into<String>) -> Self {
        Self {
            topic,
            payload: payload.into(),
            retain: true,
            qos: QoS::AtLeastOnce,
        }
    }

    fn state(topic: String, payload: String) -> Self {
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
struct Qos1Ledger(Arc<AtomicUsize>);

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

/// What the publisher knows about one bike.
struct BikeChannel {
    stats: KeiserStats,
    gate: PublishGate,
    /// The availability last published for this bike, so it is only re-sent
    /// when it changes.
    published_online: Option<bool>,
}

/// Turns the single stream of readings into per-bike state, availability and
/// discovery messages — one Home Assistant device per bike heard.
///
/// Pure with respect to the connection: it decides *what* to send, and `run`
/// sends it. That is what makes the per-bike behaviour testable without a
/// broker.
pub struct BikePublisher {
    config: MqttConfig,
    bikes: BTreeMap<u8, BikeChannel>,
    /// Shared with the connection driver, which re-announces discovery for
    /// every known bike on each reconnect.
    known_bikes: Arc<Mutex<BTreeSet<u8>>>,
}

impl BikePublisher {
    fn new(config: MqttConfig, known_bikes: Arc<Mutex<BTreeSet<u8>>>) -> Self {
        Self {
            config,
            bikes: BTreeMap::new(),
            known_bikes,
        }
    }

    /// Records a reading. A bike seen for the first time gets its discovery
    /// configs, so its device exists before its first state arrives.
    fn observe(&mut self, stats: KeiserStats) -> Vec<OutgoingMessage> {
        let Some(bike_id) = stats.bike_id() else {
            return Vec::new(); // the channel's initial value: nothing heard yet
        };
        match self.bikes.get_mut(&bike_id) {
            Some(bike) => {
                bike.stats = stats;
                Vec::new()
            }
            None => {
                tracing::info!("Bike {} heard for the first time; announcing it", bike_id);
                self.bikes.insert(
                    bike_id,
                    BikeChannel {
                        stats,
                        gate: PublishGate::new(HEARTBEAT_INTERVAL),
                        published_online: None,
                    },
                );
                self.known_bikes.lock().unwrap().insert(bike_id);
                let (topic, payload) = discovery_message(&self.config, bike_id);
                vec![OutgoingMessage::retained(topic, payload.to_string())]
            }
        }
    }

    /// Publishes every bike's availability and state through `publish`, which
    /// reports whether the message was actually queued. Only a queued state is
    /// recorded, so a rejected one is retried on the next tick rather than
    /// deduplicated away.
    fn tick(&mut self, now: Instant, mut publish: impl FnMut(&OutgoingMessage) -> bool) {
        for (&bike_id, bike) in &mut self.bikes {
            let online = !bike.stats.is_stale();
            if bike.published_online != Some(online) {
                let message = OutgoingMessage::retained(
                    self.config.bike_availability_topic(bike_id),
                    if online { "online" } else { "offline" },
                );
                if publish(&message) {
                    bike.published_online = Some(online);
                }
            }

            let payload = state_payload(&bike.stats.clone().sanitized()).to_string();
            if !bike.gate.should_publish(&payload, now) {
                continue;
            }
            let message = OutgoingMessage::state(self.config.state_topic(bike_id), payload);
            if publish(&message) {
                bike.gate.record_published(message.payload, now);
            }
        }
    }

    /// Forgets what has been published, so the next tick re-sends every bike's
    /// availability and state.
    ///
    /// Called on every (re)connect. The broker may have lost its retained
    /// messages (a restart without persistence, a migration), and the driver
    /// re-announces discovery and the bridge availability but knows nothing
    /// about per-bike liveness; without this, a bike's `online` — published
    /// only on a transition — would stay missing until the bike happened to
    /// go stale and come back, leaving all its entities unavailable.
    fn reconnected(&mut self) {
        for bike in self.bikes.values_mut() {
            bike.published_online = None;
            bike.gate = PublishGate::new(HEARTBEAT_INTERVAL);
        }
    }

    /// The retained `offline` for every bike, sent ahead of the bridge's own
    /// so a Home Assistant that comes back later sees each device as gone.
    fn offline_messages(&self) -> Vec<OutgoingMessage> {
        self.bikes
            .keys()
            .map(|&bike_id| {
                OutgoingMessage::retained(self.config.bike_availability_topic(bike_id), "offline")
            })
            .collect()
    }
}

/// Queues a message, reporting whether the channel accepted it.
fn try_send(client: &AsyncClient, ledger: &Qos1Ledger, message: &OutgoingMessage) -> bool {
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

/// Resolves the broker password, in order of precedence:
///
/// 1. `MQTT_PASSWORD` — a plain environment variable, for dev and local runs;
/// 2. the file named by `MQTT_PASSWORD_FILE` — the Docker `*_FILE` secret
///    convention, which also works outside systemd;
/// 3. `$CREDENTIALS_DIRECTORY/mqtt-password` — the systemd credential loaded by
///    `LoadCredential=mqtt-password` (see `install-service.sh`).
///
/// The credential is the one the deployment uses, because an environment
/// variable is the wrong place for a secret on this box: it is readable through
/// `/proc/<pid>/environ` and inherited by every child process, and the bridge
/// execs `btmgmt`. systemd instead copies the credential into a private,
/// unswappable directory only this unit can read.
///
/// An empty value at any step counts as unset, exactly as for `MQTT_USERNAME`.
fn resolve_password(
    lookup: &impl Fn(&str) -> Option<String>,
    read_file: &impl Fn(&Path) -> io::Result<String>,
) -> Option<String> {
    if let Some(password) = lookup("MQTT_PASSWORD").filter(|v| !v.is_empty()) {
        return Some(password);
    }

    if let Some(path) = lookup("MQTT_PASSWORD_FILE").filter(|v| !v.is_empty()) {
        // Explicitly configured, so a missing file is worth complaining about.
        return read_password(read_file, Path::new(&path), true);
    }

    let credentials_dir = lookup("CREDENTIALS_DIRECTORY").filter(|v| !v.is_empty())?;
    let path = Path::new(&credentials_dir).join(PASSWORD_CREDENTIAL);
    // systemd exports CREDENTIALS_DIRECTORY whenever the unit declares any
    // credential, and skips a missing one silently, so absence is normal here.
    read_password(read_file, &path, false)
}

fn read_password(
    read_file: &impl Fn(&Path) -> io::Result<String>,
    path: &Path,
    explicitly_configured: bool,
) -> Option<String> {
    match read_file(path) {
        Ok(contents) => {
            // systemd copies credential files through byte for byte, adding and
            // stripping nothing, so a trailing newline left by `echo` or an
            // editor would otherwise become part of the password. Strip it the
            // way Docker's own `file_env` helper does. Interior whitespace is
            // preserved — it may well be part of the password.
            let password = contents.trim_end_matches(['\r', '\n']);
            if password.is_empty() {
                tracing::warn!("MQTT password file {} is empty; ignoring", path.display());
                return None;
            }
            Some(password.to_string())
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound && !explicitly_configured => {
            tracing::debug!("No MQTT password credential at {}", path.display());
            None
        }
        Err(e) => {
            tracing::warn!(
                "Failed to read MQTT password from {}: {}",
                path.display(),
                e
            );
            None
        }
    }
}

/// Decides when a state payload is worth publishing.
///
/// Republishing identical JSON every second would be pure noise, so an
/// unchanged payload is normally suppressed — but not indefinitely. Every
/// sensor's discovery config carries `expire_after`, so Home Assistant marks it
/// unavailable when no state arrives inside that window. An idle bike produces
/// exactly that: once `sanitized()` has zeroed the live metrics the payload
/// stops changing, dedup suppresses everything, and after `expire_after` the
/// sensors all go unavailable while the bridge is perfectly healthy and its
/// availability topic still says `online`.
///
/// So dedup is bounded by a heartbeat: an unchanged payload is republished once
/// per [`HEARTBEAT_INTERVAL`]. Keeping `expire_after` rather than dropping it is
/// the point — it still means what it says, "no state has arrived", which now
/// only happens when the bridge really has stopped publishing.
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

    /// Recorded only after a publish actually succeeds, so a rejected publish
    /// is retried on the next tick rather than being deduplicated away.
    fn record_published(&mut self, payload: String, now: Instant) {
        self.last = Some((payload, now));
    }
}

/// Publishes bike state to MQTT until cancelled.
///
/// Failure policy: retry forever, internally. rumqttc's event loop reconnects
/// on its own and the driver re-announces on every ConnAck, so a broker being
/// down is an expected condition rather than a reason to stop — the bridge's
/// BLE half must keep working regardless. The only abnormal exit is the stats
/// producer disappearing, which means the process is already broken and is
/// reported as an error so the exit status says so.
pub async fn run(
    cancel_token: CancellationToken,
    mut stats_rx: watch::Receiver<KeiserStats>,
    config: MqttConfig,
) -> Result<(), crate::BoxError> {
    tracing::info!(
        "Starting MQTT publisher for broker {}:{} (topic prefix '{}')",
        config.host,
        config.port,
        config.topic_prefix
    );

    let mut options = MqttOptions::new(&config.client_id, &config.host, config.port);
    options.set_keep_alive(Duration::from_secs(30));
    if let Some(username) = &config.username {
        options.set_credentials(username, config.password.as_deref().unwrap_or(""));
    }
    options.set_last_will(LastWill::new(
        config.bridge_availability_topic(),
        "offline",
        QoS::AtLeastOnce,
        true,
    ));

    let (client, eventloop) = AsyncClient::new(options, REQUEST_CHANNEL_CAPACITY);

    let known_bikes = Arc::new(Mutex::new(BTreeSet::new()));
    let ledger = Qos1Ledger::default();
    // Bumped by the driver on every ConnAck, so the state loop can re-send
    // what the broker may have lost.
    let (connected_tx, mut connected_rx) = watch::channel(0u64);

    // The connection driver is the sole poller of the event loop, which is also
    // what performs reconnects. Nothing else can drive the connection, so it
    // owns the shutdown handshake too; this task returning is the signal that
    // the handshake is done.
    let driver = tokio::spawn(drive_connection(
        eventloop,
        client.clone(),
        config.clone(),
        known_bikes.clone(),
        ledger.clone(),
        connected_tx,
        cancel_token.clone(),
    ));

    let mut publisher = BikePublisher::new(config.clone(), known_bikes);

    let mut lost_producer = false;
    loop {
        let reading = tokio::select! {
            _ = cancel_token.cancelled() => break,
            reading = next_reading(&mut stats_rx, STATE_POLL_INTERVAL) => reading,
        };
        let Some(stats) = reading else {
            // The Bluetooth reader is the only producer, so losing it means
            // there will never be another reading. Still shut down tidily —
            // the retained "offline" message matters more than the exit code —
            // but report it once the handshake is done.
            lost_producer = true;
            break;
        };

        if connected_rx.has_changed().unwrap_or(false) {
            connected_rx.borrow_and_update();
            publisher.reconnected();
        }
        for message in publisher.observe(stats) {
            try_send(&client, &ledger, &message);
        }
        publisher.tick(Instant::now(), |message| {
            try_send(&client, &ledger, message)
        });
    }

    tracing::info!("Shutting down MQTT publisher...");
    for message in publisher.offline_messages() {
        try_send(&client, &ledger, &message);
    }
    shutdown(&client, &config, driver).await;

    if lost_producer {
        return Err("the Bluetooth reader stopped producing stats".into());
    }
    Ok(())
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
async fn shutdown(
    client: &AsyncClient,
    config: &MqttConfig,
    mut driver: tokio::task::JoinHandle<()>,
) {
    // Queued here rather than inside the driver so it is strictly ordered after
    // the last state publish. Queuing it is also what wakes the event loop and
    // starts the handshake.
    if let Err(e) = client.try_publish(
        config.bridge_availability_topic(),
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
async fn drive_connection(
    mut eventloop: EventLoop,
    client: AsyncClient,
    config: MqttConfig,
    known_bikes: Arc<Mutex<BTreeSet<u8>>>,
    ledger: Qos1Ledger,
    connected: watch::Sender<u64>,
    cancel_token: CancellationToken,
) {
    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Packet::ConnAck(_))) if !cancel_token.is_cancelled() => {
                tracing::info!("Connected to MQTT broker");
                let bikes = known_bikes.lock().unwrap().clone();
                announce(&client, &ledger, &config, &bikes);
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
    config: &MqttConfig,
    bikes: &BTreeSet<u8>,
) -> usize {
    let mut messages = vec![OutgoingMessage::retained(
        config.bridge_availability_topic(),
        "online",
    )];
    messages.extend(bikes.iter().map(|&bike_id| {
        let (topic, payload) = discovery_message(config, bike_id);
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

/// Rounds to the bike's native resolution of 0.1, which is also what makes the
/// value print cleanly.
///
/// `KeiserStats` holds cadence, heart rate and distance as `f32` because the
/// packet carries them as `value * 10` in a `u16`. `serde_json` widens an `f32`
/// to `f64` and then prints the shortest string that round-trips the *`f64`*,
/// which re-exposes the binary approximation: the real capture's 50.2 rpm
/// becomes `50.20000076293945` and 0.1 km becomes `0.10000000149011612`. Home
/// Assistant renders the state string verbatim, so that noise reaches the
/// dashboard.
///
/// Rounding here rather than setting the discovery payload's
/// `suggested_display_precision` is deliberate: that only affects how Home
/// Assistant displays a value, leaving the raw state, templates and automations
/// to deal with the noise.
fn round_to_native_resolution(value: f32) -> f64 {
    (value as f64 * 10.0).round() / 10.0
}

fn state_payload(stats: &KeiserStats) -> serde_json::Value {
    json!({
        "bike_id": stats.bike_id,
        "version": stats.version,
        "power": stats.power,
        "cadence": round_to_native_resolution(stats.cadence),
        "heart_rate": round_to_native_resolution(stats.heart_rate),
        "gear": stats.gear,
        "distance": round_to_native_resolution(stats.distance),
        "distance_unit": "Km",
        "energy": stats.energy,
        "energy_unit": "KCal",
        "elapsed_seconds": stats.elapsed_seconds(),
        "is_paused": stats.is_paused,
    })
}

/// Everything that varies between the sensors announced to Home Assistant.
///
/// A struct rather than more positional arguments: with `state_class` and
/// `precision` added, a closure would take eight parameters, five of them
/// `Option<&str>`, and every call site would be an unreadable run of `None`s.
struct SensorSpec {
    object_id: &'static str,
    name: &'static str,
    value_template: &'static str,
    unit: Option<&'static str>,
    device_class: Option<&'static str>,
    /// Drives Home Assistant's long-term statistics. Without it a sensor is
    /// recorded in history but never aggregated, so none of these entities had
    /// any long-term data at all.
    state_class: Option<&'static str>,
    /// Display only — the state itself is already rounded to the bike's
    /// resolution when the payload is built.
    precision: Option<u8>,
    icon: Option<&'static str>,
    /// `diagnostic` moves an entity out of the device's main sensor list into
    /// its Diagnostic section — right for identity, wrong for a reading.
    entity_category: Option<&'static str>,
}

/// Home Assistant metadata for each published sensor.
///
/// The device-class choices are constrained, not free: `power` accepts only
/// `measurement`, and `energy` only `total`/`total_increasing`. Where Home
/// Assistant allows a choice it is semantic — `total_increasing` for distance
/// and energy, which accumulate through a ride and reset to zero on the next
/// one (exactly the case that state class exists for), and `measurement` for
/// elapsed time, where the useful statistic is the live value rather than a
/// lifetime sum of seconds.
///
/// Heart rate and cadence have no device class in Home Assistant; `bpm` and
/// `rpm` are accepted as free-form units.
const SENSORS: &[SensorSpec] = &[
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
        // Home Assistant's energy device class has accepted cal/kcal/Mcal/Gcal
        // since 2024.10, which is what makes this valid. On an older Home
        // Assistant the entity would be rejected outright — drop the device
        // class if you need to support one; the unit and state class alone
        // still give long-term statistics.
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
    // nothing to aggregate), and filed under Diagnostic so it does not sit
    // between Power and Cadence on the dashboard.
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
/// carrying every entity, so sensors appear automatically without any YAML on
/// the HA side.
///
/// Device discovery (`<prefix>/device/<node_id>/config` with a `components`
/// map; Home Assistant 2024.11+) rather than one retained topic per entity.
/// Issue #5 recorded why that was not worth a migration for a single bike; the
/// per-bike devices of issue #6 are brand-new node ids with nothing to migrate
/// from, so they were made device-based from their first publish. The
/// per-entity topics of releases before per-bike devices are cleared by hand —
/// see the README.
///
/// `state_topic`, `availability` and `availability_mode` are shared at the
/// root and inherited by every component; `expire_after` is a per-entity
/// option and so is repeated in each. `device` and `origin` are mandatory
/// here, not merely recommended.
///
/// # Entity naming
///
/// Each component announces a short `name` ("Power") plus the shared `device`
/// block, and Home Assistant composes the two: friendly name "Keiser M3i #042
/// Power", entity id `sensor.keiser_m3i_042_power`. There is no bare
/// `sensor.power` to collide with anything else on the instance, and two bikes
/// never collide with each other.
///
/// Do **not** add `"has_entity_name": true` to these payloads. It is not an
/// MQTT discovery option — the MQTT integration sets `_attr_has_entity_name`
/// unconditionally on every entity (since Home Assistant 2023.8), and the
/// discovery schema uses `extra=vol.REMOVE_EXTRA`, so the key is silently
/// discarded. Adding it would look like it did something while changing
/// nothing.
fn discovery_message(config: &MqttConfig, bike_id: u8) -> (String, serde_json::Value) {
    let node_id = MqttConfig::node_id(bike_id);
    let device = json!({
        "identifiers": [format!("m3i_ha_bridge_{}", bike_id_label(bike_id))],
        "name": bike_display_name(bike_id),
        "manufacturer": "Keiser",
        "model": "M3i",
    });
    // Both must say online: the bridge's topic carries the last will, so a
    // crash takes every bike down; the bike's own topic goes offline when its
    // readings go stale, so a bike that is switched off greys out on its own.
    let availability = json!([
        { "topic": config.bridge_availability_topic() },
        { "topic": config.bike_availability_topic(bike_id) },
    ]);
    let origin = json!({
        "name": env!("CARGO_PKG_NAME"),
        "sw_version": env!("CARGO_PKG_VERSION"),
        "support_url": "https://github.com/JoachimC/m3i-ha-bridge",
    });

    let mut components = serde_json::Map::new();
    for spec in SENSORS {
        let mut component = json!({
            "platform": "sensor",
            "name": spec.name,
            "unique_id": format!("{}_{}", node_id, spec.object_id),
            "value_template": spec.value_template,
            "expire_after": EXPIRE_AFTER_SECS,
        });
        let obj = component.as_object_mut().unwrap();
        if let Some(unit) = spec.unit {
            obj.insert("unit_of_measurement".into(), json!(unit));
        }
        if let Some(device_class) = spec.device_class {
            obj.insert("device_class".into(), json!(device_class));
        }
        if let Some(state_class) = spec.state_class {
            obj.insert("state_class".into(), json!(state_class));
        }
        if let Some(precision) = spec.precision {
            obj.insert("suggested_display_precision".into(), json!(precision));
        }
        if let Some(icon) = spec.icon {
            obj.insert("icon".into(), json!(icon));
        }
        if let Some(entity_category) = spec.entity_category {
            obj.insert("entity_category".into(), json!(entity_category));
        }
        components.insert(spec.object_id.to_string(), component);
    }
    components.insert(
        "paused".to_string(),
        json!({
            "platform": "binary_sensor",
            "name": "Paused",
            "unique_id": format!("{}_paused", node_id),
            "value_template": "{{ 'ON' if value_json.is_paused else 'OFF' }}",
            // Same expiry as the sensors. Without it this entity stayed live
            // while every sensor went unavailable, so the device contradicted
            // itself about whether the bike was reachable.
            "expire_after": EXPIRE_AFTER_SECS,
        }),
    );

    (
        format!("{}/device/{}/config", config.discovery_prefix, node_id),
        json!({
            "device": device,
            "origin": origin,
            "state_topic": config.state_topic(bike_id),
            "availability": availability,
            "availability_mode": "all",
            "components": components,
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use hex_literal::hex;
    use std::collections::HashMap;

    fn lookup_from<'a>(map: &'a HashMap<&'a str, &'a str>) -> impl Fn(&str) -> Option<String> + 'a {
        move |key| map.get(key).map(|v| v.to_string())
    }

    /// In-memory stand-in for the filesystem: a path not in the map reads as
    /// `NotFound`, which is what both the `*_FILE` and credential paths see
    /// when nothing has been configured.
    fn reader_from<'a>(
        files: &'a HashMap<&'a str, &'a str>,
    ) -> impl Fn(&Path) -> io::Result<String> + 'a {
        move |path| {
            files
                .get(path.to_str().unwrap_or_default())
                .map(|v| v.to_string())
                .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))
        }
    }

    fn config_from(vars: &HashMap<&str, &str>, files: &HashMap<&str, &str>) -> Option<MqttConfig> {
        MqttConfig::from_lookup(lookup_from(vars), reader_from(files))
    }

    /// Resolves just the password, with MQTT_HOST supplied so a config exists.
    fn password_from(vars: &HashMap<&str, &str>, files: &HashMap<&str, &str>) -> Option<String> {
        let mut vars = vars.clone();
        vars.insert("MQTT_HOST", "broker.local");
        config_from(&vars, files).unwrap().password
    }

    #[test]
    fn given_no_mqtt_host_when_config_is_read_then_mqtt_is_disabled() {
        let vars = HashMap::new();
        assert_eq!(config_from(&vars, &HashMap::new()), None);
    }

    #[test]
    fn given_only_mqtt_host_when_config_is_read_then_defaults_are_applied() {
        let vars = HashMap::from([("MQTT_HOST", "broker.local")]);
        let config = config_from(&vars, &HashMap::new()).unwrap();
        assert_eq!(config.host, "broker.local");
        assert_eq!(config.port, 1883);
        assert_eq!(config.username, None);
        assert_eq!(config.client_id, "m3i-ha-bridge");
        assert_eq!(config.topic_prefix, "m3i");
        assert_eq!(config.discovery_prefix, "homeassistant");
    }

    #[test]
    fn given_full_configuration_when_config_is_read_then_all_values_are_used() {
        let vars = HashMap::from([
            ("MQTT_HOST", "192.168.1.10"),
            ("MQTT_PORT", "8883"),
            ("MQTT_USERNAME", "ha"),
            ("MQTT_PASSWORD", "secret"),
            ("MQTT_CLIENT_ID", "bike-bridge"),
            ("MQTT_TOPIC_PREFIX", "fitness/m3i"),
            ("MQTT_DISCOVERY_PREFIX", "ha-discovery"),
        ]);
        let config = config_from(&vars, &HashMap::new()).unwrap();
        assert_eq!(config.port, 8883);
        assert_eq!(config.username.as_deref(), Some("ha"));
        assert_eq!(config.password.as_deref(), Some("secret"));
        assert_eq!(config.client_id, "bike-bridge");
        assert_eq!(config.state_topic(42), "fitness/m3i/042/state");
        assert_eq!(
            config.bridge_availability_topic(),
            "fitness/m3i/availability"
        );
        assert_eq!(
            config.bike_availability_topic(42),
            "fitness/m3i/042/availability"
        );
    }

    #[test]
    fn given_empty_credentials_when_config_is_read_then_they_are_treated_as_unset() {
        let vars = HashMap::from([
            ("MQTT_HOST", "broker.local"),
            ("MQTT_USERNAME", ""),
            ("MQTT_PASSWORD", ""),
        ]);
        let config = config_from(&vars, &HashMap::new()).unwrap();
        assert_eq!(config.username, None);
        assert_eq!(config.password, None);
    }

    #[test]
    fn given_a_password_file_when_config_is_read_then_the_file_contents_are_used() {
        let vars = HashMap::from([("MQTT_PASSWORD_FILE", "/run/secrets/mqtt")]);
        let files = HashMap::from([("/run/secrets/mqtt", "from-file")]);
        assert_eq!(password_from(&vars, &files).as_deref(), Some("from-file"));
    }

    #[test]
    fn given_a_password_file_with_a_trailing_newline_when_read_then_it_is_stripped() {
        // systemd copies credential files through byte for byte, so an
        // operator's `echo pw > file` would otherwise put a newline in the
        // password and every connection would fail authentication.
        let vars = HashMap::from([("MQTT_PASSWORD_FILE", "/run/secrets/mqtt")]);
        let files = HashMap::from([("/run/secrets/mqtt", "from-file\r\n")]);
        assert_eq!(password_from(&vars, &files).as_deref(), Some("from-file"));
    }

    #[test]
    fn given_a_password_file_with_inner_spaces_when_read_then_they_are_preserved() {
        // Only trailing newlines are stripped: spaces may be part of the
        // password, so trimming them would silently corrupt it.
        let vars = HashMap::from([("MQTT_PASSWORD_FILE", "/run/secrets/mqtt")]);
        let files = HashMap::from([("/run/secrets/mqtt", " a b \n")]);
        assert_eq!(password_from(&vars, &files).as_deref(), Some(" a b "));
    }

    #[test]
    fn given_a_systemd_credential_when_read_then_it_is_used() {
        let vars = HashMap::from([("CREDENTIALS_DIRECTORY", "/run/credentials/m3i.service")]);
        let files = HashMap::from([(
            "/run/credentials/m3i.service/mqtt-password",
            "from-credential\n",
        )]);
        assert_eq!(
            password_from(&vars, &files).as_deref(),
            Some("from-credential")
        );
    }

    #[test]
    fn given_both_a_password_and_a_password_file_when_read_then_the_environment_wins() {
        let vars = HashMap::from([
            ("MQTT_PASSWORD", "from-env"),
            ("MQTT_PASSWORD_FILE", "/run/secrets/mqtt"),
        ]);
        let files = HashMap::from([("/run/secrets/mqtt", "from-file")]);
        assert_eq!(password_from(&vars, &files).as_deref(), Some("from-env"));
    }

    #[test]
    fn given_both_a_password_file_and_a_credential_when_read_then_the_password_file_wins() {
        let vars = HashMap::from([
            ("MQTT_PASSWORD_FILE", "/run/secrets/mqtt"),
            ("CREDENTIALS_DIRECTORY", "/run/credentials/m3i.service"),
        ]);
        let files = HashMap::from([
            ("/run/secrets/mqtt", "from-file"),
            (
                "/run/credentials/m3i.service/mqtt-password",
                "from-credential",
            ),
        ]);
        assert_eq!(password_from(&vars, &files).as_deref(), Some("from-file"));
    }

    #[test]
    fn given_an_empty_password_and_a_password_file_when_read_then_the_file_is_used() {
        // Blanking MQTT_PASSWORD must fall through rather than resolve to an
        // empty password, matching how every other credential treats "".
        let vars = HashMap::from([
            ("MQTT_PASSWORD", ""),
            ("MQTT_PASSWORD_FILE", "/run/secrets/mqtt"),
        ]);
        let files = HashMap::from([("/run/secrets/mqtt", "from-file")]);
        assert_eq!(password_from(&vars, &files).as_deref(), Some("from-file"));
    }

    #[test]
    fn given_a_credentials_directory_without_the_credential_when_read_then_no_password_is_used() {
        // The normal state on a box with no MQTT password configured: systemd
        // exports CREDENTIALS_DIRECTORY because the unit declares a credential,
        // and skips the missing file silently. The bridge must connect without
        // a password rather than fail.
        let vars = HashMap::from([("CREDENTIALS_DIRECTORY", "/run/credentials/m3i.service")]);
        assert_eq!(password_from(&vars, &HashMap::new()), None);
    }

    #[test]
    fn given_an_unreadable_password_file_when_read_then_no_password_is_used() {
        let vars = HashMap::from([("MQTT_PASSWORD_FILE", "/run/secrets/missing")]);
        assert_eq!(password_from(&vars, &HashMap::new()), None);
    }

    #[test]
    fn given_a_blank_password_file_when_read_then_no_password_is_used() {
        let vars = HashMap::from([("MQTT_PASSWORD_FILE", "/run/secrets/mqtt")]);
        let files = HashMap::from([("/run/secrets/mqtt", "\n")]);
        assert_eq!(password_from(&vars, &files), None);
    }

    #[test]
    fn given_no_password_settings_at_all_when_read_then_no_password_is_used() {
        assert_eq!(password_from(&HashMap::new(), &HashMap::new()), None);
    }

    #[test]
    fn given_empty_mqtt_host_when_config_is_read_then_mqtt_is_disabled() {
        let vars = HashMap::from([("MQTT_HOST", "")]);
        assert_eq!(config_from(&vars, &HashMap::new()), None);
    }

    #[test]
    fn given_stats_when_state_payload_is_built_then_all_fields_are_present() {
        let stats = KeiserStats {
            bike_id: 3,
            power: 150,
            cadence: 85.5,
            heart_rate: 120.0,
            gear: 12,
            distance: 4.2,
            energy: 55,
            minutes: 2,
            seconds: 5,
            is_paused: false,
            last_updated: Some(std::time::Instant::now()),
            ..Default::default()
        };
        let payload = state_payload(&stats);
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
        // End to end from the bytes doc/sample-data.md actually captured, since
        // the symptom in issue #2 is a string Home Assistant renders verbatim:
        // cadence 502 -> 50.2 rpm and distance 1 -> 0.1 km used to serialize as
        // 50.20000076293945 and 0.10000000149011612.
        let stats = crate::keiser::parse_keiser_data(&hex!("0624ff00f60100001b0002000033018008"))
            .expect("the captured packet should parse");
        let payload = state_payload(&stats).to_string();

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
        let stats = KeiserStats {
            heart_rate: 1205.0 / 10.0,
            last_updated: Some(std::time::Instant::now()),
            ..Default::default()
        };
        assert!(
            state_payload(&stats)
                .to_string()
                .contains("\"heart_rate\":120.5")
        );
    }

    #[test]
    fn given_values_at_the_bikes_resolution_when_rounded_then_they_are_preserved_exactly() {
        // The bike transmits `value * 10` in a u16, so every representable
        // reading is a multiple of 0.1. Rounding must not move any of them.
        for raw in [0u16, 1, 5, 502, 820, 1205, 65535] {
            let native = raw as f32 / 10.0;
            assert_eq!(
                round_to_native_resolution(native),
                raw as f64 / 10.0,
                "raw {raw} should round-trip through 0.1 resolution"
            );
        }
    }

    #[test]
    fn given_a_zeroed_reading_when_rounded_then_it_stays_zero() {
        // sanitized() zeroes the live metrics, and both -0.0 and 1e-9 would
        // print oddly in Home Assistant.
        assert_eq!(round_to_native_resolution(0.0), 0.0);
        assert_eq!(round_to_native_resolution(0.0).to_string(), "0");
    }

    /// The bike every discovery test announces.
    const BIKE: u8 = 42;

    #[test]
    fn given_config_when_the_discovery_message_is_built_then_it_is_one_device_topic_per_bike() {
        // Issue #5: device-based discovery, one retained topic per bike
        // carrying every entity. Issue #6: the node id, topics and unique ids
        // all carry the padded bike id.
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
        // root; repeating them per component would be redundant and, for
        // device/origin, is not permitted at all.
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
            last_updated: Some(std::time::Instant::now()),
            ..Default::default()
        };
        assert_eq!(
            state_payload(&stats)["bike_id"],
            json!(42),
            "the console shows 42, not \"042\""
        );
    }

    #[test]
    fn given_the_measurement_sensors_when_announced_then_none_is_diagnostic() {
        for spec in SENSORS.iter().filter(|spec| spec.object_id != "bike_id") {
            assert!(
                discovery_for(spec.object_id)
                    .get("entity_category")
                    .is_none(),
                "{} is a reading and belongs in the main sensor list",
                spec.object_id
            );
        }
    }

    fn device_discovery() -> (String, serde_json::Value) {
        let vars = HashMap::from([("MQTT_HOST", "broker.local")]);
        let config = config_from(&vars, &HashMap::new()).unwrap();
        discovery_message(&config, BIKE)
    }

    /// One entity's component of the device discovery payload.
    fn discovery_for(object_id: &str) -> serde_json::Value {
        let (_, payload) = device_discovery();
        payload["components"]
            .get(object_id)
            .cloned()
            .unwrap_or_else(|| panic!("no discovery component for {object_id}"))
    }

    fn reading_from(bike_id: u8, power: u16) -> KeiserStats {
        KeiserStats {
            bike_id,
            power,
            cadence: 80.0,
            last_updated: Some(std::time::Instant::now()),
            ..Default::default()
        }
    }

    fn publisher() -> BikePublisher {
        BikePublisher::new(test_config(), Arc::new(Mutex::new(BTreeSet::new())))
    }

    /// Runs one tick, accepting every message, and returns what was sent.
    fn tick_all(publisher: &mut BikePublisher) -> Vec<OutgoingMessage> {
        let mut sent = Vec::new();
        publisher.tick(Instant::now(), |message| {
            sent.push(message.clone());
            true
        });
        sent
    }

    #[test]
    fn given_the_initial_reading_when_observed_then_nothing_is_announced() {
        // The channel starts with a default reading whose bike_id is 0 — a
        // real id. Announcing it would create a phantom bike #000 device.
        let mut publisher = publisher();
        assert!(publisher.observe(KeiserStats::default()).is_empty());
        assert!(tick_all(&mut publisher).is_empty());
    }

    #[test]
    fn given_a_bike_heard_for_the_first_time_when_observed_then_its_discovery_is_sent() {
        let mut publisher = publisher();
        let messages = publisher.observe(reading_from(BIKE, 150));

        assert_eq!(messages.len(), 1, "one device-discovery message per bike");
        assert!(messages[0].retain);
        assert_eq!(
            messages[0].topic, "homeassistant/device/m3i-ha-bridge-042/config",
            "the config belongs to this bike's node"
        );
        assert_eq!(
            *publisher.known_bikes.lock().unwrap(),
            BTreeSet::from([BIKE]),
            "the driver re-announces from this set on reconnect"
        );

        assert!(
            publisher.observe(reading_from(BIKE, 160)).is_empty(),
            "discovery is sent once per bike, not per reading"
        );
    }

    #[test]
    fn given_a_live_bike_when_ticked_then_it_is_online_and_its_state_is_on_its_own_topic() {
        let mut publisher = publisher();
        publisher.observe(reading_from(BIKE, 150));

        let sent = tick_all(&mut publisher);

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
        publisher.observe(reading_from(1, 100));
        publisher.observe(reading_from(2, 200));
        publisher.observe(reading_from(1, 110));

        let sent = tick_all(&mut publisher);
        let state_of = |topic: &str| -> serde_json::Value {
            let m = sent.iter().find(|m| m.topic == topic).unwrap();
            serde_json::from_str(&m.payload).unwrap()
        };
        assert_eq!(state_of("m3i/001/state")["power"], 110);
        assert_eq!(state_of("m3i/002/state")["power"], 200);
        assert_eq!(
            *publisher.known_bikes.lock().unwrap(),
            BTreeSet::from([1, 2])
        );
    }

    #[test]
    fn given_an_unchanged_reading_when_ticked_again_then_nothing_is_resent() {
        let mut publisher = publisher();
        publisher.observe(reading_from(BIKE, 150));
        tick_all(&mut publisher);

        assert!(
            tick_all(&mut publisher).is_empty(),
            "availability and state are both unchanged"
        );
    }

    #[test]
    fn given_a_bike_that_went_stale_when_ticked_then_it_goes_offline_and_its_metrics_zero() {
        let mut publisher = publisher();
        publisher.observe(reading_from(BIKE, 150));
        tick_all(&mut publisher);

        publisher.bikes.get_mut(&BIKE).unwrap().stats.last_updated =
            Some(std::time::Instant::now() - crate::stats::STALE_AFTER * 2);
        let sent = tick_all(&mut publisher);

        assert_eq!(sent[0].topic, "m3i/042/availability");
        assert_eq!(sent[0].payload, "offline");
        let state: serde_json::Value = serde_json::from_str(&sent[1].payload).unwrap();
        assert_eq!(state["power"], 0);
        assert_eq!(state["bike_id"], 42, "identity survives staleness");
    }

    #[test]
    fn given_a_stale_bike_when_it_is_heard_again_then_it_comes_back_online() {
        let mut publisher = publisher();
        publisher.observe(reading_from(BIKE, 150));
        tick_all(&mut publisher);
        publisher.bikes.get_mut(&BIKE).unwrap().stats.last_updated =
            Some(std::time::Instant::now() - crate::stats::STALE_AFTER * 2);
        tick_all(&mut publisher);

        publisher.observe(reading_from(BIKE, 90));
        let sent = tick_all(&mut publisher);

        assert_eq!(sent[0].payload, "online");
        assert!(sent[0].retain);
    }

    #[test]
    fn given_a_rejected_publish_when_ticked_again_then_it_is_retried() {
        // try_publish can refuse when the request channel is full; the
        // rejection must not be recorded as sent, or the reading is lost until
        // the heartbeat.
        let mut publisher = publisher();
        publisher.observe(reading_from(BIKE, 150));
        publisher.tick(Instant::now(), |_| false);

        let sent = tick_all(&mut publisher);
        assert_eq!(sent.len(), 2, "availability and state both retried");
    }

    #[test]
    fn given_a_reconnect_when_ticked_then_every_bikes_availability_and_state_are_resent() {
        // The broker may have lost its retained messages while the bridge was
        // away; a bike's `online` is only ever sent on a transition, so a
        // reconnect has to forget what was published.
        let mut publisher = publisher();
        publisher.observe(reading_from(1, 100));
        publisher.observe(reading_from(2, 200));
        tick_all(&mut publisher);
        assert!(tick_all(&mut publisher).is_empty(), "steady state");

        publisher.reconnected();
        let sent = tick_all(&mut publisher);

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
        publisher.observe(reading_from(1, 100));
        publisher.observe(reading_from(2, 200));

        let offline = publisher.offline_messages();
        assert_eq!(
            offline,
            vec![
                OutgoingMessage::retained("m3i/001/availability".into(), "offline"),
                OutgoingMessage::retained("m3i/002/availability".into(), "offline"),
            ]
        );
    }

    /// Stands in for the event loop: `AsyncClient` is only a handle on
    /// rumqttc's request channel, so a plain `flume` receiver sees exactly what
    /// a publish enqueued, with no broker and no network.
    fn test_client(capacity: usize) -> (AsyncClient, flume::Receiver<rumqttc::Request>) {
        let (tx, rx) = flume::bounded(capacity);
        (AsyncClient::from_senders(tx), rx)
    }

    fn queued_publishes(
        rx: &flume::Receiver<rumqttc::Request>,
    ) -> Vec<rumqttc::mqttbytes::v4::Publish> {
        rx.drain()
            .filter_map(|request| match request {
                rumqttc::Request::Publish(publish) => Some(publish),
                _ => None,
            })
            .collect()
    }

    fn test_config() -> MqttConfig {
        let vars = HashMap::from([("MQTT_HOST", "broker.local")]);
        config_from(&vars, &HashMap::new()).unwrap()
    }

    #[test]
    fn given_a_reconnect_when_announcing_then_availability_and_every_discovery_config_are_queued() {
        let config = test_config();
        let (client, rx) = test_client(REQUEST_CHANNEL_CAPACITY);

        let queued = announce(
            &client,
            &Qos1Ledger::default(),
            &config,
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
        let config = test_config();
        let (client, rx) = test_client(REQUEST_CHANNEL_CAPACITY);

        // The worst case: every ordinal id 0–200 has been heard. With per-bike
        // devices (issue #6) the burst scales with the bikes in range, and the
        // old capacity of 64 overflowed at four bikes.
        let bikes: BTreeSet<u8> = (0..=200).collect();
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
        assert_eq!(announce(&client, &ledger, &config, &bikes), burst);
        assert_eq!(announce(&client, &ledger, &config, &bikes), burst);
        assert_eq!(queued_publishes(&rx).len(), burst * 2);
    }

    #[test]
    fn given_a_full_request_channel_when_announcing_then_it_reports_what_it_dropped() {
        // try_publish never blocks -- which is exactly why it is used inside
        // the poll task -- so an overflowing burst must be visible in the
        // return value rather than silently lost.
        let config = test_config();
        let (client, _rx) = test_client(3);

        assert_eq!(
            announce(
                &client,
                &Qos1Ledger::default(),
                &config,
                &BTreeSet::from([1, 2, 3, 4])
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
        let config = test_config();
        let (client, rx) = test_client(REQUEST_CHANNEL_CAPACITY);
        let driver = tokio::spawn(async {});

        shutdown(&client, &config, driver).await;

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
        let config = test_config();
        let (client, _rx) = test_client(REQUEST_CHANNEL_CAPACITY);
        let driver = tokio::spawn(std::future::pending::<()>());
        let handle = driver.abort_handle();

        let start = Instant::now();
        shutdown(&client, &config, driver).await;

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
        let config = test_config();
        // A rendezvous channel with no receiver waiting: try_publish fails.
        let (client, _rx) = test_client(0);
        let driver = tokio::spawn(std::future::pending::<()>());
        let handle = driver.abort_handle();

        let start = Instant::now();
        shutdown(&client, &config, driver).await;

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
        let config = test_config();

        announce(&client, &ledger, &config, &BTreeSet::from([1]));
        let mut publisher = BikePublisher::new(config, Arc::new(Mutex::new(BTreeSet::new())));
        for message in publisher.observe(reading_from(2, 100)) {
            try_send(&client, &ledger, &message);
        }
        publisher.tick(Instant::now(), |message| {
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

    #[test]
    fn given_the_messages_the_state_loop_sends_when_inspected_then_retained_ones_are_qos1() {
        // Retained discovery and availability must survive a loss on the wire;
        // state is superseded within seconds and stays QoS 0.
        let mut publisher = publisher();
        let discovery = publisher.observe(reading_from(BIKE, 150));
        assert_eq!(discovery[0].qos, QoS::AtLeastOnce);
        let sent = tick_all(&mut publisher);
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
    fn given_nothing_published_yet_when_the_gate_is_asked_then_it_publishes() {
        let gate = PublishGate::new(HEARTBEAT_INTERVAL);
        assert!(gate.should_publish("{}", Instant::now()));
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
        // The defect in issue #3. Once sanitized() has zeroed the live metrics
        // the payload stops changing, so dedup used to suppress every publish
        // and Home Assistant marked all seven sensors unavailable after
        // expire_after -- while the bridge was healthy and still reporting
        // itself online.
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
        // The gate only advances on a successful publish, so a payload rejected
        // by a full request channel is offered again on the next tick instead
        // of being deduplicated away and lost.
        let gate = PublishGate::new(HEARTBEAT_INTERVAL);
        let payload = "{\"power\":150}";
        assert!(gate.should_publish(payload, Instant::now()));
        // ... publish fails, so record_published is never called ...
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(gate.should_publish(payload, Instant::now()));
    }

    #[test]
    fn given_the_heartbeat_when_compared_to_expiry_then_a_dropped_publish_still_has_a_margin() {
        // The invariant that makes the fix work at all: at least two heartbeats
        // must fit inside expire_after, so losing one does not expire the
        // entities.
        let expire_after = Duration::from_secs(EXPIRE_AFTER_SECS as u64);
        assert!(
            HEARTBEAT_INTERVAL * 2 <= expire_after,
            "heartbeat {HEARTBEAT_INTERVAL:?} leaves no margin inside {expire_after:?}"
        );
    }

    #[test]
    fn given_the_paused_binary_sensor_when_announced_then_it_expires_with_the_others() {
        // It used to have no expire_after, so it stayed live while every sensor
        // went unavailable and the device contradicted itself.
        assert_eq!(discovery_for("paused")["expire_after"], EXPIRE_AFTER_SECS);
    }

    #[test]
    fn given_the_sensors_when_announced_then_every_one_declares_a_state_class() {
        // The point of issue #10: without state_class Home Assistant records
        // history but computes no long-term statistics, so none of these
        // entities had any long-term data.
        for spec in SENSORS.iter().filter(|spec| spec.entity_category.is_none()) {
            let payload = discovery_for(spec.object_id);
            assert!(
                payload["state_class"].is_string(),
                "{} has no state_class",
                spec.object_id
            );
        }
    }

    #[test]
    fn given_the_sensors_when_announced_then_their_units_and_classes_match_the_table() {
        let expected = [
            // object_id, unit, device_class, state_class
            ("power", Some("W"), Some("power"), "measurement"),
            ("cadence", Some("rpm"), None, "measurement"),
            ("heart_rate", Some("bpm"), None, "measurement"),
            ("gear", None, None, "measurement"),
            ("distance", Some("km"), Some("distance"), "total_increasing"),
            ("energy", Some("kcal"), Some("energy"), "total_increasing"),
            ("elapsed_time", Some("s"), Some("duration"), "measurement"),
            ("bike_id", None, None, ""),
        ];
        assert_eq!(
            expected.len(),
            SENSORS.len(),
            "a sensor is missing from this table"
        );

        for (object_id, unit, device_class, state_class) in expected {
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
                payload["state_class"].as_str().unwrap_or(""),
                state_class,
                "{object_id} state_class"
            );
        }
    }

    #[test]
    fn given_a_device_class_when_announced_then_its_unit_and_state_class_are_ones_ha_accepts() {
        // Home Assistant rejects an invalid device_class/unit pair at discovery
        // and never creates the entity — a silent loss with only a log line —
        // and warns about an impossible device_class/state_class pair. These
        // are its own tables, so a wrong combination fails here rather than on
        // the running system.
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
        // Issue #6 changed them all deliberately (one device per bike); this
        // pins the new form so it does not drift again by accident.
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
        // unknown keys silently (extra=vol.REMOVE_EXTRA). Publishing it would
        // read as configuration while doing nothing at all, so its absence is
        // deliberate and worth pinning.
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
        // The discovery configs and the state payload are edited in different
        // places, so this checks the JSON key each value_template reads is
        // actually published.
        let stats = KeiserStats {
            last_updated: Some(std::time::Instant::now()),
            ..Default::default()
        };
        let payload = state_payload(&stats);

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

    #[test]
    fn given_topic_prefix_with_slashes_when_discovery_messages_are_built_then_node_id_is_sanitized()
    {
        let vars = HashMap::from([
            ("MQTT_HOST", "broker.local"),
            ("MQTT_TOPIC_PREFIX", "fitness/m3i"),
        ]);
        let config = config_from(&vars, &HashMap::new()).unwrap();
        let (topic, _) = discovery_message(&config, BIKE);
        // A device discovery topic is exactly <prefix>/device/<node_id>/config;
        // the node id is fixed, so the topic prefix cannot leak a slash into it.
        assert_eq!(
            topic.split('/').count(),
            4,
            "unexpected topic depth: {topic}"
        );
    }
}
