//! MQTT output: one Home Assistant device per bike, over one broker
//! connection.

mod config;
mod connection;
mod discovery;
mod publisher;
#[cfg(test)]
mod test_support;
mod topics;

use std::time::Duration;

use rumqttc::{AsyncClient, LastWill, MqttOptions, QoS};
use tokio::sync::watch;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

pub use config::MqttConfig;
use connection::{Qos1Ledger, REQUEST_CHANNEL_CAPACITY, drive_connection, shutdown, try_send};
use publisher::BikePublisher;

use crate::stats::{Fleet, next_snapshot};
use std::sync::Arc;

/// How often the state loop re-evaluates even when no new advertisement
/// arrived, so a reading going stale is published rather than waited on.
const STATE_POLL_INTERVAL: Duration = Duration::from_secs(1);

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
    mut stats_rx: watch::Receiver<Arc<Fleet>>,
    config: MqttConfig,
) -> Result<(), crate::BoxError> {
    tracing::info!(
        "Starting MQTT publisher for broker {}:{} (topic prefix '{}')",
        config.host,
        config.port,
        config.topics.prefix
    );

    let mut options = MqttOptions::new(&config.client_id, &config.host, config.port);
    options.set_keep_alive(Duration::from_secs(30));
    if let Some(username) = &config.username {
        options.set_credentials(username, config.password.as_deref().unwrap_or(""));
    }
    options.set_last_will(LastWill::new(
        config.topics.bridge_availability(),
        "offline",
        QoS::AtLeastOnce,
        true,
    ));

    let (client, eventloop) = AsyncClient::new(options, REQUEST_CHANNEL_CAPACITY);

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
        config.topics.clone(),
        stats_rx.clone(),
        ledger.clone(),
        connected_tx,
        cancel_token.clone(),
    ));

    let mut publisher = BikePublisher::new(config.topics.clone());

    let mut lost_producer = false;
    loop {
        let snapshot = tokio::select! {
            _ = cancel_token.cancelled() => break,
            snapshot = next_snapshot(&mut stats_rx, STATE_POLL_INTERVAL) => snapshot,
        };
        let Some(fleet) = snapshot else {
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
        for message in publisher.observe(&fleet) {
            try_send(&client, &ledger, &message);
        }
        publisher.tick(&fleet, Instant::now(), |message| {
            try_send(&client, &ledger, message)
        });
    }

    tracing::info!("Shutting down MQTT publisher...");
    for message in publisher.offline_messages() {
        try_send(&client, &ledger, &message);
    }
    shutdown(&client, &config.topics, driver).await;

    if lost_producer {
        return Err("the Bluetooth reader stopped producing stats".into());
    }
    Ok(())
}
