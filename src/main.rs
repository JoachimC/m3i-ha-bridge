//! Wires the reader, the two publishers and their supervision together. The
//! failure policy is described in [`supervisor`].

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
mod advertising;
mod ble_platform;
mod ble_scanner;
mod bridge;
mod config;
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
mod gatt_codec;
mod gatt_server;
mod keiser;
mod mqtt;
mod retry;
#[cfg(target_os = "linux")]
mod scan_bluer;
#[cfg(not(target_os = "linux"))]
mod scan_btleplug;
mod shutdown;
mod stats;
mod supervisor;

use ble_platform::BlePlatform;
use bridge::run_bridge;
use retry::Backoff;
use supervisor::{
    HEALTHY_RUN_DURATION, MAX_RETRY_DURATION, OnFailure, RETRY_DURATION, bridge_loop, exit_status,
    join_task,
};
use tokio_util::sync::CancellationToken;

pub use supervisor::BoxError;

// The Pi Zero is a single-core armv6, and this process is a handful of
// long-lived I/O-bound tasks with no CPU-bound work anywhere. A multi-threaded
// runtime would spawn worker threads and a blocking pool, costing megabytes of
// thread stacks on a 512 MB box, and buy nothing: with one core, work stealing
// has nowhere to steal to.
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), BoxError> {
    tracing_subscriber::fmt::init();
    // BUILD_VERSION is injected by CI and reaches the cross container via
    // Cross.toml's build.env.passthrough — cross forwards no host environment
    // by default, so without that file this would silently read "dev" forever.
    // A plain local `cargo build` leaves it unset, which is what "dev" means.
    tracing::info!(
        "Keiser M3i HA Bridge {} starting...",
        option_env!("BUILD_VERSION").unwrap_or("dev")
    );

    let cancel_token = shutdown::on_signal();

    // Before anything is spawned: if there is no usable Bluetooth stack the
    // process exits here, and a publisher spawned earlier would never run its
    // shutdown handshake. One platform handle for the whole process: on Linux
    // the scanner and the GATT server then share a single bluer session
    // rather than opening two.
    let platform = std::sync::Arc::new(BlePlatform::new().await?);

    // The bluetooth reader is the single producer on this watch channel; each
    // publisher (BLE GATT, MQTT) consumes its own receiver independently.
    let (stats_tx, stats_rx) = stats::fleet_channel();

    let mqtt_handle = match mqtt::MqttConfig::from_env() {
        Some(config) => {
            let mqtt_stats_rx = stats_tx.subscribe();
            let mqtt_cancel_token = cancel_token.clone();
            Some(supervisor::spawn(
                "MQTT publisher",
                OnFailure::Report,
                cancel_token.clone(),
                async move { mqtt::run(mqtt_cancel_token, mqtt_stats_rx, config).await },
            ))
        }
        None => {
            tracing::info!("MQTT publishing disabled (MQTT_HOST not set)");
            None
        }
    };

    let gatt_platform = platform.clone();
    let gatt_cancel_token = cancel_token.clone();
    let gatt_handle = supervisor::spawn(
        "GATT server",
        OnFailure::CancelEverything,
        cancel_token.clone(),
        async move { gatt_platform.serve_gatt(gatt_cancel_token, stats_rx).await },
    );

    // Read once at startup rather than per advertisement: this is on the hot
    // path, at roughly 2 Hz per bike in range.
    let bike_id_filter = config::bike_id_filter(|key| std::env::var(key).ok());
    match bike_id_filter {
        Some(bike_id) => tracing::info!("Only accepting advertisements from bike {}", bike_id),
        None => tracing::info!("Accepting advertisements from any Keiser M3i in range"),
    }

    let retry = Backoff::new(RETRY_DURATION, MAX_RETRY_DURATION, HEALTHY_RUN_DURATION);
    let run_bridge_wrapper = move |token: CancellationToken| {
        let tx = stats_tx.clone();
        // Cheap per attempt: on Linux this clones the shared session rather
        // than opening a new D-Bus connection every five seconds.
        let scanner = platform.scanner();
        async move { run_bridge(&scanner, token, tx, bike_id_filter).await }
    };

    // `bridge_loop` takes the wrapper — and with it the last sender — by
    // value, so when it returns the channel closes. That is what lets the
    // publishers tell "the reader has stopped for good" from "no packet yet".
    bridge_loop(run_bridge_wrapper, cancel_token, retry).await;

    let mut failures = Vec::new();
    failures.extend(join_task("GATT server", gatt_handle).await);
    if let Some(handle) = mqtt_handle {
        failures.extend(join_task("MQTT publisher", handle).await);
    }
    exit_status(failures)
}

#[cfg(test)]
mod tests {
    #[test]
    fn given_a_per_target_rust_log_directive_when_parsed_then_it_still_filters_by_target() {
        // Dropping tracing-subscriber's `env-filter` feature is only safe
        // because `fmt::init()` keeps honouring RUST_LOG without it, through a
        // `Targets` filter. Nothing about that is visible at compile time, and
        // the deployment sets RUST_LOG=info, so this pins the directive forms
        // that have to keep working. What `Targets` does not support is
        // EnvFilter's span and field directives, which nothing here uses.
        use tracing::Level;
        use tracing_subscriber::filter::Targets;

        let targets: Targets = "info,bike_stats=trace"
            .parse()
            .expect("RUST_LOG directives must still parse");

        assert!(
            targets.would_enable("bike_stats", &Level::TRACE),
            "per-target override"
        );
        assert!(
            targets.would_enable("m3i_ha_bridge", &Level::INFO),
            "the default level"
        );
        assert!(
            !targets.would_enable("m3i_ha_bridge", &Level::TRACE),
            "the default level must still cap other targets"
        );

        let plain: Targets = "info".parse().expect("the deployed RUST_LOG=info");
        assert!(plain.would_enable("m3i_ha_bridge", &Level::INFO));
        assert!(!plain.would_enable("m3i_ha_bridge", &Level::DEBUG));
    }

    #[tokio::test]
    async fn given_the_test_runtime_when_inspected_then_it_is_the_flavor_main_runs_on() {
        // `#[tokio::test]` defaults to a current-thread runtime, the same
        // flavour `main` uses, so every async test here exercises the
        // scheduler the binary actually runs on. Asserting it keeps a future
        // harness change from quietly taking that coverage away.
        assert_eq!(
            tokio::runtime::Handle::current().runtime_flavor(),
            tokio::runtime::RuntimeFlavor::CurrentThread
        );
    }
}
