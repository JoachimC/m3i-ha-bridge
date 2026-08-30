//! Wires the reader, the two publishers and their supervision together.
//! [`supervisor`] describes the failure policy.

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

// The Pi Zero is a single-core armv6, and this process is a small set of
// long-lived I/O-bound tasks with no CPU-bound work anywhere. A multi-threaded
// runtime spawns worker threads and a blocking pool, costs megabytes of
// thread stacks on a 512 MB device, and gains nothing: with one core, work
// stealing has no target.
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), BoxError> {
    tracing_subscriber::fmt::init();
    // CI injects BUILD_VERSION, and it reaches the cross container via
    // Cross.toml's build.env.passthrough. cross forwards no host environment
    // by default, so without that file this silently reads "dev" forever.
    // A plain local `cargo build` leaves it unset; "dev" means that case.
    tracing::info!(
        "Keiser M3i HA Bridge {} starting...",
        option_env!("BUILD_VERSION").unwrap_or("dev")
    );

    let cancel_token = shutdown::on_signal();

    // This runs before any task spawns: if no usable Bluetooth stack exists,
    // the process exits here. A publisher spawned before this check never
    // runs its shutdown handshake. One platform handle serves the whole
    // process: on Linux the scanner and the GATT server share a single bluer
    // session and do not open two.
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

    // main reads this once at startup, not once per advertisement: the
    // filter is on the hot path, at roughly 2 Hz per bike in range.
    let bike_id_filter = config::bike_id_filter(|key| std::env::var(key).ok());
    match bike_id_filter {
        Some(bike_id) => tracing::info!(
            "Locked to bike {bike_id}: only its readings are published, and the bridge advertises as it from the start"
        ),
        None => tracing::info!(
            "Accepting any Keiser M3i in range: one Home Assistant device per bike heard, and the advertisement follows the bike being ridden"
        ),
    }

    let gatt_platform = platform.clone();
    let gatt_cancel_token = cancel_token.clone();
    let gatt_handle = supervisor::spawn(
        "GATT server",
        OnFailure::CancelEverything,
        cancel_token.clone(),
        async move {
            gatt_platform
                .serve_gatt(gatt_cancel_token, stats_rx, bike_id_filter)
                .await
        },
    );

    let retry = Backoff::new(RETRY_DURATION, MAX_RETRY_DURATION, HEALTHY_RUN_DURATION);
    let run_bridge_wrapper = move |token: CancellationToken| {
        let tx = stats_tx.clone();
        // Cheap per attempt: on Linux this clones the shared session and does
        // not open a new D-Bus connection every five seconds.
        let scanner = platform.scanner();
        async move { run_bridge(&scanner, token, tx, bike_id_filter).await }
    };

    // `bridge_loop` takes the wrapper — and with it the last sender — by
    // value, so the channel closes when the loop returns. That lets the
    // publishers separate "the reader stopped permanently" from "no packet
    // yet".
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
        // The crate omits tracing-subscriber's `env-filter` feature. That is
        // safe only because `fmt::init()` still honours RUST_LOG without it,
        // through a `Targets` filter. Nothing about that is visible at
        // compile time, and the deployment sets RUST_LOG=info, so this test
        // pins the directive forms that must keep working. `Targets` does not
        // support EnvFilter's span and field directives; nothing here uses
        // those.
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
        // scheduler that the binary runs on. This assertion prevents a future
        // harness change that silently removes that coverage.
        assert_eq!(
            tokio::runtime::Handle::current().runtime_flavor(),
            tokio::runtime::RuntimeFlavor::CurrentThread
        );
    }
}
