//! # Failure policy
//!
//! Three long-lived tasks, three deliberately different policies:
//!
//! - **Bluetooth reader** — retry forever. An adapter that is missing, wedged
//!   or being re-initialised comes back on its own, and reading the bike is the
//!   one thing the process exists to do. [`bridge_loop`] restarts it until
//!   cancelled.
//! - **MQTT publisher** — retry forever, internally. rumqttc reconnects on its
//!   own, so a broker being down must not stop the BLE half of the bridge.
//! - **GATT server** — fail fast. Its failures are registration and permission
//!   problems that do not heal by retrying in-process, so it cancels everything
//!   and lets systemd's `Restart=always` start a clean one.
//!
//! Whichever ends the process, the exit status reflects it: zero for a signal,
//! non-zero for a task failure.

mod between_retries_strategy;
mod ble_platform;
mod bluetooth_hal;
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
mod gatt_codec;
#[cfg(target_os = "linux")]
mod gatt_server;
mod keiser;
mod mqtt_publisher;
mod run_status;
#[cfg(target_os = "linux")]
mod scan_bluer;
#[cfg(not(target_os = "linux"))]
mod scan_btleplug;
mod stats;

use between_retries_strategy::{BetweenRetriesResult, BetweenRetriesStrategy, ExponentialBackoff};
use ble_platform::BlePlatform;
use bluetooth_hal::run_bridge;
use run_status::RunStatus;
use std::future::Future;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Common boxed error type used across the crate's fallible async tasks.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Delay before the first retry after a failure.
const RETRY_DURATION: Duration = Duration::from_secs(5);
/// Ceiling the retry delay backs off to. A permanent fault costs one log line
/// a minute instead of twelve.
const MAX_RETRY_DURATION: Duration = Duration::from_secs(60);
/// How long an attempt has to last before it counts as healthy, clearing any
/// backoff accumulated by earlier failures. Comfortably longer than the time it
/// takes to discover there is no adapter, and longer than the scan restart
/// interval, so a genuinely working bridge always qualifies.
const HEALTHY_RUN_DURATION: Duration = Duration::from_secs(120);

/// Awaits a supervised task, returning a description of how it failed.
///
/// Both failure shapes are reported. An `Err` is the task's own reported
/// failure; a `JoinError` means it panicked or was aborted, which is otherwise
/// completely invisible — the handle resolves and nothing else notices.
///
/// Release builds set `panic = "abort"`, so a panicking task takes the process
/// down before this can observe it; there the restart *is* the report. The
/// `JoinError` arm still matters for aborted tasks, and in debug and test
/// builds, which unwind.
async fn join_task(
    name: &str,
    handle: tokio::task::JoinHandle<Result<(), BoxError>>,
) -> Option<String> {
    match handle.await {
        Ok(Ok(())) => None,
        Ok(Err(e)) => Some(format!("{name} failed: {e}")),
        Err(e) => Some(format!("{name} did not shut down cleanly: {e}")),
    }
}

/// Turns the supervised tasks' outcomes into the process exit status.
///
/// Exiting zero after a task failure is a lie that costs real debugging time:
/// `Restart=always` masks it, so `systemctl status` shows a clean exit and the
/// journal is the only evidence anything went wrong.
fn exit_status(failures: Vec<String>) -> Result<(), BoxError> {
    if failures.is_empty() {
        return Ok(());
    }
    Err(failures.join("; ").into())
}

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

    let cancel_token = CancellationToken::new();
    let token_clone = cancel_token.clone();

    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            let mut sigterm =
                signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
            let mut sigint =
                signal(SignalKind::interrupt()).expect("failed to install SIGINT handler");
            let mut sigquit =
                signal(SignalKind::quit()).expect("failed to install SIGQUIT handler");

            tokio::select! {
                _ = sigterm.recv() => tracing::info!("SIGTERM received"),
                _ = sigint.recv() => tracing::info!("SIGINT received"),
                _ = sigquit.recv() => tracing::info!("SIGQUIT received"),
            }
        }

        #[cfg(not(unix))]
        {
            if tokio::signal::ctrl_c().await.is_ok() {
                tracing::info!("Ctrl+C received");
            }
        }

        tracing::info!("Shutdown signal received...");
        token_clone.cancel();
    });

    // Before anything is spawned: if there is no usable Bluetooth stack the
    // process exits here, and a publisher spawned earlier would never run its
    // shutdown handshake. One platform handle for the whole process: on Linux
    // the scanner and the GATT server then share a single bluer session
    // rather than opening two.
    let platform = std::sync::Arc::new(BlePlatform::new().await?);

    // The bluetooth reader is the single producer on this watch channel; each
    // publisher (BLE GATT, MQTT) consumes its own receiver independently.
    let (stats_tx, stats_rx) = tokio::sync::watch::channel(stats::KeiserStats::default());

    let mqtt_handle = match mqtt_publisher::MqttConfig::from_env() {
        Some(config) => {
            let mqtt_cancel_token = cancel_token.clone();
            let mqtt_stats_rx = stats_tx.subscribe();
            Some(tokio::spawn(async move {
                mqtt_publisher::run(mqtt_cancel_token, mqtt_stats_rx, config).await
            }))
        }
        None => {
            tracing::info!("MQTT publishing disabled (MQTT_HOST not set)");
            None
        }
    };

    let gatt_cancel_token = cancel_token.clone();
    let main_cancel_token = cancel_token.clone();
    let gatt_platform = platform.clone();
    let gatt_handle = tokio::spawn(async move {
        let res = gatt_platform.serve_gatt(gatt_cancel_token, stats_rx).await;
        if let Err(ref e) = res {
            tracing::error!("GATT server error: {}", e);
            main_cancel_token.cancel();
        }
        res
    });

    // Read once at startup rather than per advertisement: this is on the hot
    // path, at roughly 2 Hz per bike in range.
    let bike_id_filter = keiser::bike_id_filter(|key| std::env::var(key).ok());
    match bike_id_filter {
        Some(bike_id) => tracing::info!("Only accepting advertisements from bike {}", bike_id),
        None => tracing::info!("Accepting advertisements from any Keiser M3i in range"),
    }

    let retry_strategy = ExponentialBackoff::new(RETRY_DURATION, MAX_RETRY_DURATION);
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
    bridge_loop(run_bridge_wrapper, cancel_token, retry_strategy).await;

    let mut failures = Vec::new();
    failures.extend(join_task("GATT server", gatt_handle).await);
    if let Some(handle) = mqtt_handle {
        failures.extend(join_task("MQTT publisher", handle).await);
    }
    exit_status(failures)
}

async fn bridge_loop<F, Fut, S>(
    mut run_bridge_fn: F,
    cancel_token: CancellationToken,
    retry_strategy: S,
) where
    F: FnMut(CancellationToken) -> Fut,
    Fut: Future<Output = Result<RunStatus, BoxError>>,
    S: BetweenRetriesStrategy,
{
    loop {
        let started = tokio::time::Instant::now();
        match run_bridge_fn(cancel_token.clone()).await {
            Ok(RunStatus::Cancelled) => {
                break;
            }
            Ok(RunStatus::StreamEnded) => {
                tracing::info!("Bridge event stream ended.");
            }
            Err(e) => {
                tracing::error!("Bridge error: {}.", e);
            }
        }

        // An attempt that ran this long was talking to a working adapter, so
        // whatever just went wrong is a fresh problem rather than a
        // continuation of an earlier one — do not make it inherit a backoff
        // built up hours ago. The adapter-missing case fails in milliseconds
        // and so never resets, which is the whole point.
        if started.elapsed() >= HEALTHY_RUN_DURATION {
            retry_strategy.reset();
        }

        match retry_strategy.wait(cancel_token.clone()).await {
            BetweenRetriesResult::Cancelled => break,
            BetweenRetriesResult::Finished => {}
        }

        tracing::info!("Restarting...");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    struct NoopStrategy;
    impl BetweenRetriesStrategy for NoopStrategy {
        async fn wait(&self, cancel_token: CancellationToken) -> BetweenRetriesResult {
            if cancel_token.is_cancelled() {
                BetweenRetriesResult::Cancelled
            } else {
                BetweenRetriesResult::Finished
            }
        }
    }

    /// Records whether `bridge_loop` decided the last attempt was healthy.
    #[derive(Default)]
    struct RecordingStrategy {
        resets: Arc<Mutex<usize>>,
    }

    impl BetweenRetriesStrategy for RecordingStrategy {
        async fn wait(&self, cancel_token: CancellationToken) -> BetweenRetriesResult {
            if cancel_token.is_cancelled() {
                BetweenRetriesResult::Cancelled
            } else {
                BetweenRetriesResult::Finished
            }
        }

        fn reset(&self) {
            *self.resets.lock().unwrap() += 1;
        }
    }

    /// Runs `bridge_loop` over attempts that each last `attempt_duration` and
    /// then fail, stopping after two, and reports how often the retry strategy
    /// was reset.
    async fn resets_after_attempts_lasting(attempt_duration: Duration) -> usize {
        let resets = Arc::new(Mutex::new(0));
        let strategy = RecordingStrategy {
            resets: resets.clone(),
        };
        let cancel_token = CancellationToken::new();
        let token_clone = cancel_token.clone();
        let call_count = Arc::new(Mutex::new(0));

        let mock_run_bridge = move |_token: CancellationToken| {
            let count = call_count.clone();
            let token = token_clone.clone();
            async move {
                tokio::time::sleep(attempt_duration).await;
                let mut c = count.lock().unwrap();
                *c += 1;
                if *c >= 2 {
                    token.cancel();
                    Ok(RunStatus::Cancelled)
                } else {
                    Err("adapter went away".into())
                }
            }
        };

        bridge_loop(mock_run_bridge, cancel_token, strategy).await;
        *resets.lock().unwrap()
    }

    #[tokio::test(start_paused = true)]
    async fn given_an_attempt_that_failed_immediately_when_it_retries_then_the_backoff_is_kept() {
        // The adapter-missing case: run_bridge fails in milliseconds, over and
        // over. Resetting here would defeat the backoff entirely and leave the
        // bridge logging every five seconds forever.
        assert_eq!(resets_after_attempts_lasting(Duration::ZERO).await, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn given_an_attempt_that_ran_healthily_when_it_fails_then_the_backoff_is_reset() {
        // A bridge that ran for hours before failing should retry promptly
        // rather than inherit a delay earned during an unrelated bad spell.
        assert_eq!(resets_after_attempts_lasting(HEALTHY_RUN_DURATION).await, 1);
    }

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
        // `#[tokio::test]` defaults to a current-thread runtime, which is what
        // `main` now uses too. That is what makes the rest of this suite real
        // coverage for the switch: every async test here already exercises the
        // single-threaded scheduler. Asserting it means a future change to the
        // test harness cannot quietly take that coverage away.
        assert_eq!(
            tokio::runtime::Handle::current().runtime_flavor(),
            tokio::runtime::RuntimeFlavor::CurrentThread
        );
    }

    #[tokio::test]
    async fn given_a_task_that_finished_cleanly_when_joined_then_no_failure_is_reported() {
        let handle = tokio::spawn(async { Ok(()) });
        assert_eq!(join_task("GATT server", handle).await, None);
    }

    #[tokio::test]
    async fn given_a_task_that_returned_an_error_when_joined_then_the_failure_is_reported() {
        let handle = tokio::spawn(async { Err("adapter vanished".into()) });
        let failure = join_task("GATT server", handle).await.expect("a failure");
        assert!(failure.contains("GATT server"), "names the task: {failure}");
        assert!(
            failure.contains("adapter vanished"),
            "keeps the cause: {failure}"
        );
    }

    #[tokio::test]
    async fn given_a_task_that_panicked_when_joined_then_the_failure_is_reported() {
        // Otherwise completely invisible: the JoinHandle resolves to a
        // JoinError, the task is gone, and nothing else notices.
        let handle = tokio::spawn(async { panic!("notify loop died") });
        let failure = join_task("GATT server", handle).await.expect("a failure");
        assert!(failure.contains("GATT server"), "{failure}");
    }

    #[test]
    fn given_no_failures_when_the_exit_status_is_computed_then_it_is_success() {
        // A signal-driven shutdown: every task ended cleanly, so systemd should
        // see a clean exit.
        assert!(exit_status(Vec::new()).is_ok());
    }

    #[test]
    fn given_a_failure_when_the_exit_status_is_computed_then_it_is_an_error() {
        // The defect in issue #6: main returned Ok(()) even after the GATT
        // server had failed and torn the process down, so systemctl status
        // reported success after a crash.
        let status = exit_status(vec!["GATT server failed: no adapter".to_string()]);
        assert!(status.is_err());
        assert!(status.unwrap_err().to_string().contains("no adapter"));
    }

    #[test]
    fn given_several_failures_when_the_exit_status_is_computed_then_all_are_reported() {
        // Two tasks can fail from one cause -- losing the Bluetooth adapter
        // takes the GATT server down and the producer with it -- and the second
        // failure is often the more informative one.
        let status = exit_status(vec![
            "GATT server failed: no adapter".to_string(),
            "MQTT publisher failed: the Bluetooth reader stopped producing stats".to_string(),
        ]);
        let message = status.unwrap_err().to_string();
        assert!(message.contains("GATT server"), "{message}");
        assert!(message.contains("MQTT publisher"), "{message}");
    }

    #[tokio::test]
    async fn given_the_bridge_loop_owns_the_last_sender_when_it_returns_then_the_channel_closes() {
        // What makes the publishers' "producer gone" branches reachable: main
        // moves its only sender into the wrapper, and bridge_loop takes the
        // wrapper by value. Holding a sender in main would keep the channel
        // open forever and turn those branches into dead code.
        let (stats_tx, mut stats_rx) = tokio::sync::watch::channel(stats::KeiserStats::default());
        let cancel_token = CancellationToken::new();
        let token_clone = cancel_token.clone();
        let wrapper = move |_token: CancellationToken| {
            let _tx = stats_tx.clone();
            let token = token_clone.clone();
            async move {
                token.cancel();
                Ok(RunStatus::Cancelled)
            }
        };

        bridge_loop(wrapper, cancel_token, NoopStrategy).await;

        assert!(
            stats_rx.changed().await.is_err(),
            "the channel must be closed once the loop has returned"
        );
    }

    #[tokio::test]
    async fn given_run_bridge_returns_stream_ended_when_bridge_loop_runs_then_it_should_restart() {
        let call_count = Arc::new(Mutex::new(0));
        let call_count_clone = call_count.clone();
        let cancel_token = CancellationToken::new();
        let token_clone = cancel_token.clone();

        let mock_run_bridge = move |_token: CancellationToken| {
            let count = call_count_clone.clone();
            let token = token_clone.clone();
            async move {
                let mut c = count.lock().unwrap();
                *c += 1;
                if *c >= 2 {
                    token.cancel();
                    Ok(RunStatus::Cancelled)
                } else {
                    Ok(RunStatus::StreamEnded)
                }
            }
        };

        bridge_loop(mock_run_bridge, cancel_token, NoopStrategy).await;

        assert_eq!(*call_count.lock().unwrap(), 2);
    }

    #[tokio::test]
    async fn given_run_bridge_returns_error_when_bridge_loop_runs_then_it_should_restart() {
        let call_count = Arc::new(Mutex::new(0));
        let call_count_clone = call_count.clone();
        let cancel_token = CancellationToken::new();
        let token_clone = cancel_token.clone();

        let mock_run_bridge = move |_token: CancellationToken| {
            let count = call_count_clone.clone();
            let token = token_clone.clone();
            async move {
                let mut c = count.lock().unwrap();
                *c += 1;
                if *c >= 2 {
                    token.cancel();
                    Ok(RunStatus::Cancelled)
                } else {
                    Err("test error".into())
                }
            }
        };

        bridge_loop(mock_run_bridge, cancel_token, NoopStrategy).await;

        assert_eq!(*call_count.lock().unwrap(), 2);
    }

    #[tokio::test]
    async fn given_cancellation_token_is_cancelled_externally_when_bridge_loop_runs_then_it_should_terminate()
     {
        let cancel_token = CancellationToken::new();
        let token_clone = cancel_token.clone();
        let bridge_started = Arc::new(tokio::sync::Notify::new());
        let bridge_started_clone = bridge_started.clone();
        let cancel_done = Arc::new(tokio::sync::Notify::new());
        let cancel_done_clone = cancel_done.clone();

        let mock_run_bridge = move |_token: CancellationToken| {
            let started = bridge_started_clone.clone();
            let done = cancel_done_clone.clone();
            async move {
                started.notify_one();
                done.notified().await;
                Ok(RunStatus::StreamEnded)
            }
        };

        tokio::spawn(async move {
            bridge_started.notified().await;
            token_clone.cancel();
            cancel_done.notify_one();
        });

        tokio::time::timeout(
            Duration::from_secs(1),
            bridge_loop(mock_run_bridge, cancel_token, NoopStrategy),
        )
        .await
        .expect("test timed out: bridge_loop did not terminate on cancellation");
    }
}
