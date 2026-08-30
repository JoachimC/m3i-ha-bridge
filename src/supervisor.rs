//! Keeps the long-lived tasks running and maps how they ended to the
//! process exit status.
//!
//! # Failure policy
//!
//! Three long-lived tasks, three deliberately different policies:
//!
//! - **Bluetooth reader** — retry forever. An adapter that is missing, stuck,
//!   or in re-initialisation recovers on its own, and to read the bike is the
//!   main purpose of the process. [`bridge_loop`] restarts the reader until
//!   cancellation.
//! - **MQTT publisher** — retry forever, internally. rumqttc reconnects on
//!   its own, so a broker outage must not stop the BLE half of the bridge.
//!   Its task fails only after the reader stops permanently, so
//!   [`OnFailure::Report`] supervises it.
//! - **GATT server** — fail fast. Its failures are registration and
//!   permission problems, and in-process retries do not correct those.
//!   [`OnFailure::CancelEverything`] supervises it, and systemd's
//!   `Restart=always` starts a clean process.
//!
//! Whichever ends the process, the exit status reflects it: zero for a signal,
//! non-zero for a task failure.

use std::future::Future;
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::bridge::RunStatus;
use crate::retry::{RetryDelay, Wait};

/// Common boxed error type used across the crate's fallible async tasks.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Delay before the first retry after a reader failure.
pub const RETRY_DURATION: Duration = Duration::from_secs(5);
/// The maximum backoff delay. With it, a permanent fault costs one log line
/// each minute instead of twelve.
pub const MAX_RETRY_DURATION: Duration = Duration::from_secs(60);
/// Minimum duration of a reader attempt that counts as healthy. It is much
/// longer than the time to discover that no adapter exists, so a bridge
/// that works always qualifies.
pub const HEALTHY_RUN_DURATION: Duration = Duration::from_secs(120);

/// What a task's failure means for the rest of the process.
#[derive(Debug, Clone, Copy)]
pub enum OnFailure {
    /// Log it and cancel everything; systemd restarts the process.
    CancelEverything,
    /// Log it and let the others finish; the exit status still reports it.
    Report,
}

/// Spawns a task under a failure policy. The handle's result is what
/// [`join_task`] reports at exit.
pub fn spawn<F>(
    name: &'static str,
    on_failure: OnFailure,
    cancel_token: CancellationToken,
    task: F,
) -> JoinHandle<Result<(), BoxError>>
where
    F: Future<Output = Result<(), BoxError>> + Send + 'static,
{
    tokio::spawn(async move {
        let result = task.await;
        if let Err(e) = &result {
            tracing::error!("{name} error: {e}");
            if let OnFailure::CancelEverything = on_failure {
                cancel_token.cancel();
            }
        }
        result
    })
}

/// Awaits a supervised task, returning a description of how it failed.
///
/// It reports both failure shapes. An `Err` is the task's own reported
/// failure. A `JoinError` means the task panicked or an abort stopped it;
/// without this report that is invisible — the handle resolves and nothing
/// else notices.
///
/// Release builds set `panic = "abort"`, so a panic stops the process before
/// this function can observe it; in that case the restart itself is the
/// report. The `JoinError` arm still matters for aborted tasks, and in debug
/// and test builds, which unwind.
pub async fn join_task(name: &str, handle: JoinHandle<Result<(), BoxError>>) -> Option<String> {
    match handle.await {
        Ok(Ok(())) => None,
        Ok(Err(e)) => Some(format!("{name} failed: {e}")),
        Err(e) => Some(format!("{name} did not shut down cleanly: {e}")),
    }
}

/// Turns the supervised tasks' outcomes into the process exit status.
///
/// A zero exit after a task failure hides the failure and costs debugging
/// time: `Restart=always` masks it, `systemctl status` shows a clean exit,
/// and the journal is the only evidence of the fault.
pub fn exit_status(failures: Vec<String>) -> Result<(), BoxError> {
    if failures.is_empty() {
        return Ok(());
    }
    Err(failures.join("; ").into())
}

/// Runs the reader until cancellation. After every failure or stream end, it
/// restarts the reader with the delay that the strategy decides.
pub async fn bridge_loop<F, Fut, S>(
    mut run_bridge_fn: F,
    cancel_token: CancellationToken,
    mut retry: S,
) where
    F: FnMut(CancellationToken) -> Fut,
    Fut: Future<Output = Result<RunStatus, BoxError>>,
    S: RetryDelay,
{
    loop {
        let started = tokio::time::Instant::now();
        match run_bridge_fn(cancel_token.clone()).await {
            Ok(RunStatus::Cancelled) => break,
            Ok(RunStatus::StreamEnded) => tracing::info!("Bridge event stream ended."),
            Err(e) => tracing::error!("Bridge error: {}.", e),
        }

        match retry.wait(started.elapsed(), cancel_token.clone()).await {
            Wait::Cancelled => break,
            Wait::Finished => {}
        }

        tracing::info!("Restarting...");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stats;
    use std::sync::{Arc, Mutex};

    /// Waits for nothing, but records the reported duration of each attempt.
    #[derive(Default)]
    struct RecordingDelay {
        attempts: Arc<Mutex<Vec<Duration>>>,
    }

    impl RetryDelay for RecordingDelay {
        async fn wait(&mut self, last_attempt: Duration, cancel_token: CancellationToken) -> Wait {
            self.attempts.lock().unwrap().push(last_attempt);
            if cancel_token.is_cancelled() {
                Wait::Cancelled
            } else {
                Wait::Finished
            }
        }
    }

    fn no_delay() -> RecordingDelay {
        RecordingDelay::default()
    }

    /// Runs `bridge_loop` over attempts that each last `attempt_duration` and
    /// then fail. It stops after two attempts and reports the durations that
    /// the loop gave the strategy.
    async fn attempt_durations_reported_for(attempt_duration: Duration) -> Vec<Duration> {
        let delay = no_delay();
        let attempts = delay.attempts.clone();
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

        bridge_loop(mock_run_bridge, cancel_token, delay).await;
        let reported = attempts.lock().unwrap().clone();
        drop(attempts);
        reported
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_failed_attempt_when_the_loop_retries_then_the_strategy_is_told_how_long_it_ran()
     {
        // The strategy, not the loop, decides what "healthy" means; the loop's
        // job is to report the duration faithfully.
        assert_eq!(
            attempt_durations_reported_for(Duration::ZERO).await,
            [Duration::ZERO]
        );
        assert_eq!(
            attempt_durations_reported_for(HEALTHY_RUN_DURATION).await,
            [HEALTHY_RUN_DURATION]
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
        // Without this report the panic is invisible: the JoinHandle resolves
        // to a JoinError, the task is gone, and nothing else notices.
        let handle = tokio::spawn(async { panic!("notify loop died") });
        let failure = join_task("GATT server", handle).await.expect("a failure");
        assert!(failure.contains("GATT server"), "{failure}");
    }

    #[tokio::test]
    async fn given_a_task_that_fails_under_cancel_everything_when_it_ends_then_the_token_is_cancelled()
     {
        let cancel_token = CancellationToken::new();
        let handle = spawn(
            "GATT server",
            OnFailure::CancelEverything,
            cancel_token.clone(),
            async { Err("registration refused".into()) },
        );
        assert!(
            handle.await.unwrap().is_err(),
            "the failure is still reported"
        );
        assert!(cancel_token.is_cancelled());
    }

    #[tokio::test]
    async fn given_a_task_that_fails_under_report_when_it_ends_then_the_others_keep_running() {
        let cancel_token = CancellationToken::new();
        let handle = spawn(
            "MQTT publisher",
            OnFailure::Report,
            cancel_token.clone(),
            async { Err("reader stopped".into()) },
        );
        assert!(handle.await.unwrap().is_err());
        assert!(!cancel_token.is_cancelled());
    }

    #[tokio::test]
    async fn given_a_task_that_succeeds_when_it_ends_then_nothing_is_cancelled() {
        let cancel_token = CancellationToken::new();
        let handle = spawn(
            "GATT server",
            OnFailure::CancelEverything,
            cancel_token.clone(),
            async { Ok(()) },
        );
        assert!(handle.await.unwrap().is_ok());
        assert!(!cancel_token.is_cancelled());
    }

    #[test]
    fn given_no_failures_when_the_exit_status_is_computed_then_it_is_success() {
        // A signal-driven shutdown: every task ended cleanly, so systemd sees
        // a clean exit.
        assert!(exit_status(Vec::new()).is_ok());
    }

    #[test]
    fn given_a_failure_when_the_exit_status_is_computed_then_it_is_an_error() {
        // An Ok(()) exit after the GATT server failed and stopped the
        // process makes `systemctl status` report success after a crash.
        let status = exit_status(vec!["GATT server failed: no adapter".to_string()]);
        assert!(status.is_err());
        assert!(status.unwrap_err().to_string().contains("no adapter"));
    }

    #[test]
    fn given_several_failures_when_the_exit_status_is_computed_then_all_are_reported() {
        // Two tasks can fail from one cause: the loss of the Bluetooth
        // adapter stops the GATT server and the producer with it. The second
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
        // wrapper by value. A sender kept in main holds the channel open
        // forever and makes those branches dead code.
        let (stats_tx, mut stats_rx) = stats::fleet_channel();
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

        bridge_loop(wrapper, cancel_token, no_delay()).await;

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

        bridge_loop(mock_run_bridge, cancel_token, no_delay()).await;

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

        bridge_loop(mock_run_bridge, cancel_token, no_delay()).await;

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
            bridge_loop(mock_run_bridge, cancel_token, no_delay()),
        )
        .await
        .expect("test timed out: bridge_loop did not terminate on cancellation");
    }
}
