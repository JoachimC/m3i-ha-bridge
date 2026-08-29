//! Turns the process's termination signals into one cancellation token.

use tokio_util::sync::CancellationToken;

/// A token that is cancelled when the process is asked to stop.
///
/// SIGTERM is what systemd sends; SIGINT and SIGQUIT cover a terminal.
pub fn on_signal() -> CancellationToken {
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

    cancel_token
}
