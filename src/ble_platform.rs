//! The one module that selects the Bluetooth stack for this build.
//!
//! All other modules — `main`, `bridge`, the public entry point of
//! `gatt_server` — use this abstraction, so the `cfg` split stays here.

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::BoxError;
use crate::stats::{BikeId, Fleet};
use std::sync::Arc;

#[cfg(target_os = "linux")]
pub type PlatformScanner = crate::scan_bluer::BluerScanner;
#[cfg(not(target_os = "linux"))]
pub type PlatformScanner = crate::scan_btleplug::BtleplugScanner;

/// Owns the per-process Bluetooth state that the platform needs.
///
/// On Linux, that is one `bluer::Session`; the scanner and the GATT server
/// share it. Both use the same controller, and the shared session gives the
/// process one D-Bus connection, one IO-resource task, and one set of match
/// rules. Two separate BlueZ client stacks would duplicate all of that.
pub struct BlePlatform {
    #[cfg(target_os = "linux")]
    session: bluer::Session,
}

impl BlePlatform {
    #[cfg(target_os = "linux")]
    pub async fn new() -> Result<Self, BoxError> {
        Ok(Self {
            session: bluer::Session::new().await?,
        })
    }

    #[cfg(not(target_os = "linux"))]
    pub async fn new() -> Result<Self, BoxError> {
        Ok(Self {})
    }

    /// Makes a scanner for one bridge attempt. On Linux, it clones the shared
    /// session and opens no new D-Bus connection, so the retry loop does not
    /// reconnect on each attempt.
    #[cfg(target_os = "linux")]
    pub fn scanner(&self) -> PlatformScanner {
        PlatformScanner::with_session(self.session.clone())
    }

    #[cfg(not(target_os = "linux"))]
    pub fn scanner(&self) -> PlatformScanner {
        PlatformScanner::default()
    }

    /// Serves the BLE GATT application until the caller cancels the token.
    #[cfg(target_os = "linux")]
    pub async fn serve_gatt(
        &self,
        cancel_token: CancellationToken,
        stats_rx: watch::Receiver<Arc<Fleet>>,
        locked_to: Option<BikeId>,
    ) -> Result<(), BoxError> {
        crate::gatt_server::run(self.session.clone(), cancel_token, stats_rx, locked_to).await
    }

    #[cfg(not(target_os = "linux"))]
    pub async fn serve_gatt(
        &self,
        cancel_token: CancellationToken,
        stats_rx: watch::Receiver<Arc<Fleet>>,
        locked_to: Option<BikeId>,
    ) -> Result<(), BoxError> {
        crate::gatt_server::run(cancel_token, stats_rx, locked_to).await
    }
}
