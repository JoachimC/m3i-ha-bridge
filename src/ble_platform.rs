//! The only place that knows which Bluetooth stack this build uses.
//!
//! Everything else — `main`, `bluetooth_hal`, `gatt_server`'s public entry
//! point — is written against this, so the `cfg` split does not spread.

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::BoxError;
use crate::stats::KeiserStats;

#[cfg(target_os = "linux")]
pub type PlatformScanner = crate::scan_bluer::BluerScanner;
#[cfg(not(target_os = "linux"))]
pub type PlatformScanner = crate::scan_btleplug::BtleplugScanner;

/// Owns whatever per-process Bluetooth state the platform needs.
///
/// On Linux that is a single `bluer::Session`, shared by the scanner and the
/// GATT server. They contend for one controller anyway, and sharing means one
/// D-Bus connection, one IO-resource task and one set of match rules for the
/// whole process, instead of two BlueZ client stacks that know nothing about
/// each other.
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

    /// A scanner for one bridge attempt. Cheap to make: on Linux it clones the
    /// shared session rather than opening a new D-Bus connection, so the retry
    /// loop no longer reconnects on every attempt.
    #[cfg(target_os = "linux")]
    pub fn scanner(&self) -> PlatformScanner {
        PlatformScanner::with_session(self.session.clone())
    }

    #[cfg(not(target_os = "linux"))]
    pub fn scanner(&self) -> PlatformScanner {
        PlatformScanner::default()
    }

    /// Serves the BLE GATT application until cancelled.
    #[cfg(target_os = "linux")]
    pub async fn serve_gatt(
        &self,
        cancel_token: CancellationToken,
        stats_rx: watch::Receiver<KeiserStats>,
    ) -> Result<(), BoxError> {
        crate::gatt_server::run(self.session.clone(), cancel_token, stats_rx).await
    }

    #[cfg(not(target_os = "linux"))]
    pub async fn serve_gatt(
        &self,
        cancel_token: CancellationToken,
        stats_rx: watch::Receiver<KeiserStats>,
    ) -> Result<(), BoxError> {
        tracing::warn!(
            "BLE GATT server broadcasting is only supported on Linux (BlueZ). \
             Broadcasting is disabled on this platform."
        );
        let _ = stats_rx;
        cancel_token.cancelled().await;
        Ok(())
    }
}
