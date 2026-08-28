//! BLE scanning everywhere except Linux, via btleplug.
//!
//! Linux uses bluer instead (`scan_bluer`), so this exists to keep the bridge
//! runnable on a macOS dev machine. It is the implementation the whole project
//! used before that port, behind the [`BleScanner`] trait.

use std::time::Duration;

use async_stream::stream;
use btleplug::api::{Central, CentralEvent, Manager as _, ScanFilter};
use btleplug::platform::{Adapter, Manager};
use futures_util::stream::{Stream, StreamExt};
use tokio_util::sync::CancellationToken;

use crate::BoxError;
use crate::bluetooth_hal::{Advertisement, BleScanner, ScanEvent, ScanStream};

const SCAN_RESTART_INTERVAL: Duration = Duration::from_secs(60);
const SCAN_SETTLE_DELAY: Duration = Duration::from_millis(100);

#[derive(Clone, Default)]
pub struct BtleplugScanner;

impl BleScanner for BtleplugScanner {
    async fn scan(&self, cancel_token: CancellationToken) -> Result<ScanStream, BoxError> {
        tracing::info!("Initializing Bluetooth adapter and starting scan...");
        let manager = Manager::new().await?;
        let adapters = manager.adapters().await?;
        let central = adapters
            .into_iter()
            .next()
            .ok_or("No Bluetooth adapter found")?;

        tracing::info!("Using BLE adapter: {:?}", central.adapter_info().await?);
        Ok(Box::pin(scan_stream(central, cancel_token)) as ScanStream)
    }
}

fn scan_stream(
    central: Adapter,
    cancel_token: CancellationToken,
) -> impl Stream<Item = ScanEvent> + Send {
    stream! {
        let mut events = match central.events().await {
            Ok(events) => events,
            Err(e) => {
                yield ScanEvent::Error(e.into());
                return;
            }
        };

        let filter = ScanFilter::default();

        tracing::info!("Starting scan... (Note: Keiser M3i only advertises when pedaling)");
        let _ = central.stop_scan().await;
        tokio::time::sleep(SCAN_SETTLE_DELAY).await;
        if let Err(e) = central.start_scan(filter.clone()).await {
            yield ScanEvent::Error(e.into());
            return;
        }

        let mut scan_restart_timer = tokio::time::interval(SCAN_RESTART_INTERVAL);
        scan_restart_timer.tick().await;

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => break,
                _ = scan_restart_timer.tick() => {
                    tracing::info!("Periodically restarting scan...");
                    let _ = central.stop_scan().await;
                    tokio::time::sleep(SCAN_SETTLE_DELAY).await;
                    if let Err(e) = central.start_scan(filter.clone()).await {
                        yield ScanEvent::Error(e.into());
                        break;
                    }
                }
                next_event = events.next() => {
                    match next_event {
                        // ManufacturerDataAdvertisement is the only event that
                        // carries a freshly received advertisement; reading a
                        // peripheral's properties would return cached data and
                        // reset the staleness clock (issue #5).
                        Some(CentralEvent::ManufacturerDataAdvertisement { id, manufacturer_data }) => {
                            yield ScanEvent::Advertisement(Advertisement {
                                device: id.to_string(),
                                manufacturer_data,
                            });
                        }
                        Some(_) => {}
                        None => {
                            yield ScanEvent::Error("Bluetooth event stream ended unexpectedly".into());
                            break;
                        }
                    }
                }
            }
        }
    }
}
