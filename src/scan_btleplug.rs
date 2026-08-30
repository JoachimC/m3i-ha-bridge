//! BLE scanning everywhere except Linux, via btleplug.
//!
//! Linux uses bluer instead (`scan_bluer`), so this exists to keep the bridge
//! runnable on a macOS dev machine, behind the [`BleScanner`] trait.

use std::time::Duration;

use async_stream::stream;
use btleplug::api::{Central, CentralEvent, Manager as _, ScanFilter};
use btleplug::platform::{Adapter, Manager};
use futures_util::stream::{Stream, StreamExt};
use tokio_util::sync::CancellationToken;

use crate::BoxError;
use crate::ble_scanner::{BleScanner, ReceivedAdvertisement, ScanEvent, ScanStream};

/// Pause between stopping a scan and starting the next, so the stack has
/// processed the stop before it is asked to start again.
const SCAN_SETTLE_DELAY: Duration = Duration::from_millis(100);

#[derive(Default)]
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

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => break,
                next_event = events.next() => {
                    match next_event {
                        Some(event) => {
                            if let Some(advertisement) = to_advertisement(event) {
                                yield ScanEvent::Advertisement(advertisement);
                            }
                        }
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

/// Maps one central event to an advertisement, if it carries one.
///
/// `ManufacturerDataAdvertisement` is the only event that carries a freshly
/// received advertisement; reading a peripheral's properties would return
/// cached data and reset the staleness clock.
fn to_advertisement(event: CentralEvent) -> Option<ReceivedAdvertisement> {
    match event {
        CentralEvent::ManufacturerDataAdvertisement {
            id,
            manufacturer_data,
        } => Some(ReceivedAdvertisement {
            device: id.to_string(),
            manufacturer_data,
        }),
        _ => None,
    }
}

// btleplug's PeripheralId is opaque and only constructible from a UUID on
// macOS, which is the platform this scanner is developed on.
#[cfg(all(test, target_os = "macos"))]
mod tests {
    use super::*;
    use btleplug::platform::PeripheralId;
    use std::collections::HashMap;

    fn some_peripheral() -> PeripheralId {
        PeripheralId::from(uuid::Uuid::from_u128(0x1234))
    }

    #[test]
    fn given_a_manufacturer_data_advertisement_when_mapped_then_an_advertisement_is_produced() {
        let payload = HashMap::from([(0x0102u16, vec![1, 2, 3])]);
        let event = CentralEvent::ManufacturerDataAdvertisement {
            id: some_peripheral(),
            manufacturer_data: payload.clone(),
        };

        let advertisement = to_advertisement(event).expect("carries an advertisement");

        assert_eq!(advertisement.manufacturer_data, payload);
        assert!(!advertisement.device.is_empty());
    }

    #[test]
    fn given_any_other_central_event_when_mapped_then_nothing_is_produced() {
        // Discovery, connection and RSSI events all fire constantly; only
        // manufacturer data is an advertisement as far as this bridge is
        // concerned.
        for event in [
            CentralEvent::DeviceDiscovered(some_peripheral()),
            CentralEvent::DeviceUpdated(some_peripheral()),
            CentralEvent::DeviceConnected(some_peripheral()),
            CentralEvent::DeviceDisconnected(some_peripheral()),
        ] {
            assert!(to_advertisement(event).is_none());
        }
    }
}
