//! BLE scanning on Linux, via bluer.
//!
//! Shares its `Session` with the GATT server, so the whole process holds one
//! D-Bus connection and one set of match rules instead of two BlueZ client
//! stacks contending for the same controller.

use std::collections::HashSet;
use std::pin::Pin;
use std::time::Duration;

use async_stream::stream;
use bluer::{
    Adapter, AdapterEvent, Address, DeviceEvent, DeviceProperty, DiscoveryFilter,
    DiscoveryTransport, Session,
};
use futures_util::stream::{BoxStream, SelectAll, Stream, StreamExt};
use tokio_util::sync::CancellationToken;

use crate::BoxError;
use crate::bluetooth_hal::{Advertisement, BleScanner, ScanEvent, ScanStream};

/// BlueZ deduplicates advertising packets; periodically restarting the scan
/// forces duplicates (i.e. fresh bike readings) to keep flowing.
///
/// Kept from the btleplug implementation on purpose. `duplicate_data: true`
/// below should make it unnecessary, but that has not been confirmed on the
/// hardware, and retiring it is a separate change with its own measurement.
const SCAN_RESTART_INTERVAL: Duration = Duration::from_secs(60);
const SCAN_SETTLE_DELAY: Duration = Duration::from_millis(100);

#[derive(Clone)]
pub struct BluerScanner {
    session: Session,
}

impl BluerScanner {
    pub fn with_session(session: Session) -> Self {
        Self { session }
    }
}

impl BleScanner for BluerScanner {
    async fn scan(&self, cancel_token: CancellationToken) -> Result<ScanStream, BoxError> {
        tracing::info!("Initializing Bluetooth adapter and starting scan...");
        let adapter = self.session.default_adapter().await?;
        adapter.set_powered(true).await?;
        tracing::info!(
            "Using BLE adapter: {} ({})",
            adapter.name(),
            adapter.address().await?
        );

        // Must precede discover_devices: bluer refuses with
        // ErrorKind::DiscoveryActive if a discovery session is already open on
        // this adapter path.
        adapter.set_discovery_filter(discovery_filter()).await?;
        Ok(Box::pin(scan_stream(adapter, cancel_token)) as ScanStream)
    }
}

/// The reason for the port, and the one function that would silence the bridge
/// entirely if it were wrong.
///
/// **Never** write `..Default::default()` over these two fields. bluer's
/// `DiscoveryFilter::default()` is `transport: Auto` with
/// `duplicate_data: false` — both bugs at once, and both silent:
///
/// - `Auto` on a dual-mode controller makes BlueZ interleave LE scanning with
///   classic-Bluetooth inquiry, so the radio is deaf to the bike for seconds at
///   a time. It is exactly why `ControllerMode = le` has to be set in
///   `/etc/bluetooth/main.conf` today; stating the transport here puts the
///   requirement in code, where re-imaging an SD card cannot lose it.
/// - `duplicate_data: false` lets bluetoothd suppress a `ManufacturerData`
///   signal whose payload is unchanged — which is precisely the paused-bike
///   case. It is also load-bearing a layer down: a filter with no other
///   criteria and `duplicate_data: false` is treated by BlueZ as an *empty*
///   filter and downgraded to regular discovery, which on controllers carrying
///   `HCI_QUIRK_STRICT_DUPLICATE_FILTER` (Broadcom, i.e. the Pi Zero W) leaves
///   the controller's own duplicate filter enabled.
fn discovery_filter() -> DiscoveryFilter {
    DiscoveryFilter {
        transport: DiscoveryTransport::Le,
        duplicate_data: true,
        ..Default::default()
    }
}

type DiscoveryStream = Pin<Box<dyn Stream<Item = AdapterEvent> + Send>>;
type AddressedDeviceEvent = (Address, DeviceEvent);

fn scan_stream(
    adapter: Adapter,
    cancel_token: CancellationToken,
) -> impl Stream<Item = ScanEvent> + Send {
    stream! {
        tracing::info!("Starting scan... (Note: Keiser M3i only advertises when pedaling)");
        let mut discovery: DiscoveryStream = match adapter.discover_devices().await {
            Ok(discovery) => Box::pin(discovery),
            Err(e) => {
                yield ScanEvent::Error(e.into());
                return;
            }
        };

        // Per-device subscriptions, fanned out in-process from the session's
        // single PropertiesChanged match rule: no D-Bus round trip per device,
        // and none per advertisement. btleplug did one `get_device_info` call
        // for every manufacturer-data advertisement, purely to build an id.
        //
        // Both collections grow with every distinct BLE device seen. The 60 s
        // restart bounds them as a side effect, by clearing both; if that
        // restart is ever retired, this needs an explicit cap or a prune of
        // devices that have produced no Keiser data.
        let mut device_events: SelectAll<BoxStream<'static, AddressedDeviceEvent>> =
            SelectAll::new();
        let mut subscribed: HashSet<Address> = HashSet::new();

        let mut scan_restart_timer = tokio::time::interval(SCAN_RESTART_INTERVAL);
        scan_restart_timer.tick().await;

        loop {
            tokio::select! {
                // Ending the stream is how cancellation is reported; run_bridge
                // reads it together with the token.
                _ = cancel_token.cancelled() => break,

                _ = scan_restart_timer.tick() => {
                    tracing::info!("Periodically restarting scan...");
                    // StopDiscovery makes BlueZ delete every temporary,
                    // non-connectable device — and the M3i is one, it sends
                    // ADV_NONCONN_IND — so every per-device subscription dies
                    // with the restart, and the DeviceRemoved signals arrive
                    // while no adapter stream is held to observe them. Forget
                    // all of it and let the fresh discovery re-announce
                    // whatever still exists; keeping the set would leave the
                    // bridge permanently silent after the first restart.
                    device_events = SelectAll::new();
                    subscribed.clear();
                    if let Err(e) = restart_discovery(&adapter, &mut discovery).await {
                        yield ScanEvent::Error(e);
                        break;
                    }
                }

                adapter_event = discovery.next() => {
                    match adapter_event {
                        Some(AdapterEvent::DeviceAdded(address)) => {
                            if subscribed.insert(address) {
                                match subscribe(&adapter, address).await {
                                    Some(events) => device_events.push(events),
                                    // Leave it unsubscribed so a later
                                    // DeviceAdded can retry.
                                    None => { subscribed.remove(&address); }
                                }
                            }
                        }
                        Some(AdapterEvent::DeviceRemoved(address)) => { subscribed.remove(&address); }
                        Some(AdapterEvent::PropertyChanged(_)) => {}
                        None => {
                            // bluer ends this stream on Discovering=false, which
                            // BlueZ emits only on an explicit stop or an adapter
                            // power-down — never during the kernel's own ~10 s
                            // scan recycling. So this really is an error.
                            yield ScanEvent::Error("BlueZ discovery ended unexpectedly".into());
                            break;
                        }
                    }
                }

                Some(event) = device_events.next(), if !device_events.is_empty() => {
                    if let Some(advertisement) = to_advertisement(event) {
                        yield ScanEvent::Advertisement(advertisement);
                    }
                }
            }
        }
    }
}

/// Stops and restarts discovery, keeping the adapter's filter.
///
/// Replacing the stream is what stops it: dropping bluer's discovery stream
/// drops the session token, whose spawned task issues StopDiscovery. The settle
/// delay gives that a chance to land before a new discovery is requested.
async fn restart_discovery(
    adapter: &Adapter,
    discovery: &mut DiscoveryStream,
) -> Result<(), BoxError> {
    *discovery = Box::pin(futures_util::stream::empty());
    tokio::time::sleep(SCAN_SETTLE_DELAY).await;
    *discovery = Box::pin(adapter.discover_devices().await?);
    Ok(())
}

/// Subscribes to one device's property changes.
///
/// Deliberately does *not* read `device.manufacturer_data()` here. That returns
/// BlueZ's cached payload, which the parser would then stamp as freshly
/// received — the staleness bug of issue #5, in bluer form. The cost of not
/// doing it is one missed advertisement (~2 s) per device appearance, well
/// inside the 20 s staleness window.
async fn subscribe(
    adapter: &Adapter,
    address: Address,
) -> Option<BoxStream<'static, AddressedDeviceEvent>> {
    let device = adapter
        .device(address)
        .inspect_err(|e| tracing::debug!("Cannot open device {address}: {e}"))
        .ok()?;
    let events = device
        .events()
        .await
        .inspect_err(|e| tracing::debug!("Cannot subscribe to {address}: {e}"))
        .ok()?;
    Some(events.map(move |event| (address, event)).boxed())
}

/// Maps one device event to an advertisement, if it carries one.
///
/// Pure, so the mapping is unit-tested without a D-Bus connection.
fn to_advertisement((address, event): AddressedDeviceEvent) -> Option<Advertisement> {
    match event {
        DeviceEvent::PropertyChanged(DeviceProperty::ManufacturerData(manufacturer_data)) => {
            Some(Advertisement {
                device: address.to_string(),
                manufacturer_data,
            })
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    const ADDRESS: Address = Address::new([0x00, 0x11, 0x22, 0x33, 0x44, 0x55]);

    #[test]
    fn given_the_discovery_filter_when_built_then_le_transport_and_duplicates_are_requested() {
        // Worth more than the rest of this file's tests combined: it is a
        // compile-and-run assertion on the single line whose two silent default
        // values would leave the bridge deaf half the time and blind to a
        // paused bike.
        let filter = discovery_filter();
        assert_eq!(
            filter.transport,
            DiscoveryTransport::Le,
            "Auto interleaves LE scanning with classic inquiry"
        );
        assert!(
            filter.duplicate_data,
            "without this bluetoothd suppresses unchanged manufacturer data"
        );
    }

    #[test]
    fn given_bluers_defaults_when_compared_then_neither_is_what_this_bridge_needs() {
        // Pins *why* the two fields above are written out explicitly, so nobody
        // simplifies discovery_filter() to `Default::default()` and reintroduces
        // both bugs. bluer's own doc comment claims the default provides
        // duplicate data; it has not since 0.17.
        let defaults = DiscoveryFilter::default();
        assert_eq!(defaults.transport, DiscoveryTransport::Auto);
        assert!(!defaults.duplicate_data);
    }

    #[test]
    fn given_a_manufacturer_data_change_when_mapped_then_an_advertisement_is_produced() {
        let payload = HashMap::from([(0x0102u16, vec![1, 2, 3])]);
        let event = DeviceEvent::PropertyChanged(DeviceProperty::ManufacturerData(payload.clone()));

        let advertisement = to_advertisement((ADDRESS, event)).expect("carries an advertisement");

        assert_eq!(advertisement.manufacturer_data, payload);
        assert_eq!(advertisement.device, ADDRESS.to_string());
    }

    #[test]
    fn given_an_unrelated_property_change_when_mapped_then_nothing_is_produced() {
        // Devices emit RSSI and name changes constantly; only manufacturer data
        // is an advertisement as far as this bridge is concerned.
        let event = DeviceEvent::PropertyChanged(DeviceProperty::Rssi(-60));
        assert!(to_advertisement((ADDRESS, event)).is_none());
    }
}
