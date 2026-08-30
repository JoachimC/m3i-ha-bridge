//! BLE scanning on Linux, via bluer.
//!
//! Shares its `Session` with the GATT server, so the whole process holds one
//! D-Bus connection and one set of match rules instead of two BlueZ client
//! stacks contending for the same controller.

use std::collections::HashSet;
use std::pin::Pin;

use async_stream::stream;
use bluer::{
    Adapter, AdapterEvent, Address, DeviceEvent, DeviceProperty, DiscoveryFilter,
    DiscoveryTransport, Session,
};
use futures_util::stream::{BoxStream, SelectAll, Stream, StreamExt};
use tokio_util::sync::CancellationToken;

use crate::BoxError;
use crate::ble_scanner::{BleScanner, ReceivedAdvertisement, ScanEvent, ScanStream};

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
///   a time. This filter is the only LE-only control: the Pi's
///   `/etc/bluetooth/main.conf` sets no `ControllerMode`, and a measured ride
///   confirms that the filter alone keeps the update rate healthy (issue #3).
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

        let mut subscriptions = DeviceSubscriptions::default();

        loop {
            tokio::select! {
                // Ending the stream is how cancellation is reported; run_bridge
                // reads it together with the token.
                _ = cancel_token.cancelled() => break,

                adapter_event = discovery.next() => {
                    match adapter_event {
                        Some(AdapterEvent::DeviceAdded(address)) => {
                            subscriptions.try_subscribe(&adapter, address).await;
                        }
                        Some(AdapterEvent::DeviceRemoved(address)) => subscriptions.forget(address),
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

                Some(event) = subscriptions.next_event(), if subscriptions.has_any() => {
                    if let Some(advertisement) = to_advertisement(event) {
                        yield ScanEvent::Advertisement(advertisement);
                    }
                }
            }
        }
    }
}

/// Upper bound on live per-device subscriptions.
///
/// The set grows with every distinct BLE device that the radio hears, and
/// the scan runs for the life of the process, so this cap bounds the set.
/// `DeviceRemoved` events free slots when BlueZ purges a device. The cap is
/// generous for a home: hitting it means an abnormal radio environment, and
/// the warning in `try_subscribe` makes that visible in the journal.
const MAX_SUBSCRIPTIONS: usize = 128;

/// Per-device subscriptions, fanned out in-process from the session's single
/// PropertiesChanged match rule: no D-Bus round trip per device, and none per
/// advertisement.
///
/// Bounded by [`MAX_SUBSCRIPTIONS`].
#[derive(Default)]
struct DeviceSubscriptions {
    events: SelectAll<BoxStream<'static, AddressedDeviceEvent>>,
    subscribed: HashSet<Address>,
}

impl DeviceSubscriptions {
    /// Subscribes to one device's property changes, once. A failed
    /// subscription is forgotten so a later DeviceAdded can retry.
    ///
    /// Deliberately does *not* read `device.manufacturer_data()`. That returns
    /// BlueZ's cached payload, which would then be stamped as freshly
    /// received. The cost of not doing it is one missed advertisement (~2 s)
    /// per device appearance, well inside the 20 s staleness window.
    async fn try_subscribe(&mut self, adapter: &Adapter, address: Address) {
        if self.subscribed.contains(&address) {
            return;
        }
        if self.is_full() {
            tracing::warn!("Subscription cap of {MAX_SUBSCRIPTIONS} reached; ignoring {address}");
            return;
        }
        self.subscribed.insert(address);
        let events = adapter
            .device(address)
            .inspect_err(|e| tracing::debug!("Cannot open device {address}: {e}"))
            .ok()
            .map(|device| async move { device.events().await });
        let events = match events {
            Some(events) => events
                .await
                .inspect_err(|e| tracing::debug!("Cannot subscribe to {address}: {e}"))
                .ok(),
            None => None,
        };
        match events {
            Some(events) => self
                .events
                .push(events.map(move |event| (address, event)).boxed()),
            None => {
                self.subscribed.remove(&address);
            }
        }
    }

    fn forget(&mut self, address: Address) {
        self.subscribed.remove(&address);
    }

    fn is_full(&self) -> bool {
        self.subscribed.len() >= MAX_SUBSCRIPTIONS
    }

    fn has_any(&self) -> bool {
        !self.events.is_empty()
    }

    async fn next_event(&mut self) -> Option<AddressedDeviceEvent> {
        self.events.next().await
    }
}

/// Maps one device event to an advertisement, if it carries one.
///
/// Pure, so the mapping is unit-tested without a D-Bus connection.
fn to_advertisement((address, event): AddressedDeviceEvent) -> Option<ReceivedAdvertisement> {
    match event {
        DeviceEvent::PropertyChanged(DeviceProperty::ManufacturerData(manufacturer_data)) => {
            Some(ReceivedAdvertisement {
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

    fn distinct_address(i: usize) -> Address {
        Address::new([0xC0, 0, 0, 0, (i / 256) as u8, (i % 256) as u8])
    }

    #[test]
    fn given_max_subscriptions_when_one_more_device_appears_then_the_set_is_full() {
        // Without the cap, the set grows with every distinct BLE device for
        // the life of the process.
        let mut subscriptions = DeviceSubscriptions::default();
        for i in 0..MAX_SUBSCRIPTIONS {
            subscriptions.subscribed.insert(distinct_address(i));
        }
        assert!(subscriptions.is_full());
    }

    #[test]
    fn given_a_full_set_when_a_device_is_forgotten_then_a_slot_frees() {
        // A permanent lockout at the cap would silence a bike that BlueZ
        // removes and later re-announces; forget() must free its slot.
        let mut subscriptions = DeviceSubscriptions::default();
        for i in 0..MAX_SUBSCRIPTIONS {
            subscriptions.subscribed.insert(distinct_address(i));
        }
        subscriptions.forget(distinct_address(0));
        assert!(!subscriptions.is_full());
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
