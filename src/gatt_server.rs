//! BLE GATT server, Linux only — BlueZ has no cross-platform equivalent.
//!
//! `ble_platform` decides whether this is reachable at all; on other platforms
//! it parks until cancellation instead.

#[cfg(target_os = "linux")]
mod linux_impl {
    use crate::BoxError;
    use crate::gatt_codec::{
        AdvertisedIdTracker, FTMS_FEATURE_VALUE, LEGACY_ADVERTISING_CAPACITY, NewArrivals,
        cps_has_value, ftms_has_value, hrs_has_value, initial_notification,
        legacy_advertising_size, local_name, reading_for_advertised_bike, serial_number,
        serialize_cps, serialize_ftms, serialize_hrs, wrap_u16,
    };
    use crate::stats::{KeiserStats, current_reading, next_reading};
    use bluer::{
        adv::{Advertisement, AdvertisementHandle},
        gatt::local::{
            Application, Characteristic, CharacteristicNotifier, CharacteristicNotify,
            CharacteristicNotifyMethod, CharacteristicRead, Service,
        },
    };
    use futures_util::FutureExt;
    use std::collections::BTreeSet;
    use std::time::Duration;
    use tokio::sync::watch;

    // UUIDs for standard Bluetooth SIG services and characteristics
    const FTMS_SERVICE_UUID: bluer::Uuid =
        bluer::Uuid::from_u128(0x00001826_0000_1000_8000_00805f9b34fb);
    const FTMS_FEATURE_CHAR_UUID: bluer::Uuid =
        bluer::Uuid::from_u128(0x00002acc_0000_1000_8000_00805f9b34fb);
    const INDOOR_BIKE_DATA_CHAR_UUID: bluer::Uuid =
        bluer::Uuid::from_u128(0x00002ad2_0000_1000_8000_00805f9b34fb);

    const CPS_SERVICE_UUID: bluer::Uuid =
        bluer::Uuid::from_u128(0x00001818_0000_1000_8000_00805f9b34fb);
    const CPS_FEATURE_CHAR_UUID: bluer::Uuid =
        bluer::Uuid::from_u128(0x00002a65_0000_1000_8000_00805f9b34fb);
    const CPS_MEASUREMENT_CHAR_UUID: bluer::Uuid =
        bluer::Uuid::from_u128(0x00002a63_0000_1000_8000_00805f9b34fb);
    const CPS_SENSOR_LOCATION_CHAR_UUID: bluer::Uuid =
        bluer::Uuid::from_u128(0x00002a5d_0000_1000_8000_00805f9b34fb);

    const HRS_SERVICE_UUID: bluer::Uuid =
        bluer::Uuid::from_u128(0x0000180d_0000_1000_8000_00805f9b34fb);
    const HRS_MEASUREMENT_CHAR_UUID: bluer::Uuid =
        bluer::Uuid::from_u128(0x00002a37_0000_1000_8000_00805f9b34fb);
    const HRS_BODY_SENSOR_LOCATION_CHAR_UUID: bluer::Uuid =
        bluer::Uuid::from_u128(0x00002a38_0000_1000_8000_00805f9b34fb);

    // Device Information Service: how a client that has connected can read
    // which bike this is, independent of the advertised name.
    const DIS_SERVICE_UUID: bluer::Uuid =
        bluer::Uuid::from_u128(0x0000180a_0000_1000_8000_00805f9b34fb);
    const DIS_MANUFACTURER_NAME_CHAR_UUID: bluer::Uuid =
        bluer::Uuid::from_u128(0x00002a29_0000_1000_8000_00805f9b34fb);
    const DIS_MODEL_NUMBER_CHAR_UUID: bluer::Uuid =
        bluer::Uuid::from_u128(0x00002a24_0000_1000_8000_00805f9b34fb);
    const DIS_SERIAL_NUMBER_CHAR_UUID: bluer::Uuid =
        bluer::Uuid::from_u128(0x00002a25_0000_1000_8000_00805f9b34fb);

    /// How often each notify loop re-sends the current stats even when no new
    /// advertisement arrived, so clients see values decay to zero on staleness.
    const NOTIFY_POLL_INTERVAL: Duration = Duration::from_millis(1000);

    /// Time given to BlueZ between unregistering one advertisement and
    /// registering the next.
    ///
    /// Dropping a bluer `AdvertisementHandle` does not unregister anything
    /// synchronously: it signals a spawned task, which then sends
    /// `UnregisterAdvertisement` over D-Bus. Without a pause, the new
    /// `RegisterAdvertisement` can reach bluetoothd while the old instance is
    /// still live, fail with a generic D-Bus error, exhaust the retries and
    /// drop into the btmgmt fallback for a switch that would have worked a
    /// moment later. Same idiom as `SCAN_SETTLE_DELAY` in `scan_bluer.rs`.
    const ADVERTISING_SETTLE_DELAY: Duration = Duration::from_millis(500);

    /// Service UUIDs listed in the advertising packet.
    ///
    /// FTMS has to be here, not merely discoverable after connecting: many
    /// clients — Zwift's FTMS pairing screen among them — filter discovery on
    /// 0x1826 in the advertising data, so a trainer that omits it is simply
    /// never offered, and the pairing flow never gets far enough to read the
    /// service list. FTMS v1.0 §3.1 requires it for that reason.
    ///
    /// Heart Rate (0x180D) is advertised for the same reason (issue #4):
    /// Zwift's HR pairing screen filters on it identically, so without it a
    /// rider whose strap is paired to the bike cannot pick the bridge as a
    /// heart-rate source. The characteristic only notifies while the bike
    /// reports a rate, so with no strap the sensor is offered but silent —
    /// the usual case here is a strap being worn.
    fn advertised_service_uuids() -> BTreeSet<bluer::Uuid> {
        BTreeSet::from([CPS_SERVICE_UUID, FTMS_SERVICE_UUID, HRS_SERVICE_UUID])
    }

    /// btmgmt fallback for the same advertisement, used when every D-Bus
    /// registration attempt fails. Kept beside [`advertised_service_uuids`]
    /// because the two must list the same services; a test asserts they do.
    const BTMGMT_ADD_ADV_ARGS: [&str; 11] = [
        "add-adv", "-u", "1818", "-u", "1826", "-u", "180d", "-c", "-g", "-n", "1",
    ];

    /// Serves a subscriber of one notify characteristic: sends the current
    /// stats immediately (when `has_value` says they are worth sending), then
    /// re-serializes and notifies on every stats change or poll tick until the
    /// client disconnects or the stats sender is dropped.
    ///
    /// Only the advertised bike's readings go out — the client paired to a
    /// named bike, and the channel carries every bike in range. See
    /// [`reading_for_advertised_bike`].
    ///
    /// Every payload it sends, the first one included, is built from sanitized
    /// stats — see [`initial_notification`].
    fn spawn_notify_loop<F>(
        name: &'static str,
        mut rx: watch::Receiver<KeiserStats>,
        advertised_id: watch::Receiver<Option<u8>>,
        mut notifier: CharacteristicNotifier,
        has_value: fn(&KeiserStats) -> bool,
        mut serialize: F,
    ) where
        F: FnMut(&KeiserStats) -> Vec<u8> + Send + 'static,
    {
        tokio::spawn(async move {
            tracing::info!("GATT: Client subscribed to {}", name);

            let initial = current_reading(&mut rx);
            let mut kept = reading_for_advertised_bike(None, initial, *advertised_id.borrow());
            if let Some(initial) = &kept
                && let Some(payload) = initial_notification(initial, has_value, &mut serialize)
            {
                let _ = notifier.notify(payload).await;
            }

            while let Some(stats) = next_reading(&mut rx, NOTIFY_POLL_INTERVAL).await {
                kept = reading_for_advertised_bike(kept, stats, *advertised_id.borrow());
                let Some(stats) = &kept else {
                    continue; // nothing from the advertised bike yet
                };
                let stats = stats.clone().sanitized();
                if let Err(e) = notifier.notify(serialize(&stats)).await {
                    tracing::debug!("{} notification failed: {}, removing subscriber", name, e);
                    break;
                }
            }
            tracing::info!("GATT: Client unsubscribed from {}", name);
        });
    }

    /// Builds a serializer for the Cycling Power Measurement characteristic.
    /// CPS is stateful: it must report cumulative crank revolutions and the
    /// last crank event time (1/1024 s), both wrapping at u16 as per spec.
    fn cps_serializer() -> impl FnMut(&KeiserStats) -> Vec<u8> + Send + 'static {
        let mut cumulative_revolutions: f64 = 0.0;
        let mut last_event_time: f64 = 0.0;
        let mut last_update = tokio::time::Instant::now();
        move |stats| {
            let now = tokio::time::Instant::now();
            let delta_t = now.duration_since(last_update).as_secs_f64();
            last_update = now;

            if stats.cadence > 0.0 {
                cumulative_revolutions += (stats.cadence as f64 / 60.0) * delta_t;
                last_event_time += delta_t * 1024.0;
            }

            serialize_cps(
                stats,
                wrap_u16(cumulative_revolutions),
                wrap_u16(last_event_time),
            )
        }
    }

    fn read_characteristic(uuid: bluer::Uuid, value: &'static [u8]) -> Characteristic {
        Characteristic {
            uuid,
            read: Some(CharacteristicRead {
                read: true,
                fun: Box::new(move |_req| async move { Ok(value.to_vec()) }.boxed()),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn notify_characteristic<F>(
        uuid: bluer::Uuid,
        name: &'static str,
        stats_rx: watch::Receiver<KeiserStats>,
        advertised_id: watch::Receiver<Option<u8>>,
        has_value: fn(&KeiserStats) -> bool,
        make_serializer: F,
    ) -> Characteristic
    where
        F: Fn() -> Box<dyn FnMut(&KeiserStats) -> Vec<u8> + Send> + Send + Sync + 'static,
    {
        Characteristic {
            uuid,
            notify: Some(CharacteristicNotify {
                notify: true,
                method: CharacteristicNotifyMethod::Fun(Box::new(move |notifier| {
                    let rx = stats_rx.clone();
                    let advertised_id = advertised_id.clone();
                    let serialize = make_serializer();
                    async move {
                        spawn_notify_loop(name, rx, advertised_id, notifier, has_value, serialize);
                    }
                    .boxed()
                })),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    /// Serial Number String: the advertised bike's zero-padded id, read live
    /// so it follows the advertisement when the bridge switches bikes.
    fn serial_number_characteristic(advertised_id: watch::Receiver<Option<u8>>) -> Characteristic {
        Characteristic {
            uuid: DIS_SERIAL_NUMBER_CHAR_UUID,
            read: Some(CharacteristicRead {
                read: true,
                fun: Box::new(move |_req| {
                    let serial = serial_number(*advertised_id.borrow());
                    async move { Ok(serial.into_bytes()) }.boxed()
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn build_application(
        stats_rx: &watch::Receiver<KeiserStats>,
        advertised_id: &watch::Receiver<Option<u8>>,
    ) -> Application {
        Application {
            services: vec![
                // 1. Fitness Machine Service (FTMS)
                Service {
                    uuid: FTMS_SERVICE_UUID,
                    primary: true,
                    characteristics: vec![
                        // Features: Cadence, Distance, Resistance, HR, Elapsed Time, Power.
                        read_characteristic(FTMS_FEATURE_CHAR_UUID, &FTMS_FEATURE_VALUE),
                        notify_characteristic(
                            INDOOR_BIKE_DATA_CHAR_UUID,
                            "FTMS (Indoor Bike Data)",
                            stats_rx.clone(),
                            advertised_id.clone(),
                            ftms_has_value,
                            || Box::new(serialize_ftms),
                        ),
                    ],
                    ..Default::default()
                },
                // 2. Cycling Power Service (CPS)
                Service {
                    uuid: CPS_SERVICE_UUID,
                    primary: true,
                    characteristics: vec![
                        // Features: Crank Revolution Data Supported (Bit 3 = 0x08)
                        read_characteristic(CPS_FEATURE_CHAR_UUID, &[0x08, 0x00, 0x00, 0x00]),
                        notify_characteristic(
                            CPS_MEASUREMENT_CHAR_UUID,
                            "CPS (Cycling Power)",
                            stats_rx.clone(),
                            advertised_id.clone(),
                            cps_has_value,
                            || Box::new(cps_serializer()),
                        ),
                        // Sensor Location (required for CPS): 0 = Other
                        read_characteristic(CPS_SENSOR_LOCATION_CHAR_UUID, &[0x00]),
                    ],
                    ..Default::default()
                },
                // 3. Heart Rate Service (HRS)
                Service {
                    uuid: HRS_SERVICE_UUID,
                    primary: true,
                    characteristics: vec![
                        notify_characteristic(
                            HRS_MEASUREMENT_CHAR_UUID,
                            "HRS (Heart Rate)",
                            stats_rx.clone(),
                            advertised_id.clone(),
                            hrs_has_value,
                            || Box::new(serialize_hrs),
                        ),
                        // Body Sensor Location: 1 = Chest
                        read_characteristic(HRS_BODY_SENSOR_LOCATION_CHAR_UUID, &[0x01]),
                    ],
                    ..Default::default()
                },
                // 4. Device Information Service (DIS): which bike this is.
                Service {
                    uuid: DIS_SERVICE_UUID,
                    primary: true,
                    characteristics: vec![
                        read_characteristic(DIS_MANUFACTURER_NAME_CHAR_UUID, b"Keiser"),
                        read_characteristic(DIS_MODEL_NUMBER_CHAR_UUID, b"M3i"),
                        serial_number_characteristic(advertised_id.clone()),
                    ],
                    ..Default::default()
                },
            ],
            ..Default::default()
        }
    }

    /// A registered advertisement, by whichever route succeeded.
    enum AdvertisingHandle {
        /// Dropping the handle unregisters it.
        DBus(AdvertisementHandle),
        /// Registered through `btmgmt add-adv`; needs `rm-adv` to remove.
        Btmgmt,
    }

    /// Registers the advertisement under `name`, retrying and finally falling
    /// back to `btmgmt` — advertising registration on this hardware is fragile
    /// enough that both are needed.
    async fn register_advertisement(
        adapter: &bluer::Adapter,
        name: &str,
    ) -> Result<AdvertisingHandle, BoxError> {
        let service_uuids = advertised_service_uuids();

        // BlueZ rejects an oversized advertisement with a generic D-Bus error,
        // which the retry loop below then reports five times without ever
        // saying what is wrong. Say it up front instead.
        let payload_size = legacy_advertising_size(name, service_uuids.len());
        if payload_size > LEGACY_ADVERTISING_CAPACITY {
            tracing::warn!(
                "Advertising payload is {} bytes, over the {}-byte legacy limit; \
                 BlueZ will likely refuse to register it",
                payload_size,
                LEGACY_ADVERTISING_CAPACITY
            );
        } else {
            tracing::debug!(
                "Advertising {} service UUIDs in {} of {} bytes",
                service_uuids.len(),
                payload_size,
                LEGACY_ADVERTISING_CAPACITY
            );
        }

        let mut le_advertisement = Advertisement {
            advertisement_type: bluer::adv::Type::Peripheral,
            service_uuids,
            discoverable: Some(true),
            local_name: Some(name.to_string()),
            min_interval: Some(std::time::Duration::from_millis(300)),
            max_interval: Some(std::time::Duration::from_millis(500)),
            ..Default::default()
        };

        let mut retries = 5;
        let mut try_without_intervals = false;

        loop {
            if try_without_intervals {
                le_advertisement.min_interval = None;
                le_advertisement.max_interval = None;
            }

            match adapter.advertise(le_advertisement.clone()).await {
                Ok(handle) => return Ok(AdvertisingHandle::DBus(handle)),
                Err(e) => {
                    retries -= 1;
                    tracing::warn!(
                        "Failed to register BLE advertisement via BlueZ D-Bus (retries left: {}): {}. {}",
                        retries,
                        e,
                        if !try_without_intervals {
                            "Retrying without custom intervals next..."
                        } else {
                            "Retrying..."
                        }
                    );
                    try_without_intervals = true;
                    if retries > 0 {
                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                        continue;
                    }

                    tracing::warn!(
                        "All D-Bus advertising attempts failed. Falling back to manual btmgmt legacy advertising..."
                    );

                    // btmgmt's -n flag advertises the adapter's own alias, so
                    // the bike's name has to be set there first. BlueZ persists
                    // the alias on disk; `run` restores the original at shutdown.
                    if let Err(alias_err) = adapter.set_alias(name.to_string()).await {
                        tracing::warn!("Could not set adapter alias to {:?}: {}", name, alias_err);
                    }

                    // Try clearing any existing instance 1 first
                    let _ = run_btmgmt(&["rm-adv", "1"]).await;

                    // Register using btmgmt. The -u list must mirror
                    // advertised_service_uuids(), or the fallback path
                    // silently drops FTMS and the trainer stops appearing
                    // in FTMS pairing screens exactly when D-Bus
                    // registration is already misbehaving.
                    return match run_btmgmt(&BTMGMT_ADD_ADV_ARGS).await {
                        Ok(_) => {
                            tracing::info!(
                                "Successfully registered legacy advertisement via btmgmt fallback!"
                            );
                            Ok(AdvertisingHandle::Btmgmt)
                        }
                        Err(bt_err) => {
                            tracing::error!("btmgmt fallback also failed: {}", bt_err);
                            Err(e.into())
                        }
                    };
                }
            }
        }
    }

    /// Unregisters an advertisement and waits for BlueZ to have processed it,
    /// so the caller may register another immediately afterwards.
    async fn unregister_advertisement(handle: AdvertisingHandle) {
        match handle {
            AdvertisingHandle::DBus(handle) => {
                drop(handle);
                tokio::time::sleep(ADVERTISING_SETTLE_DELAY).await;
            }
            AdvertisingHandle::Btmgmt => {
                if let Err(e) = run_btmgmt(&["rm-adv", "1"]).await {
                    tracing::error!("Failed to remove btmgmt advertisement: {}", e);
                } else {
                    tracing::info!("Successfully removed legacy advertisement via btmgmt.");
                }
            }
        }
    }

    /// Takes the session rather than opening one: the scanner and the GATT
    /// server contend for the same controller, so the process holds a single
    /// D-Bus connection (see `ble_platform`).
    pub async fn run(
        session: bluer::Session,
        cancel_token: tokio_util::sync::CancellationToken,
        stats_rx: watch::Receiver<KeiserStats>,
    ) -> Result<(), BoxError> {
        tracing::info!("Initializing BLE GATT server via bluer...");

        let adapter = session.default_adapter().await?;
        adapter.set_powered(true).await?;
        // The btmgmt fallback renames the adapter, and BlueZ persists that in
        // /var/lib/bluetooth across restarts and reboots, so remember what to
        // put back.
        let original_alias = adapter.alias().await?;

        tracing::info!(
            "Serving standard BLE services on adapter {} with address {}",
            adapter.name(),
            adapter.address().await?
        );

        // What the advertisement currently says, for the DIS serial number.
        let (advertised_tx, advertised_rx) = watch::channel(None);

        tracing::info!("Registering GATT application...");
        let app = build_application(&stats_rx, &advertised_rx);
        let app_handle = adapter.serve_gatt_application(app).await?;
        tracing::info!("GATT application served successfully.");

        // Issue #6: nothing is advertised until a bike has been heard, and the
        // name then carries that bike's id. The tracker decides when a
        // different bike has held the "latest" slot long enough to take over.
        let mut stats_rx = stats_rx;
        let mut tracker = AdvertisedIdTracker::default();
        let mut arrivals = NewArrivals::default();
        let mut advertisement: Option<AdvertisingHandle> = None;

        if let Some(bike_id) = arrivals.bike_id_if_new(&current_reading(&mut stats_rx)) {
            tracker.observe(bike_id, tokio::time::Instant::now());
        }
        tracing::info!("Waiting for a bike before advertising...");

        loop {
            let reading = tokio::select! {
                _ = cancel_token.cancelled() => break,
                reading = next_reading(&mut stats_rx, NOTIFY_POLL_INTERVAL) => reading,
            };
            let now = tokio::time::Instant::now();
            match reading {
                Some(stats) => {
                    if let Some(bike_id) = arrivals.bike_id_if_new(&stats) {
                        tracker.observe(bike_id, now);
                    }
                }
                None => {
                    // The producer is gone; keep serving what is registered
                    // until shutdown, as before.
                    cancel_token.cancelled().await;
                    break;
                }
            }

            if let Some(bike_id) = tracker.take_due(now) {
                let name = local_name(bike_id);
                if let Some(previous) = advertisement.take() {
                    tracing::info!("Re-advertising as {:?}", name);
                    unregister_advertisement(previous).await;
                } else {
                    tracing::info!("Advertising as {:?}", name);
                }
                // Fail fast, per the module policy: a registration that fails
                // even via btmgmt does not heal in-process.
                advertisement = Some(register_advertisement(&adapter, &name).await?);
                let _ = advertised_tx.send(Some(bike_id));
                tracing::info!("BLE broadcasting active as {:?}", name);
            }
        }

        tracing::info!("Shutting down BLE GATT server and advertising...");
        if let Some(handle) = advertisement {
            unregister_advertisement(handle).await;
        }
        drop(app_handle);
        restore_alias(&adapter, &original_alias).await;
        // Same reason as the settle delay: the GATT application's
        // unregistration is asynchronous too, and this process exits next.
        tokio::time::sleep(Duration::from_secs(1)).await;

        Ok(())
    }

    /// Puts the adapter's alias back if the btmgmt fallback changed it.
    async fn restore_alias(adapter: &bluer::Adapter, original: &str) {
        match adapter.alias().await {
            Ok(current) if current == original => {}
            Ok(current) => match adapter.set_alias(original.to_string()).await {
                Ok(()) => tracing::info!(
                    "Restored adapter alias {:?} (was {:?} while advertising)",
                    original,
                    current
                ),
                Err(e) => tracing::warn!("Could not restore adapter alias {:?}: {}", original, e),
            },
            Err(e) => tracing::warn!("Could not read adapter alias to restore it: {}", e),
        }
    }

    async fn run_btmgmt(args: &[&str]) -> Result<(), BoxError> {
        let mut cmd = tokio::process::Command::new("/usr/bin/btmgmt");
        cmd.args(args);
        let output = cmd.output().await?;
        if output.status.success() {
            Ok(())
        } else {
            let err = String::from_utf8_lossy(&output.stderr).into_owned();
            Err(format!("btmgmt failed: {err}").into())
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        /// The 16-bit alias of a Bluetooth SIG base UUID, as it appears in the
        /// advertising packet and in btmgmt's `-u` arguments.
        fn short_uuid(uuid: bluer::Uuid) -> String {
            format!("{:04x}", (uuid.as_u128() >> 96) as u16)
        }

        #[test]
        fn given_the_advertisement_when_built_then_it_lists_every_service_a_pairing_screen_filters_on()
         {
            // 0x1826 is the point of issue #8 and 0x180D of issue #4: clients
            // filter discovery on the advertised UUID, so a service that is
            // only discoverable after connecting is never offered.
            let advertised = advertised_service_uuids();
            assert!(
                advertised.contains(&FTMS_SERVICE_UUID),
                "Fitness Machine (0x1826)"
            );
            assert!(
                advertised.contains(&CPS_SERVICE_UUID),
                "Cycling Power (0x1818)"
            );
            assert!(
                advertised.contains(&HRS_SERVICE_UUID),
                "Heart Rate (0x180D)"
            );
        }

        #[test]
        fn given_the_btmgmt_fallback_when_compared_then_it_advertises_the_same_services() {
            // The fallback runs precisely when D-Bus registration is already
            // failing, so a divergence here would only ever show up on a Pi
            // that is having a bad day.
            let via_btmgmt: BTreeSet<String> = BTMGMT_ADD_ADV_ARGS
                .windows(2)
                .filter(|pair| pair[0] == "-u")
                .map(|pair| pair[1].to_string())
                .collect();
            let via_dbus: BTreeSet<String> = advertised_service_uuids()
                .iter()
                .copied()
                .map(short_uuid)
                .collect();

            assert_eq!(via_btmgmt, via_dbus);
        }

        #[test]
        fn given_the_advertisement_when_sized_then_it_fits_a_legacy_advertising_packet() {
            // Three digits is the widest id, so bike 200 is the longest name.
            let size = legacy_advertising_size(&local_name(200), advertised_service_uuids().len());
            assert!(
                size <= LEGACY_ADVERTISING_CAPACITY,
                "advertisement needs {size} bytes, over the {LEGACY_ADVERTISING_CAPACITY}-byte \
                 legacy limit — BlueZ would refuse to register it"
            );
        }
    }
}

#[cfg(target_os = "linux")]
pub use linux_impl::run;
