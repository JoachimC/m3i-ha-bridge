//! BLE GATT server on BlueZ: the standard fitness services, and the
//! [`Advertiser`] that broadcasts the advertising policy of `advertising`.

#[cfg(target_os = "linux")]
mod linux_impl {
    use crate::BoxError;
    use crate::advertising::{
        ADVERTISED_SERVICE_UUIDS, Advertiser, LEGACY_ADVERTISING_CAPACITY, btmgmt_add_adv_args,
        legacy_advertising_size, sig_uuid, track_advertised_bike,
    };
    use crate::gatt_codec::{
        CrankAccumulator, FTMS_FEATURE_VALUE, cps_has_value, ftms_has_value, hrs_has_value,
        initial_notification, serialize_cps, serialize_ftms, serialize_hrs,
    };
    use crate::stats::{BikeId, Fleet, Sanitized, current_snapshot, next_snapshot};
    use bluer::{
        adv::{Advertisement, AdvertisementHandle},
        gatt::local::{
            Application, Characteristic, CharacteristicNotifier, CharacteristicNotify,
            CharacteristicNotifyMethod, CharacteristicRead, Service,
        },
    };
    use futures_util::FutureExt;
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::watch;

    const fn uuid(short: u16) -> bluer::Uuid {
        bluer::Uuid::from_u128(sig_uuid(short))
    }

    const FTMS_SERVICE_UUID: bluer::Uuid = uuid(crate::advertising::FITNESS_MACHINE_SERVICE);
    const FTMS_FEATURE_CHAR_UUID: bluer::Uuid = uuid(0x2acc);
    const INDOOR_BIKE_DATA_CHAR_UUID: bluer::Uuid = uuid(0x2ad2);

    const CPS_SERVICE_UUID: bluer::Uuid = uuid(crate::advertising::CYCLING_POWER_SERVICE);
    const CPS_FEATURE_CHAR_UUID: bluer::Uuid = uuid(0x2a65);
    const CPS_MEASUREMENT_CHAR_UUID: bluer::Uuid = uuid(0x2a63);
    const CPS_SENSOR_LOCATION_CHAR_UUID: bluer::Uuid = uuid(0x2a5d);

    const HRS_SERVICE_UUID: bluer::Uuid = uuid(crate::advertising::HEART_RATE_SERVICE);
    const HRS_MEASUREMENT_CHAR_UUID: bluer::Uuid = uuid(0x2a37);
    const HRS_BODY_SENSOR_LOCATION_CHAR_UUID: bluer::Uuid = uuid(0x2a38);

    /// Device Information Service: how a client that has connected can read
    /// which bike this is, independent of the advertised name.
    const DIS_SERVICE_UUID: bluer::Uuid = uuid(0x180a);
    const DIS_MANUFACTURER_NAME_CHAR_UUID: bluer::Uuid = uuid(0x2a29);
    const DIS_MODEL_NUMBER_CHAR_UUID: bluer::Uuid = uuid(0x2a24);
    const DIS_SERIAL_NUMBER_CHAR_UUID: bluer::Uuid = uuid(0x2a25);

    /// How often each notify loop sends the current stats again when no new
    /// advertisement arrived, so clients see values decay to zero when a
    /// reading becomes stale.
    const NOTIFY_POLL_INTERVAL: Duration = Duration::from_millis(1000);

    /// The advertising interval range that the bridge requests from BlueZ.
    /// Retries omit it, because some controllers reject custom intervals.
    const ADVERTISING_MIN_INTERVAL: Duration = Duration::from_millis(300);
    const ADVERTISING_MAX_INTERVAL: Duration = Duration::from_millis(500);
    /// The number of D-Bus registration attempts before the btmgmt
    /// fallback, and the pause between them.
    const ADVERTISING_ATTEMPTS: usize = 5;
    const ADVERTISING_RETRY_DELAY: Duration = Duration::from_millis(500);

    /// Time that the bridge gives BlueZ between the unregistration of one
    /// advertisement and the registration of the next.
    ///
    /// A drop of a bluer `AdvertisementHandle` does not unregister anything
    /// synchronously: it signals a spawned task, which then sends
    /// `UnregisterAdvertisement` over D-Bus. Without a pause, the new
    /// `RegisterAdvertisement` can reach bluetoothd while the old instance
    /// is still live. The call then fails with a generic D-Bus error,
    /// exhausts the retries, and uses the btmgmt fallback for a switch that
    /// would succeed a moment later. `SCAN_SETTLE_DELAY` in
    /// `scan_btleplug.rs` follows the same pattern.
    const ADVERTISING_SETTLE_DELAY: Duration = Duration::from_millis(500);

    /// Time for BlueZ to process the GATT application's unregistration
    /// before the process exits; the operation is asynchronous for the same
    /// reason.
    const GATT_UNREGISTER_SETTLE_DELAY: Duration = Duration::from_secs(1);

    const BTMGMT: &str = "/usr/bin/btmgmt";
    /// The advertising instance the btmgmt fallback registers and removes.
    const BTMGMT_INSTANCE: &str = "1";

    /// Serves a subscriber of one notify characteristic: sends the current
    /// stats immediately (when `has_value` approves them), then serializes
    /// and notifies again on every stats change or poll tick. The loop ends
    /// when the client disconnects or the stats sender drops.
    ///
    /// The loop sends only the advertised bike's reading: the client paired
    /// to a named bike, and the snapshot carries every bike in range. The
    /// send on every tick lets the reading decay to zero when it becomes
    /// stale.
    ///
    /// The loop builds every payload, the first one included, from
    /// sanitized stats — see [`initial_notification`].
    fn spawn_notify_loop<F>(
        name: &'static str,
        mut rx: watch::Receiver<Arc<Fleet>>,
        advertised_id: watch::Receiver<Option<BikeId>>,
        mut notifier: CharacteristicNotifier,
        has_value: fn(&Sanitized) -> bool,
        mut serialize: F,
    ) where
        F: FnMut(&Sanitized) -> Vec<u8> + Send + 'static,
    {
        tokio::spawn(async move {
            tracing::info!("GATT: Client subscribed to {}", name);

            // The closure reads the advertised id fresh each time and drops
            // the guard before any await; the channel is a shared cell here,
            // not a stream.
            let advertised =
                |advertised_id: &watch::Receiver<Option<BikeId>>| *advertised_id.borrow();

            let initial = current_snapshot(&mut rx);
            if let Some(reading) = advertised(&advertised_id).and_then(|id| initial.get(&id))
                && let Some(payload) = initial_notification(reading, has_value, &mut serialize)
            {
                let _ = notifier.notify(payload).await;
            }

            while let Some(fleet) = next_snapshot(&mut rx, NOTIFY_POLL_INTERVAL).await {
                let Some(reading) = advertised(&advertised_id).and_then(|id| fleet.get(&id)) else {
                    continue;
                };
                let stats = reading.sanitized();
                if let Err(e) = notifier.notify(serialize(&stats)).await {
                    tracing::debug!("{} notification failed: {}, removing subscriber", name, e);
                    break;
                }
            }
            tracing::info!("GATT: Client unsubscribed from {}", name);
        });
    }

    fn cps_serializer() -> impl FnMut(&Sanitized) -> Vec<u8> + Send + 'static {
        let mut crank = CrankAccumulator::new(tokio::time::Instant::now());
        move |stats| {
            let (revolutions, event_time) =
                crank.advance(stats.cadence, tokio::time::Instant::now());
            serialize_cps(stats, revolutions, event_time)
        }
    }

    /// A readable characteristic; each read produces a fresh value.
    fn read_characteristic(
        uuid: bluer::Uuid,
        value: impl Fn() -> Vec<u8> + Send + Sync + 'static,
    ) -> Characteristic {
        Characteristic {
            uuid,
            read: Some(CharacteristicRead {
                read: true,
                fun: Box::new(move |_req| {
                    let value = value();
                    async move { Ok(value) }.boxed()
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn notify_characteristic<F>(
        uuid: bluer::Uuid,
        name: &'static str,
        stats_rx: watch::Receiver<Arc<Fleet>>,
        advertised_id: watch::Receiver<Option<BikeId>>,
        has_value: fn(&Sanitized) -> bool,
        make_serializer: F,
    ) -> Characteristic
    where
        F: Fn() -> Box<dyn FnMut(&Sanitized) -> Vec<u8> + Send> + Send + Sync + 'static,
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

    fn build_application(
        stats_rx: &watch::Receiver<Arc<Fleet>>,
        advertised_id: &watch::Receiver<Option<BikeId>>,
    ) -> Application {
        let serial_number = {
            let advertised_id = advertised_id.clone();
            move || {
                advertised_id
                    .borrow()
                    .map(|id| id.to_string())
                    .unwrap_or_default()
                    .into_bytes()
            }
        };
        Application {
            services: vec![
                Service {
                    uuid: FTMS_SERVICE_UUID,
                    primary: true,
                    characteristics: vec![
                        read_characteristic(FTMS_FEATURE_CHAR_UUID, || FTMS_FEATURE_VALUE.to_vec()),
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
                Service {
                    uuid: CPS_SERVICE_UUID,
                    primary: true,
                    characteristics: vec![
                        // Crank Revolution Data Supported (bit 3).
                        read_characteristic(CPS_FEATURE_CHAR_UUID, || vec![0x08, 0x00, 0x00, 0x00]),
                        notify_characteristic(
                            CPS_MEASUREMENT_CHAR_UUID,
                            "CPS (Cycling Power)",
                            stats_rx.clone(),
                            advertised_id.clone(),
                            cps_has_value,
                            || Box::new(cps_serializer()),
                        ),
                        // Sensor Location (required for CPS): 0 = Other.
                        read_characteristic(CPS_SENSOR_LOCATION_CHAR_UUID, || vec![0x00]),
                    ],
                    ..Default::default()
                },
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
                        // Body Sensor Location: 1 = Chest.
                        read_characteristic(HRS_BODY_SENSOR_LOCATION_CHAR_UUID, || vec![0x01]),
                    ],
                    ..Default::default()
                },
                Service {
                    uuid: DIS_SERVICE_UUID,
                    primary: true,
                    characteristics: vec![
                        read_characteristic(DIS_MANUFACTURER_NAME_CHAR_UUID, || b"Keiser".to_vec()),
                        read_characteristic(DIS_MODEL_NUMBER_CHAR_UUID, || b"M3i".to_vec()),
                        read_characteristic(DIS_SERIAL_NUMBER_CHAR_UUID, serial_number),
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
        /// `btmgmt add-adv` registered it; `rm-adv` removes it.
        Btmgmt,
    }

    /// Broadcasts advertisements through BlueZ, with the btmgmt fallback.
    struct BluerAdvertiser {
        adapter: bluer::Adapter,
        current: Option<AdvertisingHandle>,
    }

    impl Advertiser for BluerAdvertiser {
        async fn advertise(&mut self, name: &str) -> Result<(), BoxError> {
            self.stop().await;
            self.current = Some(register_advertisement(&self.adapter, name).await?);
            Ok(())
        }

        async fn stop(&mut self) {
            match self.current.take() {
                None => {}
                Some(AdvertisingHandle::DBus(handle)) => {
                    drop(handle);
                    tokio::time::sleep(ADVERTISING_SETTLE_DELAY).await;
                }
                Some(AdvertisingHandle::Btmgmt) => {
                    if let Err(e) = run_btmgmt(&["rm-adv", BTMGMT_INSTANCE]).await {
                        tracing::error!("Failed to remove btmgmt advertisement: {}", e);
                    } else {
                        tracing::info!("Removed legacy advertisement via btmgmt.");
                    }
                }
            }
        }
    }

    /// Registers the advertisement under `name` via D-Bus, with `btmgmt` as
    /// the fallback. Advertising registration on this hardware is
    /// unreliable, so the bridge needs both routes.
    async fn register_advertisement(
        adapter: &bluer::Adapter,
        name: &str,
    ) -> Result<AdvertisingHandle, BoxError> {
        warn_if_oversized(name);
        match advertise_via_dbus(adapter, name).await {
            Ok(handle) => Ok(AdvertisingHandle::DBus(handle)),
            Err(dbus_error) => {
                tracing::warn!(
                    "All D-Bus advertising attempts failed ({}). Falling back to btmgmt...",
                    dbus_error
                );
                match advertise_via_btmgmt(adapter, name).await {
                    Ok(()) => Ok(AdvertisingHandle::Btmgmt),
                    Err(btmgmt_error) => {
                        tracing::error!("btmgmt fallback also failed: {}", btmgmt_error);
                        Err(dbus_error.into())
                    }
                }
            }
        }
    }

    /// BlueZ rejects an oversized advertisement with a generic D-Bus error,
    /// and the retry loop reports that error several times without the
    /// cause. This warning states the cause before the first attempt.
    fn warn_if_oversized(name: &str) {
        let payload_size = legacy_advertising_size(name, ADVERTISED_SERVICE_UUIDS.len());
        if payload_size > LEGACY_ADVERTISING_CAPACITY {
            tracing::warn!(
                "Advertising payload is {} bytes, over the {}-byte legacy limit; \
                 BlueZ will likely refuse to register it",
                payload_size,
                LEGACY_ADVERTISING_CAPACITY
            );
        }
    }

    /// One attempt with the preferred intervals, then the remaining attempts
    /// without them.
    async fn advertise_via_dbus(
        adapter: &bluer::Adapter,
        name: &str,
    ) -> Result<AdvertisementHandle, bluer::Error> {
        let service_uuids: BTreeSet<bluer::Uuid> =
            ADVERTISED_SERVICE_UUIDS.into_iter().map(uuid).collect();
        let mut advertisement = Advertisement {
            advertisement_type: bluer::adv::Type::Peripheral,
            service_uuids,
            discoverable: Some(true),
            local_name: Some(name.to_string()),
            min_interval: Some(ADVERTISING_MIN_INTERVAL),
            max_interval: Some(ADVERTISING_MAX_INTERVAL),
            ..Default::default()
        };

        let mut last_error = None;
        for attempt in 1..=ADVERTISING_ATTEMPTS {
            match adapter.advertise(advertisement.clone()).await {
                Ok(handle) => return Ok(handle),
                Err(e) => {
                    tracing::warn!(
                        "Failed to register BLE advertisement via BlueZ D-Bus (attempt {} of {}): {}",
                        attempt,
                        ADVERTISING_ATTEMPTS,
                        e
                    );
                    last_error = Some(e);
                }
            }
            advertisement.min_interval = None;
            advertisement.max_interval = None;
            if attempt < ADVERTISING_ATTEMPTS {
                tokio::time::sleep(ADVERTISING_RETRY_DELAY).await;
            }
        }
        Err(last_error.expect("at least one attempt was made"))
    }

    /// btmgmt's `-n` flag advertises the adapter's own alias, so this
    /// function first sets the alias to the bike's name. BlueZ persists the
    /// alias on disk; [`run`] restores the original at shutdown.
    async fn advertise_via_btmgmt(adapter: &bluer::Adapter, name: &str) -> Result<(), BoxError> {
        if let Err(e) = adapter.set_alias(name.to_string()).await {
            tracing::warn!("Could not set adapter alias to {:?}: {}", name, e);
        }
        // A previous process that died before its cleanup can leave the
        // instance registered.
        let _ = run_btmgmt(&["rm-adv", BTMGMT_INSTANCE]).await;
        let args = btmgmt_add_adv_args();
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        run_btmgmt(&args).await?;
        tracing::info!("Registered legacy advertisement via btmgmt fallback.");
        Ok(())
    }

    /// Takes the session rather than opening one: the scanner and the GATT
    /// server contend for the same controller, so the process holds a single
    /// D-Bus connection (see `ble_platform`).
    pub async fn run(
        session: bluer::Session,
        cancel_token: tokio_util::sync::CancellationToken,
        stats_rx: watch::Receiver<Arc<Fleet>>,
        locked_to: Option<BikeId>,
    ) -> Result<(), BoxError> {
        tracing::info!("Initializing BLE GATT server via bluer...");

        let adapter = session.default_adapter().await?;
        adapter.set_powered(true).await?;
        // The btmgmt fallback renames the adapter, and BlueZ persists that
        // in /var/lib/bluetooth across restarts and reboots. Record the
        // original so shutdown can restore it.
        let original_alias = adapter.alias().await?;

        tracing::info!(
            "Serving standard BLE services on adapter {} with address {}",
            adapter.name(),
            adapter.address().await?
        );

        let (advertised_tx, advertised_rx) = watch::channel(None);

        tracing::info!("Registering GATT application...");
        let app = build_application(&stats_rx, &advertised_rx);
        let app_handle = adapter.serve_gatt_application(app).await?;
        tracing::info!("GATT application served successfully.");

        let mut advertiser = BluerAdvertiser {
            adapter: adapter.clone(),
            current: None,
        };
        let result = track_advertised_bike(
            &mut advertiser,
            stats_rx,
            &advertised_tx,
            cancel_token,
            locked_to,
        )
        .await;

        tracing::info!("Shutting down BLE GATT server...");
        drop(app_handle);
        restore_alias(&adapter, &original_alias).await;
        tokio::time::sleep(GATT_UNREGISTER_SETTLE_DELAY).await;

        result
    }

    /// Restores the adapter's alias if the btmgmt fallback changed it.
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
        let mut cmd = tokio::process::Command::new(BTMGMT);
        cmd.args(args);
        let output = cmd.output().await?;
        if output.status.success() {
            Ok(())
        } else {
            // btmgmt reports its failures on stdout, not stderr.
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            Err(format!(
                "btmgmt {} failed ({}): {}{}",
                args.join(" "),
                output.status,
                stdout.trim(),
                stderr.trim()
            )
            .into())
        }
    }
}

#[cfg(target_os = "linux")]
pub use linux_impl::run;

/// BlueZ has no cross-platform equivalent: on other platforms there is no
/// GATT server, and this function waits until cancellation, so the scanner
/// and MQTT halves of the bridge still run on a dev machine.
#[cfg(not(target_os = "linux"))]
pub async fn run(
    cancel_token: tokio_util::sync::CancellationToken,
    _stats_rx: tokio::sync::watch::Receiver<std::sync::Arc<crate::stats::Fleet>>,
    _locked_to: Option<crate::stats::BikeId>,
) -> Result<(), crate::BoxError> {
    tracing::warn!(
        "BLE GATT server broadcasting is only supported on Linux (BlueZ). \
         Broadcasting is disabled on this platform."
    );
    cancel_token.cancelled().await;
    Ok(())
}
