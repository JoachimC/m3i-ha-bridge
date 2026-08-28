//! BLE GATT server, Linux only — BlueZ has no cross-platform equivalent.
//!
//! `ble_platform` decides whether this is reachable at all; on other platforms
//! it parks until cancellation instead.

#[cfg(target_os = "linux")]
mod linux_impl {
    use crate::BoxError;
    use crate::gatt_codec::{
        FTMS_FEATURE_VALUE, LEGACY_ADVERTISING_CAPACITY, LOCAL_NAME, cps_has_value, ftms_has_value,
        hrs_has_value, initial_notification, legacy_advertising_size, serialize_cps,
        serialize_ftms, serialize_hrs, wrap_u16,
    };
    use crate::stats::{KeiserStats, current_reading, next_reading};
    use bluer::{
        adv::Advertisement,
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

    /// How often each notify loop re-sends the current stats even when no new
    /// advertisement arrived, so clients see values decay to zero on staleness.
    const NOTIFY_POLL_INTERVAL: Duration = Duration::from_millis(1000);

    /// Service UUIDs listed in the advertising packet.
    ///
    /// FTMS has to be here, not merely discoverable after connecting: many
    /// clients — Zwift's FTMS pairing screen among them — filter discovery on
    /// 0x1826 in the advertising data, so a trainer that omits it is simply
    /// never offered, and the pairing flow never gets far enough to read the
    /// service list. FTMS v1.0 §3.1 requires it for that reason.
    ///
    /// Heart Rate (0x180D) is deliberately left out even though it would fit.
    /// The bridge only reports a heart rate when the rider's strap is paired to
    /// the bike, so advertising it would offer HR-pairing screens a sensor that
    /// usually has nothing to say — and advertising registration on this
    /// hardware is fragile enough (see the retry and btmgmt fallback below)
    /// that each addition deserves its own verification.
    fn advertised_service_uuids() -> BTreeSet<bluer::Uuid> {
        BTreeSet::from([CPS_SERVICE_UUID, FTMS_SERVICE_UUID])
    }

    /// btmgmt fallback for the same advertisement, used when every D-Bus
    /// registration attempt fails. Kept beside [`advertised_service_uuids`]
    /// because the two must list the same services; a test asserts they do.
    const BTMGMT_ADD_ADV_ARGS: [&str; 9] =
        ["add-adv", "-u", "1818", "-u", "1826", "-c", "-g", "-n", "1"];

    /// Serves a subscriber of one notify characteristic: sends the current
    /// stats immediately (when `has_value` says they are worth sending), then
    /// re-serializes and notifies on every stats change or poll tick until the
    /// client disconnects or the stats sender is dropped.
    ///
    /// Every payload it sends, the first one included, is built from sanitized
    /// stats — see [`initial_notification`].
    fn spawn_notify_loop<F>(
        name: &'static str,
        mut rx: watch::Receiver<KeiserStats>,
        mut notifier: CharacteristicNotifier,
        has_value: fn(&KeiserStats) -> bool,
        mut serialize: F,
    ) where
        F: FnMut(&KeiserStats) -> Vec<u8> + Send + 'static,
    {
        tokio::spawn(async move {
            tracing::info!("GATT: Client subscribed to {}", name);

            let initial = current_reading(&mut rx);
            if let Some(payload) = initial_notification(&initial, has_value, &mut serialize) {
                let _ = notifier.notify(payload).await;
            }

            while let Some(stats) = next_reading(&mut rx, NOTIFY_POLL_INTERVAL).await {
                let stats = stats.sanitized();
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
                    let serialize = make_serializer();
                    async move {
                        spawn_notify_loop(name, rx, notifier, has_value, serialize);
                    }
                    .boxed()
                })),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn build_application(stats_rx: &watch::Receiver<KeiserStats>) -> Application {
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
                            hrs_has_value,
                            || Box::new(serialize_hrs),
                        ),
                        // Body Sensor Location: 1 = Chest
                        read_characteristic(HRS_BODY_SENSOR_LOCATION_CHAR_UUID, &[0x01]),
                    ],
                    ..Default::default()
                },
            ],
            ..Default::default()
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

        tracing::info!(
            "Advertising standard BLE services on adapter {} with address {}",
            adapter.name(),
            adapter.address().await?
        );

        let service_uuids = advertised_service_uuids();

        // BlueZ rejects an oversized advertisement with a generic D-Bus error,
        // which the retry loop below then reports five times without ever
        // saying what is wrong. Say it up front instead.
        let payload_size = legacy_advertising_size(LOCAL_NAME, service_uuids.len());
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
            local_name: Some(LOCAL_NAME.to_string()),
            min_interval: Some(std::time::Duration::from_millis(300)),
            max_interval: Some(std::time::Duration::from_millis(500)),
            ..Default::default()
        };

        let mut adv_handle = None;
        let mut retries = 5;
        let mut try_without_intervals = false;
        let mut using_btmgmt = false;

        while retries > 0 {
            if try_without_intervals {
                le_advertisement.min_interval = None;
                le_advertisement.max_interval = None;
            }

            match adapter.advertise(le_advertisement.clone()).await {
                Ok(handle) => {
                    adv_handle = Some(handle);
                    break;
                }
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
                    } else {
                        tracing::warn!(
                            "All D-Bus advertising attempts failed. Falling back to manual btmgmt legacy advertising..."
                        );

                        // Try clearing any existing instance 1 first
                        let _ = run_btmgmt(&["rm-adv", "1"]).await;

                        // Register using btmgmt. The -u list must mirror
                        // advertised_service_uuids(), or the fallback path
                        // silently drops FTMS and the trainer stops appearing
                        // in FTMS pairing screens exactly when D-Bus
                        // registration is already misbehaving.
                        match run_btmgmt(&BTMGMT_ADD_ADV_ARGS).await {
                            Ok(_) => {
                                tracing::info!(
                                    "Successfully registered legacy advertisement via btmgmt fallback!"
                                );
                                using_btmgmt = true;
                            }
                            Err(bt_err) => {
                                tracing::error!("btmgmt fallback also failed: {}", bt_err);
                                return Err(e.into());
                            }
                        }
                    }
                }
            }
        }

        tracing::info!("Registering GATT application...");
        let app = build_application(&stats_rx);

        let app_handle = adapter.serve_gatt_application(app).await?;
        tracing::info!("GATT application served successfully. BLE broadcasting active!");

        // Wait for cancellation
        cancel_token.cancelled().await;

        tracing::info!("Shutting down BLE GATT server and advertising...");
        if using_btmgmt {
            if let Err(e) = run_btmgmt(&["rm-adv", "1"]).await {
                tracing::error!(
                    "Failed to remove btmgmt advertisement during cleanup: {}",
                    e
                );
            } else {
                tracing::info!("Successfully removed legacy advertisement via btmgmt.");
            }
        }
        drop(app_handle);
        drop(adv_handle);
        tokio::time::sleep(Duration::from_secs(1)).await;

        Ok(())
    }

    async fn run_btmgmt(args: &[&str]) -> Result<(), BoxError> {
        let mut cmd = tokio::process::Command::new("/usr/bin/btmgmt");
        cmd.args(args);
        let output = cmd.output().await?;
        if output.status.success() {
            Ok(())
        } else {
            let err = String::from_utf8_lossy(&output.stderr).into_owned();
            Err(format!("btmgmt failed: {}", err).into())
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
        fn given_the_advertisement_when_built_then_it_lists_fitness_machine_and_cycling_power() {
            // 0x1826 is the point of issue #8: clients that filter discovery on
            // the FTMS UUID never see a trainer that omits it.
            let advertised = advertised_service_uuids();
            assert!(
                advertised.contains(&FTMS_SERVICE_UUID),
                "Fitness Machine (0x1826)"
            );
            assert!(
                advertised.contains(&CPS_SERVICE_UUID),
                "Cycling Power (0x1818)"
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
            let size = legacy_advertising_size(LOCAL_NAME, advertised_service_uuids().len());
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
