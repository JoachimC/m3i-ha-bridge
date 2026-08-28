# Keiser M3i HA Bridge

[![CI](https://github.com/JoachimC/m3i-ha-bridge/actions/workflows/ci.yml/badge.svg)](https://github.com/JoachimC/m3i-ha-bridge/actions/workflows/ci.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
[![Release](https://img.shields.io/github/v/release/JoachimC/m3i-ha-bridge?sort=date)](https://github.com/JoachimC/m3i-ha-bridge/releases/latest)
[![Open Source Maintenance Fee](https://img.shields.io/badge/Open%20Source-Maintenance%20Fee-blue)](#open-source-maintenance-fee)

A Bluetooth Low Energy (BLE) bridge that scans for Keiser M3i advertising data and republishes it to two independent destinations:

1. **BLE GATT server** — standard Cycling Power (CPS), Fitness Machine (FTMS) and Heart Rate services, so devices/applications (like Zwift, Garmin, etc.) can connect to the M3i. The bridge advertises as `Keiser M3i #042` — the bike's id from its console — so a pairing list tells bikes apart. Cycling Power (`0x1818`) and Fitness Machine (`0x1826`) are both named in the advertising packet, because pairing screens generally filter discovery on the advertised UUID rather than connecting first to find out. Heart Rate is discoverable after connecting but not advertised — it only carries data when the rider's strap is paired to the bike.
2. **MQTT** — a JSON state stream with Home Assistant MQTT discovery, so the bike shows up automatically as a device with sensors in Home Assistant.

The M3i broadcasts its metrics as non-connectable BLE advertisements in a Keiser-specific format that fitness apps do not understand. The bridge sits next to the bike on a Raspberry Pi, decodes those packets and re-exposes them in the standard profiles. It runs continuously as a systemd service and needs no interaction once installed.

> **Not affiliated with Keiser.** This is an independent project. Keiser and M3i are trademarks of Keiser Corporation. The advertising format was worked out from Keiser's publicly published [parser](https://github.com/KeiserCorp/Keiser.MSeries.BLE-Parser), [simulator](https://github.com/KeiserCorp/Keiser.M3i.BLE-HCI-Simulator) and [M Series Direct documentation](https://dev.keiser.com/mseries/direct/); see `doc/bluetooth-protocol.md`.

## Hardware requirements

| | Requirement |
|---|---|
| Bike | Keiser M3i with firmware **6.21 or newer**, set to **metric** units. Older firmware (no gear byte) and imperial mode are detected and ignored, not mis-parsed. |
| Host | Any Linux box with BlueZ and a BLE adapter. Developed and run on a **Raspberry Pi Zero W** (ARMv6); the published binaries cover ARMv6, ARM64 and x86-64, all statically linked so any recent distro works. |
| Placement | Within a few metres of the bike. The Pi's radio must be LE-only for full packet rate — see [Raspberry Pi Bluetooth Configuration](#raspberry-pi-bluetooth-configuration). |
| Privileges | The service runs as root: the GATT server registers with BlueZ over D-Bus and falls back to `btmgmt` for advertising. See `SECURITY.md`. |
| Optional | An MQTT broker (e.g. the Home Assistant Mosquitto add-on) for the Home Assistant side. The GATT side needs nothing else. |

macOS and Windows can build and run the scanner and MQTT publisher for development, but the GATT server is Linux/BlueZ only.

> **Hardware-verification status:** the parser, codecs and MQTT layer are unit-tested on every push. BlueZ advertising, scanning behaviour and the systemd unit can only be checked on a real Pi, and not every change is re-verified there before release. If something behaves differently on your hardware, please open an issue with `sudo btmon` output.

## Quickstart

Do these steps on the Pi, or on a different Linux host that has BlueZ.

1. Select the architecture of your host. Set `ARCH` to one of these values:

   | Host | `ARCH` |
   |---|---|
   | Pi Zero, Pi 1, Pi 2 (32-bit OS) | `arm-unknown-linux-musleabihf` |
   | Pi 3, Pi 4, Pi 5 (64-bit OS) | `aarch64-unknown-linux-musl` |
   | PC, NUC or VM | `x86_64-unknown-linux-musl` |

2. Download the latest release and its checksum. Then make sure that the checksum is correct:
   ```bash
   ARCH=arm-unknown-linux-musleabihf
   TAG=$(curl -fsSL https://api.github.com/repos/JoachimC/m3i-ha-bridge/releases/latest | grep -o '"tag_name": *"[^"]*"' | cut -d'"' -f4)
   curl -fsSLO "https://github.com/JoachimC/m3i-ha-bridge/releases/download/$TAG/m3i-ha-bridge-$TAG-$ARCH"
   curl -fsSLO "https://github.com/JoachimC/m3i-ha-bridge/releases/download/$TAG/m3i-ha-bridge-$TAG-$ARCH.sha256"
   sha256sum -c "m3i-ha-bridge-$TAG-$ARCH.sha256"
   ```
   The last command must show `OK`. If it does not, stop. Download the files again.

3. Move the binary to your home directory. Make it executable:
   ```bash
   mv "m3i-ha-bridge-$TAG-$ARCH" ~/m3i-ha-bridge-static
   chmod +x ~/m3i-ha-bridge-static
   ```

4. Start the bridge in the foreground:
   ```bash
   sudo RUST_LOG=info ~/m3i-ha-bridge-static
   ```
   Pedal the bike. The log shows a reading approximately every 2 seconds. Press `Ctrl+C` to stop the bridge.

5. Install the bridge as a service:
   ```bash
   curl -fsSLO https://raw.githubusercontent.com/JoachimC/m3i-ha-bridge/main/install-service.sh
   chmod +x install-service.sh
   ./install-service.sh
   ```
   The script makes the file `/etc/default/m3i-ha-bridge`. The service starts automatically.

6. Optional. Connect the bridge to Home Assistant:
   1. Set `MQTT_HOST` in `/etc/default/m3i-ha-bridge`. See [MQTT Configuration](#mqtt-configuration).
   2. Set the broker password. See [Broker password](#broker-password).
   3. Restart the service: `sudo systemctl restart m3i-ha-bridge`.

   Home Assistant finds the sensors automatically.

7. Optional. Connect a fitness app. In Zwift or Garmin, select the device named "Keiser M3i".

If you develop on a different machine, use `./deploy.sh`. It builds the binary and copies it to the Pi. See [Deployment](#deployment).

## Architecture

The Bluetooth reader is the single producer: it parses Keiser advertisements into `KeiserStats` (`stats.rs`) and broadcasts them on a `tokio::sync::watch` channel. Each publisher — the BLE GATT server (`gatt_server`) and the MQTT publisher (`mqtt_publisher`) — consumes its own receiver independently, so reading from the bike is fully decoupled from publishing, and a slow or failing destination never blocks the reader or the other destination.

### Module layout

| Module | Responsibility |
|---|---|
| `stats.rs` | The `KeiserStats` domain model and staleness/sanitization rules |
| `keiser.rs` | Pure parser for the M3i advertising protocol (see `doc/bluetooth-protocol.md`) |
| `bluetooth_hal.rs` | Platform-independent scanning: the `BleScanner` trait, advertisement filtering, feeding the watch channel |
| `scan_bluer.rs` | BLE scanning via `bluer` — Linux only |
| `scan_btleplug.rs` | BLE scanning via `btleplug` — everywhere else |
| `ble_platform.rs` | The only module that knows which Bluetooth stack this build uses |
| `gatt_codec.rs` | Pure serializers for the FTMS / Cycling Power / Heart Rate GATT payloads |
| `gatt_server.rs` | BlueZ (`bluer`) GATT server and advertising — Linux only |
| `mqtt_publisher.rs` | MQTT state publishing and Home Assistant discovery |
| `between_retries_strategy.rs` | Pluggable, cancellable retry backoff for the bridge loop |

Protocol parsing (`keiser`) and payload serialization (`gatt_codec`) are deliberately free of Bluetooth-stack dependencies, so they build and unit-test on any platform even though the GATT server itself only runs on Linux.

## Development

Requires Rust 1.88 or newer (edition 2024 plus `let` chains; declared as `rust-version` in `Cargo.toml` and enforced by the `msrv` CI job). On macOS/Windows the bridge builds and runs, but the GATT server is disabled (BlueZ only); scanning and MQTT work everywhere `btleplug` does.

```bash
cargo test                 # unit tests (platform independent, incl. protocol/codec tests)
cargo clippy --all-targets # lints — the codebase is kept clippy-clean
cargo fmt --check          # formatting — CI rejects unformatted code
cargo run                  # run locally; set RUST_LOG=debug/trace for more detail
```

`RUST_LOG` accepts a default level and per-target overrides, e.g. `RUST_LOG=info,bike_stats=trace` to keep the bridge quiet while logging every parsed bike reading.

Test vectors for the protocol parser come from real `btmon` captures in `doc/sample-data.md`.

Because the GATT server is `cfg(target_os = "linux")`, it never compiles on a Mac. Two things cover that gap:

- **Devcontainer** (`.devcontainer/`): open the repo in a Linux container to compile, test and lint the bluer path locally, with rust-analyzer seeing the Linux cfg. Note the container has no Bluetooth hardware — it's for building and testing, not running against a real adapter; that still needs the Pi.
- **CI** (`.github/workflows/ci.yml`): every push runs `cargo clippy --all-targets -- -D warnings` and `cargo test` on a Linux runner.

## Bike Selection

By default the bridge accepts every Keiser M3i it hears. All M-Series bikes advertise under the same company id and packet format, so in a room with more than one bike they all feed the same channel and the last advertisement received wins.

| Variable | Default | Description |
|---|---|---|
| `KEISER_BIKE_ID` | *(unset — accept any bike)* | Ordinal id (0–200) configured on the bike, as shown in its console. Advertisements from any other bike are ignored. Note `0` is a valid id, so setting it does filter. |

### Bike identity

Every bike the bridge hears is identified by that ordinal id, zero-padded to three digits in names and topics (`#042`, `042`). The id appears on both outputs:

* **BLE** — the advertised name is `Keiser M3i #042`, and the Device Information Service (`0x180A`) reports `042` as the Serial Number String. Nothing is advertised until the first bike is heard. With no filter and several bikes in range, the advertisement follows the bike heard most recently, but only switches after a different id has been the latest for 10 s, so two bikes alternating packets do not make the name flap. A connected client receives only the advertised bike's readings; the others are never mixed in. One adapter is one peripheral, so a studio that wants Zwift pairing per bike needs one bridge per bike, each with `KEISER_BIKE_ID` set — the Home Assistant side works either way.
* **MQTT** — one Home Assistant device per bike, named `Keiser M3i #042`, with its own topics (see [MQTT Configuration](#mqtt-configuration)) and a diagnostic `Bike ID` sensor carrying the plain integer (`42`). A room with several bikes gets a device per bike.

## MQTT Configuration

MQTT publishing is **disabled by default** and enabled by setting `MQTT_HOST`. Settings come from environment variables (the systemd service reads them from `/etc/default/m3i-ha-bridge`, mode `600`); the password is resolved separately, see [Broker password](#broker-password):

| Variable | Default | Description |
|---|---|---|
| `MQTT_HOST` | *(unset — MQTT disabled)* | Broker hostname or IP |
| `MQTT_PORT` | `1883` | Broker port (plain TCP) |
| `MQTT_USERNAME` | *(none)* | Username, if the broker requires auth |
| `MQTT_PASSWORD` | *(none)* | Password, for dev and local runs. Avoid it on the Pi — see below |
| `MQTT_PASSWORD_FILE` | *(none)* | Path to a file holding the password, used when `MQTT_PASSWORD` is unset (the Docker `*_FILE` secret convention). Trailing newlines are stripped |
| `MQTT_CLIENT_ID` | `m3i-ha-bridge` | MQTT client id |
| `MQTT_TOPIC_PREFIX` | `m3i` | Prefix for state/availability topics |
| `MQTT_DISCOVERY_PREFIX` | `homeassistant` | Home Assistant discovery prefix |

### Broker password

Resolved in order: `MQTT_PASSWORD`, then the file named by `MQTT_PASSWORD_FILE`, then the systemd credential `$CREDENTIALS_DIRECTORY/mqtt-password`. An empty value at any step counts as unset, and a trailing newline in a file is stripped.

On the Pi, use the systemd credential. `install-service.sh` adds `LoadCredential=mqtt-password` to the unit, so systemd reads the file as root and exposes a 0400 copy in a private, non-swappable directory that only this service can read. It never enters the environment — which matters because environment variables are readable through `/proc/<pid>/environ` and are inherited by every child process, and the bridge execs `btmgmt`.

To set the password:

1. Make the credential store:
   ```bash
   sudo install -d -m 700 -o root -g root /etc/credstore
   ```
2. Write the password to the credential file:
   ```bash
   sudo sh -c 'umask 077; cat > /etc/credstore/mqtt-password'
   ```
   Type the password. Then press `Ctrl+D`.
3. Restart the service:
   ```bash
   sudo systemctl restart m3i-ha-bridge
   ```
4. Make sure that the service can read the credential:
   ```bash
   sudo ls -l /run/credentials/m3i-ha-bridge.service
   ```
   The output must show the file `mqtt-password`.

This procedure needs systemd 251 or later. Raspberry Pi OS Bookworm has systemd 252. To see your version, run `systemctl --version`. On an older systemd, the installer uses a different unit setting automatically. If the credential file does not exist, the bridge connects without a password. The bridge does not fail to start.

The broker connection is plain TCP on 1883, so the password still crosses the LAN in the clear on every connect. `LoadCredential` protects it at rest on the SD card and from other local processes, which is the right level for a home broker; it is not a substitute for TLS.

Topics published, per bike heard (`<id>` is the zero-padded bike id, e.g. `042`):

* `<prefix>/<id>/state` — JSON payload with `power`, `cadence`, `heart_rate`, `gear`, `distance`, `energy`, `elapsed_seconds`, `is_paused`, `bike_id`, published whenever the data changes. Live metrics are zeroed when the bike pauses or data goes stale.
* `<prefix>/<id>/availability` — `online` while that bike's readings are fresh, `offline` (retained) once they go stale, so a bike that is switched off greys out in Home Assistant on its own.
* `<prefix>/availability` — whether the *bridge* is running: `online`/`offline` (retained, with MQTT Last Will for ungraceful disconnects). Every entity requires both this and its bike's topic to say `online`.
* `homeassistant/(binary_)sensor/m3i-ha-bridge-<id>/<entity>/config` — retained Home Assistant discovery configs, published the first time a bike is heard and again on every reconnect. No YAML needed on the HA side; entities appear under a "Keiser M3i #042" device.

Every sensor declares a `state_class`, so Home Assistant keeps long-term statistics for all of them:

| Entity | Unit | Device class | State class |
|---|---|---|---|
| Power | `W` | `power` | `measurement` |
| Cadence | `rpm` | — | `measurement` |
| Heart Rate | `bpm` | — | `measurement` |
| Gear | — | — | `measurement` |
| Distance | `km` | `distance` | `total_increasing` |
| Energy | `kcal` | `energy` | `total_increasing` |
| Elapsed Time | `s` | `duration` | `measurement` |
| Bike ID | — | — | — *(diagnostic)* |
| Paused | — | *(binary sensor)* | — |

Entities are named by the device: each config announces a short name ("Power") plus the device block, and Home Assistant composes them into the friendly name "Keiser M3i #042 Power" and the entity id `sensor.keiser_m3i_042_power` — so there is no bare `sensor.power` to collide with anything else on the instance, and two bikes never collide with each other. This is automatic; the MQTT integration sets `has_entity_name` on every entity itself, and it is not a discovery option you can pass.

#### Upgrading from a release before per-bike devices

Older releases published one device, "Keiser M3i", under the node id `m3i` (the topic prefix) with state on `m3i/state`. Those retained discovery configs are not removed automatically, so the old device stays in Home Assistant as *unavailable* next to the new `Keiser M3i #042` one. To remove it, do these steps once:

1. In Home Assistant, open **Settings → Devices & services → MQTT**, select the old **Keiser M3i** device, and delete it.
2. Clear the retained configs on the broker, so the device does not come back on the next Home Assistant restart. With the Mosquitto add-on or `mosquitto_pub`, publish an empty retained message to each old topic:
   ```bash
   for e in power cadence heart_rate gear distance energy elapsed_time; do
     mosquitto_pub -h <broker> -u <user> -P <password> -r -n -t "homeassistant/sensor/m3i/$e/config"
   done
   mosquitto_pub -h <broker> -u <user> -P <password> -r -n -t homeassistant/binary_sensor/m3i/paused/config
   ```
   If you changed `MQTT_TOPIC_PREFIX` or `MQTT_DISCOVERY_PREFIX`, use those values instead of `m3i` and `homeassistant`.

History recorded against the old entity ids is not carried over.

`total_increasing` is the right class for distance and energy because both accumulate through a ride and reset to zero on the next one. The `kcal` unit on an `energy` device class requires **Home Assistant 2024.10 or newer**; on anything older that entity is rejected at discovery. If you need to support an older release, drop `device_class` from the energy sensor in `src/mqtt_publisher.rs` — long-term statistics come from the state class alone.

## Deployment

Use `./deploy.sh` to send a build from your development machine to the Pi. The script does these steps:

1. It builds the static release binary for the Pi (`arm-unknown-linux-musleabihf`). The build needs [`cross`](https://github.com/cross-rs/cross) and Docker.
2. It stops the bridge on the Pi, if the bridge runs.
3. It copies the binary to the Pi with `scp`. The file goes to the home directory of the ssh user, as `m3i-ha-bridge-static`.
4. It starts the binary in the foreground, so that you can see the log.

The default ssh target is `pi@m3i-bridge.local`. To use a different user or host, set `PI`:

```bash
PI=pi@bike.local ./deploy.sh
```

The examples that follow use `$PI` for the same value.

### Published builds

Every green push to `main` publishes a GitHub Release containing static binaries for `arm-unknown-linux-musleabihf` (Pi Zero/1/2), `aarch64-unknown-linux-musl` (64-bit Pi OS) and `x86_64-unknown-linux-musl`, each with a SHA-256 checksum, tagged `YYYY-MM-DD-<shortsha>` (e.g. `2026-08-16-a1b2c3d`). The date sorts chronologically and the SHA says exactly which commit it is, with no version bookkeeping to keep in step.

That version is compiled into the binary, so the Pi can say what it is running:

```bash
sudo journalctl -u m3i-ha-bridge | head -1
# Keiser M3i HA Bridge 2026-08-16-a1b2c3d starting...
```

A locally built binary reports `local-<git describe>` instead, so a hand-built deploy is never mistaken for a published one. A plain `cargo build` reports `dev`.

To deploy a published build, and not the code in your working tree, use `--release`. This also gives you rollback: deploy an earlier tag.

* To deploy the newest published build:
  ```bash
  ./deploy.sh --release
  ```
* To deploy a specific build:
  ```bash
  ./deploy.sh --release 2026-08-16-a1b2c3d
  ```

The script downloads the build with the `gh` CLI. It makes sure that the checksum is correct before it copies the binary.

---

## Raspberry Pi Bluetooth Configuration

The adapter must be LE-only, or BlueZ interleaves LE scanning with classic-Bluetooth inquiry and the radio misses ~50% of the bike's advertisements in ~8-second deaf periods.

The bridge now requests `DiscoveryTransport::Le` itself (`scan_bluer.rs`), which achieves the same thing per-client, so this setting should be redundant. Keep it anyway until that has been confirmed on the hardware: `main.conf` is a global guarantee, whereas BlueZ merges the discovery filters of *all* D-Bus clients, so another client asking for `auto` would reinstate interleaved discovery for everyone.

To set the adapter to LE-only, do these steps once on the Pi:

1. Open `/etc/bluetooth/main.conf` with `sudo`.
2. In the `[General]` section, add this line:
   ```ini
   [General]
   ControllerMode = le
   ```
3. Save the file.
4. Restart the Bluetooth service:
   ```bash
   sudo systemctl restart bluetooth
   ```

Measured effect (90 s riding windows, bike advertises every 1.94 s): mean update gap improved from ~3.9 s (worst 7.8 s) to ~2.1 s (worst 4.9 s). The bridge itself is lossless — it logs every advertisement the radio receives (verify with `sudo btmon`).

---

## Configuring as a systemd Service

A systemd service starts the bridge when the Pi boots. It also restarts the bridge if the bridge stops. The script `install-service.sh` makes the service.

### Installation Steps

Do these steps on your development machine.

1. Deploy the latest binary to the Pi:
   ```bash
   ./deploy.sh
   ```
   The bridge starts in the foreground. When you see readings in the log, press `Ctrl+C`.

2. Copy the installation script to the Pi:
   ```bash
   scp install-service.sh "$PI:install-service.sh"
   ```

3. Run the script on the Pi:
   ```bash
   ssh "$PI" "chmod +x ./install-service.sh && ./install-service.sh"
   ```

The service uses the file `m3i-ha-bridge-static` in the home directory of the ssh user. This is where `deploy.sh` put it. If the binary is in a different directory, set `INSTALL_DIR` when you run the script.

---

## Managing the Service

Use the standard systemd commands on the Pi. Each example runs the command through ssh from your development machine.

| To do this | Run this command |
|---|---|
| Show the service status | `ssh "$PI" "sudo systemctl status m3i-ha-bridge"` |
| Show the live log | `ssh "$PI" "sudo journalctl -u m3i-ha-bridge -f"` |
| Stop the service | `ssh "$PI" "sudo systemctl stop m3i-ha-bridge"` |
| Start the service | `ssh "$PI" "sudo systemctl start m3i-ha-bridge"` |
| Restart the service | `ssh "$PI" "sudo systemctl restart m3i-ha-bridge"` |
| Stop the service from starting at boot | `ssh "$PI" "sudo systemctl disable m3i-ha-bridge"` |
| Start the service at boot again | `ssh "$PI" "sudo systemctl enable m3i-ha-bridge"` |

---

## Contributing

Issues and pull requests are welcome — see `CONTRIBUTING.md` for the workflow (one branch and PR per issue, `given_…_when_…_then_…` test names, `cargo fmt`/`clippy` gates) and `SECURITY.md` for the threat model and how to report a vulnerability.

## Open Source Maintenance Fee

This project participates in the [Open Source Maintenance Fee](https://opensourcemaintenancefee.org). The source code is freely available under the terms of the [License](#license) below. To support sustainable maintenance, use of the project's official releases in revenue-generating activities requires adherence to the [Open Source Maintenance Fee](./OSMFEULA.txt).

In practice: if you ride at home, you owe nothing. If you use this project's official releases as part of revenue-generating activities — a gym, studio, hotel or any other operation whose paying customers use bikes fed by this bridge — and your annual gross revenue is US$10,000 or more, the Maintenance Fee applies. It is a small monthly sponsorship, not a licence fee: the code stays MIT OR Apache-2.0, and you remain free to build from source yourself.

To pay the Maintenance Fee, [become a Sponsor](https://github.com/sponsors/JoachimC).

## License

Licensed under either of

* Apache License, Version 2.0 (`LICENSE-APACHE` or <http://www.apache.org/licenses/LICENSE-2.0>)
* MIT license (`LICENSE-MIT` or <http://opensource.org/licenses/MIT>)

at your option. Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.

The Linux release binaries statically link the D-Bus reference library, used under the Academic Free License v2.1 — see `THIRD-PARTY-NOTICES.md`.
