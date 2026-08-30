#!/bin/bash
# Exit immediately if any command fails
set -e

# deploy.sh puts the binary in the invoking user's home directory by default.
# Override with INSTALL_DIR=/opt/m3i-ha-bridge ./install-service.sh
INSTALL_DIR="${INSTALL_DIR:-$HOME}"
BINARY="$INSTALL_DIR/m3i-ha-bridge-static"
SERVICE_FILE="/etc/systemd/system/m3i-ha-bridge.service"
ENV_FILE="/etc/default/m3i-ha-bridge"
CREDSTORE_DIR="/etc/credstore"
PASSWORD_CRED="$CREDSTORE_DIR/mqtt-password"

if [ ! -x "$BINARY" ]; then
  echo "Binary not found or not executable: $BINARY" >&2
  echo "Run ./deploy.sh first, or set INSTALL_DIR to where the binary lives." >&2
  exit 1
fi

echo "=== 1. Creating environment file (if missing) ==="
if [ ! -f "$ENV_FILE" ]; then
sudo tee "$ENV_FILE" > /dev/null <<EOF
# Keiser M3i HA Bridge configuration
#
# Unset: accept any Keiser bike in range (one Home Assistant device per bike,
# the BLE name follows the bike being ridden). Set to the ordinal id shown on
# the bike's console to dedicate this bridge to that one bike: only it is
# published, and the bridge advertises as it from startup.
#KEISER_BIKE_ID=0
#
# Log level. RUST_LOG=info,bike_stats=trace additionally logs every parsed
# bike reading while keeping the rest of the bridge quiet.
#RUST_LOG=info
#
# The broker password does NOT belong in this file. Environment variables are
# readable through /proc/<pid>/environ and are inherited by every child process,
# and this service execs btmgmt. Store it as a systemd credential instead:
#   sudo install -d -m 700 -o root -g root $CREDSTORE_DIR
#   sudo sh -c 'umask 077; cat > $PASSWORD_CRED'   # type the password, then Ctrl-D
#   sudo systemctl restart m3i-ha-bridge
#
# MQTT publishing is disabled unless MQTT_HOST is set.
#MQTT_HOST=homeassistant.local
#MQTT_PORT=1883
#MQTT_USERNAME=mqtt-user
#MQTT_CLIENT_ID=m3i-ha-bridge
#MQTT_TOPIC_PREFIX=m3i
#MQTT_DISCOVERY_PREFIX=homeassistant
EOF
echo "Created $ENV_FILE (edit it to enable MQTT publishing)"
else
echo "$ENV_FILE already exists, leaving it untouched"
fi

# Unconditional, and outside the branch above: an existing env file can be
# mode 0644 and can still hold a password.
sudo chown root:root "$ENV_FILE"
sudo chmod 600 "$ENV_FILE"

echo "=== 2. Preparing the credential store ==="
# systemd >= 253 ships a tmpfiles rule that creates /etc/credstore 0700 root:root.
# This step also makes the permissions correct on Bookworm (systemd 252).
sudo install -d -m 700 -o root -g root "$CREDSTORE_DIR"
if [ -f "$PASSWORD_CRED" ]; then
  sudo chown root:root "$PASSWORD_CRED"
  sudo chmod 600 "$PASSWORD_CRED"
  echo "Using MQTT password credential $PASSWORD_CRED"
else
  echo "No $PASSWORD_CRED yet - the bridge will connect without a password."
  echo "To set one:  sudo sh -c 'umask 077; cat > $PASSWORD_CRED'   # type it, then Ctrl-D"
fi

# LoadCredential= dates to systemd 247. The bare form below needs >= 251:
# systemd looks the name up in /etc/credstore/ and does not fail when the file
# is absent. The absolute-path form is fatal instead: a missing file fails the
# unit with exit 243, and Restart=always then loops. That matters because this
# script writes the unit with or without a configured password.
SYSTEMD_VERSION="$(systemctl --version | awk 'NR==1 {print $2}')"
SYSTEMD_VERSION="${SYSTEMD_VERSION%%[!0-9]*}"
if [ "${SYSTEMD_VERSION:-0}" -ge 251 ]; then
  LOAD_CREDENTIAL="LoadCredential=mqtt-password"
elif [ -f "$PASSWORD_CRED" ]; then
  LOAD_CREDENTIAL="LoadCredential=mqtt-password:$PASSWORD_CRED"
else
  LOAD_CREDENTIAL="# LoadCredential=mqtt-password:$PASSWORD_CRED  # uncomment once the file exists"
fi

echo "=== 3. Creating systemd service file ==="
sudo tee "$SERVICE_FILE" > /dev/null <<EOF
[Unit]
Description=Keiser M3i Home Assistant Bluetooth Bridge
After=bluetooth.target
Requires=bluetooth.target

[Service]
Type=simple
WorkingDirectory=$INSTALL_DIR
ExecStart=$BINARY
Environment=RUST_LOG=info
EnvironmentFile=-$ENV_FILE
# The MQTT password is passed as a credential rather than an environment
# variable: systemd copies $PASSWORD_CRED into a private, non-swappable,
# root-only directory exported as \$CREDENTIALS_DIRECTORY, so it never appears
# in /proc/<pid>/environ and is not inherited by btmgmt.
$LOAD_CREDENTIAL
# SIGTERM goes to the bridge only. The default (control-group) also signals
# the btmgmt the bridge runs during its own shutdown to remove the
# advertisement, killing it about half the time and leaving the advertising
# instance registered. Anything still running after the bridge exits is
# SIGKILLed as before.
KillMode=mixed
Restart=always
RestartSec=5
User=root
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

echo "=== 4. Reloading systemd daemon ==="
sudo systemctl daemon-reload

echo "=== 5. Enabling m3i-ha-bridge service ==="
sudo systemctl enable m3i-ha-bridge

echo "=== 6. Starting m3i-ha-bridge service ==="
sudo systemctl restart m3i-ha-bridge

echo "=== Installation complete! ==="
echo "You can check the status with: sudo systemctl status m3i-ha-bridge"
echo "You can follow logs with: sudo journalctl -u m3i-ha-bridge -f"
