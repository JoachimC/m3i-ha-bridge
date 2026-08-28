# Security

## Threat model

The bridge is designed for a **home LAN that you trust**. It assumes these
conditions:

* You own and administer the host. The host is usually a Raspberry Pi next to
  the bike.
* The MQTT broker is on the same LAN. The connection does not cross the
  internet.
* Everyone in BLE range can see the bike's metrics. The bike itself broadcasts
  them without encryption. The GATT server sends the same data to every
  central that connects. There is no pairing and no bonding.

Know these limits before you deploy the bridge on a network that you trust
less:

* **The service runs as root.** The GATT server registers with BlueZ over
  D-Bus. On some adapters, it runs `/usr/bin/btmgmt` to control advertising.
  Both need root on standard Raspberry Pi OS. A contribution that runs the
  service as a dedicated user, with the correct D-Bus policy, is welcome.
* **MQTT uses plain TCP on port 1883.** The broker password crosses the LAN
  without encryption at each connect. On the Pi, the password is a systemd
  credential (`LoadCredential=`). This protects the password on the SD card
  and from other local processes. It does not replace TLS. The bridge does
  not support TLS at this time.
* The bridge accepts **no inbound network connections**. Its only listener is
  the BLE GATT server. Its only outbound connection is to the broker.
* `src/keiser.rs` parses all data from the radio. The parser is pure Rust,
  checks all bounds, and has unit tests for malformed and truncated packets.

## Report a vulnerability

Do **not** open a public issue for a problem that an attacker can use.

1. Go to the **Security** tab of this repository.
2. Select **Report a vulnerability**. This sends a private report.
3. If you cannot use that, send an email to the maintainer. The address is on
   the maintainer's GitHub profile.

You get an answer within one week.

A problem that is inside the threat model above is a known limit, not a
vulnerability. Example: "the password is visible on the LAN". For those, open
an issue or a pull request.
