//! Broker connection settings, read from the environment.

use std::io;
use std::path::Path;

use super::topics::Topics;

/// Credential name declared by `LoadCredential=mqtt-password` in the systemd
/// unit. systemd copies the file into a private ramfs and points
/// `$CREDENTIALS_DIRECTORY` at it, so the password never enters the environment.
const PASSWORD_CREDENTIAL: &str = "mqtt-password";

#[derive(Debug, Clone, PartialEq)]
pub struct MqttConfig {
    pub host: String,
    pub port: u16,
    pub username: Option<String>,
    pub password: Option<String>,
    pub client_id: String,
    pub(crate) topics: Topics,
}

impl MqttConfig {
    /// `None` when `MQTT_HOST` is unset, which disables MQTT publishing
    /// entirely.
    pub fn from_env() -> Option<Self> {
        Self::from_lookup(
            |key| std::env::var(key).ok(),
            |path: &Path| std::fs::read_to_string(path),
        )
    }

    /// The environment lookup and the file reads are both injected so tests
    /// stay hermetic: no process environment, no disk.
    pub fn from_lookup(
        lookup: impl Fn(&str) -> Option<String>,
        read_file: impl Fn(&Path) -> io::Result<String>,
    ) -> Option<Self> {
        let host = lookup("MQTT_HOST").filter(|v| !v.is_empty())?;
        let port = lookup("MQTT_PORT")
            .and_then(|v| v.parse().ok())
            .unwrap_or(1883);
        Some(MqttConfig {
            host,
            port,
            // An empty value and an unset one are deliberately equivalent for
            // every optional credential: commenting a line out of
            // /etc/default/m3i-ha-bridge and blanking its value must behave
            // the same way.
            username: lookup("MQTT_USERNAME").filter(|v| !v.is_empty()),
            password: resolve_password(&lookup, &read_file),
            client_id: lookup("MQTT_CLIENT_ID").unwrap_or_else(|| "m3i-ha-bridge".to_string()),
            topics: Topics {
                prefix: lookup("MQTT_TOPIC_PREFIX").unwrap_or_else(|| "m3i".to_string()),
                discovery_prefix: lookup("MQTT_DISCOVERY_PREFIX")
                    .unwrap_or_else(|| "homeassistant".to_string()),
            },
        })
    }
}

/// Resolves the broker password, in order of precedence:
///
/// 1. `MQTT_PASSWORD` — a plain environment variable, for dev and local runs;
/// 2. the file named by `MQTT_PASSWORD_FILE` — the Docker `*_FILE` secret
///    convention, which also works outside systemd;
/// 3. `$CREDENTIALS_DIRECTORY/mqtt-password` — the systemd credential loaded by
///    `LoadCredential=mqtt-password` (see `install-service.sh`).
///
/// The credential is the one the deployment uses, because an environment
/// variable is the wrong place for a secret on this box: it is readable through
/// `/proc/<pid>/environ` and inherited by every child process, and the bridge
/// execs `btmgmt`. systemd instead copies the credential into a private,
/// unswappable directory only this unit can read.
///
/// An empty value at any step counts as unset, exactly as for `MQTT_USERNAME`.
fn resolve_password(
    lookup: &impl Fn(&str) -> Option<String>,
    read_file: &impl Fn(&Path) -> io::Result<String>,
) -> Option<String> {
    if let Some(password) = lookup("MQTT_PASSWORD").filter(|v| !v.is_empty()) {
        return Some(password);
    }

    if let Some(path) = lookup("MQTT_PASSWORD_FILE").filter(|v| !v.is_empty()) {
        return read_password(read_file, Path::new(&path), PasswordSource::ConfiguredFile);
    }

    let credentials_dir = lookup("CREDENTIALS_DIRECTORY").filter(|v| !v.is_empty())?;
    let path = Path::new(&credentials_dir).join(PASSWORD_CREDENTIAL);
    read_password(read_file, &path, PasswordSource::SystemdCredential)
}

/// Where a password file came from, which decides how loudly its absence is
/// reported.
#[derive(Debug, Clone, Copy)]
enum PasswordSource {
    /// Named explicitly by `MQTT_PASSWORD_FILE`: a missing file is a
    /// misconfiguration worth a warning.
    ConfiguredFile,
    /// systemd exports `CREDENTIALS_DIRECTORY` whenever the unit declares any
    /// credential and skips a missing one silently, so absence is normal.
    SystemdCredential,
}

fn read_password(
    read_file: &impl Fn(&Path) -> io::Result<String>,
    path: &Path,
    source: PasswordSource,
) -> Option<String> {
    match read_file(path) {
        Ok(contents) => {
            // systemd copies credential files through byte for byte, adding and
            // stripping nothing, so a trailing newline left by `echo` or an
            // editor would otherwise become part of the password. Strip it the
            // way Docker's own `file_env` helper does. Interior whitespace is
            // preserved — it may well be part of the password.
            let password = contents.trim_end_matches(['\r', '\n']);
            if password.is_empty() {
                tracing::warn!("MQTT password file {} is empty; ignoring", path.display());
                return None;
            }
            Some(password.to_string())
        }
        Err(e)
            if e.kind() == io::ErrorKind::NotFound
                && matches!(source, PasswordSource::SystemdCredential) =>
        {
            tracing::debug!("No MQTT password credential at {}", path.display());
            None
        }
        Err(e) => {
            tracing::warn!(
                "Failed to read MQTT password from {}: {}",
                path.display(),
                e
            );
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn lookup_from<'a>(map: &'a HashMap<&'a str, &'a str>) -> impl Fn(&str) -> Option<String> + 'a {
        move |key| map.get(key).map(|v| v.to_string())
    }

    /// In-memory stand-in for the filesystem: a path not in the map reads as
    /// `NotFound`, which is what both the `*_FILE` and credential paths see
    /// when nothing has been configured.
    fn reader_from<'a>(
        files: &'a HashMap<&'a str, &'a str>,
    ) -> impl Fn(&Path) -> io::Result<String> + 'a {
        move |path| {
            files
                .get(path.to_str().unwrap_or_default())
                .map(|v| v.to_string())
                .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))
        }
    }

    fn config_from(vars: &HashMap<&str, &str>, files: &HashMap<&str, &str>) -> Option<MqttConfig> {
        MqttConfig::from_lookup(lookup_from(vars), reader_from(files))
    }

    /// Resolves just the password, with MQTT_HOST supplied so a config exists.
    fn password_from(vars: &HashMap<&str, &str>, files: &HashMap<&str, &str>) -> Option<String> {
        let mut vars = vars.clone();
        vars.insert("MQTT_HOST", "broker.local");
        config_from(&vars, files).unwrap().password
    }

    #[test]
    fn given_no_mqtt_host_when_config_is_read_then_mqtt_is_disabled() {
        let vars = HashMap::new();
        assert_eq!(config_from(&vars, &HashMap::new()), None);
    }

    #[test]
    fn given_only_mqtt_host_when_config_is_read_then_defaults_are_applied() {
        let vars = HashMap::from([("MQTT_HOST", "broker.local")]);
        let config = config_from(&vars, &HashMap::new()).unwrap();
        assert_eq!(config.host, "broker.local");
        assert_eq!(config.port, 1883);
        assert_eq!(config.username, None);
        assert_eq!(config.client_id, "m3i-ha-bridge");
        assert_eq!(config.topics.prefix, "m3i");
        assert_eq!(config.topics.discovery_prefix, "homeassistant");
    }

    #[test]
    fn given_full_configuration_when_config_is_read_then_all_values_are_used() {
        let vars = HashMap::from([
            ("MQTT_HOST", "192.168.1.10"),
            ("MQTT_PORT", "8883"),
            ("MQTT_USERNAME", "ha"),
            ("MQTT_PASSWORD", "secret"),
            ("MQTT_CLIENT_ID", "bike-bridge"),
            ("MQTT_TOPIC_PREFIX", "fitness/m3i"),
            ("MQTT_DISCOVERY_PREFIX", "ha-discovery"),
        ]);
        let config = config_from(&vars, &HashMap::new()).unwrap();
        assert_eq!(config.port, 8883);
        assert_eq!(config.username.as_deref(), Some("ha"));
        assert_eq!(config.password.as_deref(), Some("secret"));
        assert_eq!(config.client_id, "bike-bridge");
        assert_eq!(config.topics.prefix, "fitness/m3i");
        assert_eq!(config.topics.discovery_prefix, "ha-discovery");
    }

    #[test]
    fn given_empty_credentials_when_config_is_read_then_they_are_treated_as_unset() {
        let vars = HashMap::from([
            ("MQTT_HOST", "broker.local"),
            ("MQTT_USERNAME", ""),
            ("MQTT_PASSWORD", ""),
        ]);
        let config = config_from(&vars, &HashMap::new()).unwrap();
        assert_eq!(config.username, None);
        assert_eq!(config.password, None);
    }

    #[test]
    fn given_a_password_file_when_config_is_read_then_the_file_contents_are_used() {
        let vars = HashMap::from([("MQTT_PASSWORD_FILE", "/run/secrets/mqtt")]);
        let files = HashMap::from([("/run/secrets/mqtt", "from-file")]);
        assert_eq!(password_from(&vars, &files).as_deref(), Some("from-file"));
    }

    #[test]
    fn given_a_password_file_with_a_trailing_newline_when_read_then_it_is_stripped() {
        // systemd copies credential files through byte for byte, so an
        // operator's `echo pw > file` would otherwise put a newline in the
        // password and every connection would fail authentication.
        let vars = HashMap::from([("MQTT_PASSWORD_FILE", "/run/secrets/mqtt")]);
        let files = HashMap::from([("/run/secrets/mqtt", "from-file\r\n")]);
        assert_eq!(password_from(&vars, &files).as_deref(), Some("from-file"));
    }

    #[test]
    fn given_a_password_file_with_inner_spaces_when_read_then_they_are_preserved() {
        // Only trailing newlines are stripped: spaces may be part of the
        // password, so trimming them would silently corrupt it.
        let vars = HashMap::from([("MQTT_PASSWORD_FILE", "/run/secrets/mqtt")]);
        let files = HashMap::from([("/run/secrets/mqtt", " a b \n")]);
        assert_eq!(password_from(&vars, &files).as_deref(), Some(" a b "));
    }

    #[test]
    fn given_a_systemd_credential_when_read_then_it_is_used() {
        let vars = HashMap::from([("CREDENTIALS_DIRECTORY", "/run/credentials/m3i.service")]);
        let files = HashMap::from([(
            "/run/credentials/m3i.service/mqtt-password",
            "from-credential\n",
        )]);
        assert_eq!(
            password_from(&vars, &files).as_deref(),
            Some("from-credential")
        );
    }

    #[test]
    fn given_both_a_password_and_a_password_file_when_read_then_the_environment_wins() {
        let vars = HashMap::from([
            ("MQTT_PASSWORD", "from-env"),
            ("MQTT_PASSWORD_FILE", "/run/secrets/mqtt"),
        ]);
        let files = HashMap::from([("/run/secrets/mqtt", "from-file")]);
        assert_eq!(password_from(&vars, &files).as_deref(), Some("from-env"));
    }

    #[test]
    fn given_both_a_password_file_and_a_credential_when_read_then_the_password_file_wins() {
        let vars = HashMap::from([
            ("MQTT_PASSWORD_FILE", "/run/secrets/mqtt"),
            ("CREDENTIALS_DIRECTORY", "/run/credentials/m3i.service"),
        ]);
        let files = HashMap::from([
            ("/run/secrets/mqtt", "from-file"),
            (
                "/run/credentials/m3i.service/mqtt-password",
                "from-credential",
            ),
        ]);
        assert_eq!(password_from(&vars, &files).as_deref(), Some("from-file"));
    }

    #[test]
    fn given_an_empty_password_and_a_password_file_when_read_then_the_file_is_used() {
        // Blanking MQTT_PASSWORD must fall through rather than resolve to an
        // empty password, matching how every other credential treats "".
        let vars = HashMap::from([
            ("MQTT_PASSWORD", ""),
            ("MQTT_PASSWORD_FILE", "/run/secrets/mqtt"),
        ]);
        let files = HashMap::from([("/run/secrets/mqtt", "from-file")]);
        assert_eq!(password_from(&vars, &files).as_deref(), Some("from-file"));
    }

    #[test]
    fn given_a_credentials_directory_without_the_credential_when_read_then_no_password_is_used() {
        // The normal state on a box with no MQTT password configured: systemd
        // exports CREDENTIALS_DIRECTORY because the unit declares a credential,
        // and skips the missing file silently. The bridge must connect without
        // a password rather than fail.
        let vars = HashMap::from([("CREDENTIALS_DIRECTORY", "/run/credentials/m3i.service")]);
        assert_eq!(password_from(&vars, &HashMap::new()), None);
    }

    #[test]
    fn given_an_unreadable_password_file_when_read_then_no_password_is_used() {
        let vars = HashMap::from([("MQTT_PASSWORD_FILE", "/run/secrets/missing")]);
        assert_eq!(password_from(&vars, &HashMap::new()), None);
    }

    #[test]
    fn given_a_blank_password_file_when_read_then_no_password_is_used() {
        let vars = HashMap::from([("MQTT_PASSWORD_FILE", "/run/secrets/mqtt")]);
        let files = HashMap::from([("/run/secrets/mqtt", "\n")]);
        assert_eq!(password_from(&vars, &files), None);
    }

    #[test]
    fn given_no_password_settings_at_all_when_read_then_no_password_is_used() {
        assert_eq!(password_from(&HashMap::new(), &HashMap::new()), None);
    }

    #[test]
    fn given_empty_mqtt_host_when_config_is_read_then_mqtt_is_disabled() {
        let vars = HashMap::from([("MQTT_HOST", "")]);
        assert_eq!(config_from(&vars, &HashMap::new()), None);
    }
}
