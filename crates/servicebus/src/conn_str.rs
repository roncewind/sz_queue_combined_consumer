//! Service Bus connection string parsing and the validated broker parameters.
//!
//! Format (Azure portal "Shared access policies"):
//! `Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=<rule>;SharedAccessKey=<key>[;EntityPath=<queue>]`,
//! plus `UseDevelopmentEmulator=true` for the local Service Bus emulator.
//! Parsing follows azure-sdk-for-python `_base_handler._parse_conn_str`:
//! `;`-separated `key=value` pairs split on the FIRST `=` (keys are base64 and
//! end in `=`), keys case-insensitive, a trailing `;` allowed, `Endpoint`'s
//! host[:port] taken from the URL. A pre-signed `SharedAccessSignature=` is
//! rejected (it cannot be refreshed for a multi-hour run).

use std::fmt;
use std::time::Duration;

/// AMQP over TLS (Service Bus requires TLS; TLS first, then AMQP — port 5671).
pub const AMQPS_PORT: u16 = 5671;
/// Plain AMQP: the development emulator only (`UseDevelopmentEmulator=true`).
pub const AMQP_PORT: u16 = 5672;
/// SAS token lifetime (azure-sdk-for-python default: 1 h); re-put at half.
pub const DEFAULT_TOKEN_TTL: Duration = Duration::from_secs(3600);

/// How the driver authenticates its CBS `put-token`. Only SAS keys today; a
/// managed-identity (Entra JWT, token type `jwt`) variant slots in here.
#[derive(Clone, PartialEq, Eq)]
pub enum Auth {
    /// Shared access policy (rule) name + key: tokens are signed locally.
    SasKey { key_name: String, key: String },
}

impl fmt::Debug for Auth {
    /// Never prints the key.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SasKey { key_name, .. } => {
                write!(f, "SasKey {{ key_name: {key_name:?}, key: *** }}")
            }
        }
    }
}

/// Validated Service Bus parameters, handed to the transport.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceBusParams {
    /// Namespace host, e.g. `contoso.servicebus.windows.net`.
    pub host: String,
    /// Explicit port from the endpoint, if any.
    pub port: Option<u16>,
    /// `false` only for the development emulator.
    pub tls: bool,
    pub auth: Auth,
    /// Queue name (the AMQP node address).
    pub queue: String,
    /// Lifetime of each SAS token put on `$cbs` (re-put at half of it).
    pub token_ttl: Duration,
}

impl ServiceBusParams {
    /// `amqps://host:5671` (or `amqp://host:5672` for the emulator). Safe to
    /// log: carries no credentials.
    pub fn url(&self) -> String {
        let (scheme, default_port) = if self.tls {
            ("amqps", AMQPS_PORT)
        } else {
            ("amqp", AMQP_PORT)
        };
        format!(
            "{scheme}://{}:{}",
            self.host,
            self.port.unwrap_or(default_port)
        )
    }

    /// The fully qualified namespace as the Azure SDK uses it in the token
    /// audience: the host, plus the port only for the non-TLS emulator.
    fn namespace(&self) -> String {
        match (self.tls, self.port) {
            (false, Some(port)) => format!("{}:{port}", self.host),
            _ => self.host.clone(),
        }
    }

    /// CBS `put-token` name / SAS audience: `sb://<namespace>/<queue>`
    /// (azure-sdk-for-python `ReceiverMixin._auth_uri`).
    pub fn audience(&self) -> String {
        format!("sb://{}/{}", self.namespace(), self.queue)
    }
}

/// Parses `conn_str` and resolves the queue: `queue` (flag/env) or the
/// connection string's `EntityPath`; both set and different is an error (SDK
/// parity).
pub fn parse_connection_string(
    conn_str: &str,
    queue: Option<&str>,
) -> Result<ServiceBusParams, String> {
    const MALFORMED: &str = "Service Bus connection string is blank or malformed \
                             (expected Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=...)";
    let mut endpoint = None;
    let mut key_name = None;
    let mut key = None;
    let mut entity_path = None;
    let mut emulator = false;
    let trimmed = conn_str.trim().trim_end_matches(';');
    if trimmed.is_empty() {
        return Err(MALFORMED.to_string());
    }
    for part in trimmed.split(';') {
        let (k, v) = part.split_once('=').ok_or(MALFORMED)?;
        match k.trim().to_ascii_lowercase().as_str() {
            "endpoint" | "hostname" => endpoint = Some(v.trim().trim_end_matches('/')),
            "sharedaccesskeyname" => key_name = Some(v.to_string()),
            "sharedaccesskey" => key = Some(v.to_string()),
            "entitypath" => entity_path = Some(v.to_string()),
            "usedevelopmentemulator" => emulator = v.trim().eq_ignore_ascii_case("true"),
            "sharedaccesssignature" => {
                return Err(
                    "SharedAccessSignature connection strings are not supported (a \
                            pre-signed token cannot be refreshed); use SharedAccessKeyName + \
                            SharedAccessKey"
                        .to_string(),
                );
            }
            _ => {} // unknown keys are ignored, like the SDK
        }
    }
    let endpoint = endpoint.filter(|e| !e.is_empty()).ok_or(MALFORMED)?;
    // Endpoint is a URL (sb://host[:port][/]); a bare host is accepted too.
    let authority = endpoint
        .split_once("://")
        .map_or(endpoint, |(_, rest)| rest)
        .split('/')
        .next()
        .unwrap_or_default();
    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) => (
            h,
            Some(
                p.parse::<u16>()
                    .map_err(|_| format!("invalid port in Service Bus Endpoint: {p:?}"))?,
            ),
        ),
        None => (authority, None),
    };
    if host.is_empty() || authority.contains('@') {
        return Err("invalid Endpoint on the Service Bus connection string".to_string());
    }
    let (key_name, key) = match (
        key_name.filter(|s| !s.is_empty()),
        key.filter(|s| !s.is_empty()),
    ) {
        (Some(n), Some(k)) => (n, k),
        _ => {
            return Err(
                "Service Bus connection string must have both SharedAccessKeyName and \
                 SharedAccessKey"
                    .to_string(),
            );
        }
    };
    let entity_path = entity_path.filter(|s| !s.is_empty());
    let queue = match (queue.filter(|q| !q.is_empty()), entity_path) {
        (Some(q), Some(e)) if q != e => {
            return Err(format!(
                "queue name {q:?} does not match the connection string's EntityPath {e:?}"
            ));
        }
        (Some(q), _) => q.to_string(),
        (None, Some(e)) => e,
        (None, None) => {
            return Err(
                "No queue provided (use --queue or SENZING_AZURE_QUEUE_NAME, or \
                        EntityPath in the connection string); required when redo% < 100"
                    .to_string(),
            );
        }
    };
    Ok(ServiceBusParams {
        host: host.to_string(),
        port,
        tls: !emulator,
        auth: Auth::SasKey { key_name, key },
        queue,
        token_ttl: DEFAULT_TOKEN_TTL,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const CS: &str = "Endpoint=sb://contoso.servicebus.windows.net/;\
                      SharedAccessKeyName=RootManageSharedAccessKey;\
                      SharedAccessKey=abc+def/ghi=";

    #[test]
    fn parses_a_portal_connection_string() {
        let p = parse_connection_string(CS, Some("q1")).unwrap();
        assert_eq!(p.host, "contoso.servicebus.windows.net");
        assert_eq!((p.port, p.tls), (None, true));
        assert_eq!(
            p.auth,
            Auth::SasKey {
                key_name: "RootManageSharedAccessKey".into(),
                key: "abc+def/ghi=".into(),
            },
            "the key keeps its trailing '=' (split on the first '=' only)"
        );
        assert_eq!(p.url(), "amqps://contoso.servicebus.windows.net:5671");
        assert_eq!(p.audience(), "sb://contoso.servicebus.windows.net/q1");
    }

    #[test]
    fn keys_are_case_insensitive_and_trailing_semicolon_is_ok() {
        let cs = "endpoint=sb://h.example/;sharedaccesskeyname=n;SHAREDACCESSKEY=k;";
        let p = parse_connection_string(cs, Some("q")).unwrap();
        assert_eq!((p.host.as_str(), p.queue.as_str()), ("h.example", "q"));
    }

    #[test]
    fn entity_path_supplies_or_must_match_the_queue() {
        let cs = format!("{CS};EntityPath=input");
        assert_eq!(parse_connection_string(&cs, None).unwrap().queue, "input");
        assert_eq!(
            parse_connection_string(&cs, Some("input")).unwrap().queue,
            "input"
        );
        let err = parse_connection_string(&cs, Some("other")).unwrap_err();
        assert!(err.contains("does not match"), "{err}");
        let err = parse_connection_string(CS, None).unwrap_err();
        assert!(err.contains("No queue"), "{err}");
    }

    #[test]
    fn emulator_is_plain_amqp_and_keeps_its_port_in_the_audience() {
        let cs = "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;\
                  SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";
        let p = parse_connection_string(cs, Some("queue.1")).unwrap();
        assert!(!p.tls);
        assert_eq!(p.url(), "amqp://localhost:5672");
        assert_eq!(p.audience(), "sb://localhost/queue.1");
        let cs = cs.replace("sb://localhost", "sb://localhost:5673");
        let p = parse_connection_string(&cs, Some("queue.1")).unwrap();
        assert_eq!(p.url(), "amqp://localhost:5673");
        assert_eq!(p.audience(), "sb://localhost:5673/queue.1");
    }

    #[test]
    fn rejects_malformed_and_incomplete_strings() {
        for (cs, needle) in [
            ("", "malformed"),
            ("garbage", "malformed"),
            ("SharedAccessKeyName=n;SharedAccessKey=k", "malformed"),
            ("Endpoint=sb://h/;SharedAccessKeyName=n", "both"),
            ("Endpoint=sb://h/;SharedAccessKey=k", "both"),
            (
                "Endpoint=sb://h:x/;SharedAccessKeyName=n;SharedAccessKey=k",
                "port",
            ),
            (
                "Endpoint=sb://h/;SharedAccessSignature=SharedAccessSignature sr=x",
                "not supported",
            ),
        ] {
            let err = parse_connection_string(cs, Some("q")).unwrap_err();
            assert!(err.contains(needle), "{cs:?}: {err}");
        }
    }

    #[test]
    fn debug_never_prints_the_key() {
        let p = parse_connection_string(CS, Some("q")).unwrap();
        let shown = format!("{p:?}");
        assert!(!shown.contains("abc+def"), "{shown}");
        assert!(shown.contains("RootManageSharedAccessKey"));
    }
}
