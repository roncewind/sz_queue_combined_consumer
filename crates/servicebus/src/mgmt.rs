//! Service Bus request/response operations on an entity's management node
//! (`<queue>/$management`), over `fe2o3-amqp-management`.
//!
//! Wire format (Microsoft "AMQP 1.0 request/response operations in Azure
//! Service Bus", *Message Renew Lock*; azure-sdk-for-python
//! `ServiceBusReceiver._renew_locks` / `_mgmt_request_response`):
//!
//! * application-properties: `operation` = `com.microsoft:renew-lock`,
//!   `com.microsoft:server-timeout` (uint, ms), `associated-link-name` (the
//!   receiver link the locks were taken on);
//! * body: amqp-value map `{"lock-tokens": array of uuid}`;
//! * response: `statusCode` 200, body amqp-value map
//!   `{"expirations": array of timestamp}`; a lost lock is `statusCode` 410.

use fe2o3_amqp::types::messaging::{ApplicationProperties, Message};
use fe2o3_amqp::types::primitives::{Array, OrderedMap, SimpleValue, Timestamp, Uuid, Value};
use fe2o3_amqp_management::error::Error as MgmtError;
use fe2o3_amqp_management::request::Request;
use fe2o3_amqp_management::response::Response;

/// `operation` of a renew-lock request.
pub const RENEW_LOCK_OPERATION: &str = "com.microsoft:renew-lock";
/// Application property: server-side operation timeout (uint, ms).
pub const SERVER_TIMEOUT: &str = "com.microsoft:server-timeout";
/// Application property: the receiver link whose locks are renewed.
pub const ASSOCIATED_LINK_NAME: &str = "associated-link-name";
/// Body key: the lock tokens.
pub const LOCK_TOKENS: &str = "lock-tokens";
/// Response body key: the new lock expirations.
pub const EXPIRATIONS: &str = "expirations";
/// `statusCode` of a renew on a lock that has expired or was settled
/// (`com.microsoft:message-lock-lost`, HTTP 410 Gone).
pub const STATUS_LOCK_LOST: u16 = 410;

/// `com.microsoft:renew-lock` for the given lock tokens (RFC 4122 bytes).
#[derive(Debug, Clone)]
pub struct RenewLockRequest {
    pub lock_tokens: Vec<[u8; 16]>,
    pub associated_link_name: String,
    pub server_timeout_ms: u32,
}

impl Request for RenewLockRequest {
    const OPERATION: &'static str = RENEW_LOCK_OPERATION;
    type Response = RenewLockResponse;
    type Body = Value;

    fn encode_application_properties(&mut self) -> Option<ApplicationProperties> {
        Some(
            ApplicationProperties::builder()
                .insert(SERVER_TIMEOUT, SimpleValue::Uint(self.server_timeout_ms))
                .insert(
                    ASSOCIATED_LINK_NAME,
                    SimpleValue::String(self.associated_link_name.clone()),
                )
                .build(),
        )
    }

    fn encode_body(self) -> Value {
        let tokens = self
            .lock_tokens
            .into_iter()
            .map(|t| Value::Uuid(Uuid::from(t)))
            .collect();
        let mut map = OrderedMap::new();
        map.insert(
            Value::String(LOCK_TOKENS.to_string()),
            Value::Array(Array(tokens)),
        );
        Value::Map(map)
    }
}

/// Response to [`RenewLockRequest`]: the new expiry of each token, in request
/// order (empty if the broker sent none we could read).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenewLockResponse {
    pub expirations: Vec<Timestamp>,
}

impl Response for RenewLockResponse {
    const STATUS_CODE: u16 = 200;
    type Body = Value;
    type Error = MgmtError;

    fn decode_message(message: Message<Value>) -> Result<Self, Self::Error> {
        Ok(Self {
            expirations: expirations(&message.body),
        })
    }
}

/// The `expirations` timestamps of a renew-lock response body (array or
/// list; non-timestamps skipped).
fn expirations(body: &Value) -> Vec<Timestamp> {
    let Value::Map(map) = body else {
        return Vec::new();
    };
    let items = map
        .iter()
        .find(|(k, _)| match k {
            Value::String(s) => s == EXPIRATIONS,
            Value::Symbol(s) => s.as_str() == EXPIRATIONS,
            _ => false,
        })
        .map(|(_, v)| v);
    let values: &[Value] = match items {
        Some(Value::Array(a)) => &a.0,
        Some(Value::List(l)) => l,
        _ => return Vec::new(),
    };
    values
        .iter()
        .filter_map(|v| match v {
            Value::Timestamp(t) => Some(t.clone()),
            _ => None,
        })
        .collect()
}

/// `true` when a renew failed because the lock is gone (expired, settled or
/// the message was redelivered): retrying cannot help.
pub fn is_lock_lost(err: &MgmtError) -> bool {
    match err {
        MgmtError::Status(s) => {
            let code = s.code.0.get();
            let names_the_lock = s
                .description
                .as_deref()
                .is_some_and(|d| d.to_ascii_lowercase().contains("lock"));
            code == STATUS_LOCK_LOST || (code / 100 == 4 && names_the_lock)
        }
        _ => false,
    }
}

/// `true` when the error is a management-node answer (a status code): the
/// request/response links are fine. Anything else (send/receive/disposition
/// failure, an un-accepted request, a decode error) means the management
/// client is unusable and must be re-attached.
pub fn is_status_error(err: &MgmtError) -> bool {
    matches!(err, MgmtError::Status(_))
}

#[cfg(test)]
mod tests {
    use super::*;
    use fe2o3_amqp_management::error::StatusError;
    use fe2o3_amqp_management::status::StatusCode;

    fn request() -> RenewLockRequest {
        RenewLockRequest {
            lock_tokens: vec![[3, 2, 1, 0, 5, 4, 7, 6, 8, 9, 10, 11, 12, 13, 14, 15]],
            associated_link_name: "sz-link".into(),
            server_timeout_ms: 30_000,
        }
    }

    #[test]
    fn renew_lock_message_has_the_documented_shape() {
        let msg = request().into_message();
        let props = msg.application_properties.expect("app properties");
        assert_eq!(
            props.0.get("operation"),
            Some(&SimpleValue::String(RENEW_LOCK_OPERATION.into()))
        );
        assert_eq!(
            props.0.get(SERVER_TIMEOUT),
            Some(&SimpleValue::Uint(30_000))
        );
        assert_eq!(
            props.0.get(ASSOCIATED_LINK_NAME),
            Some(&SimpleValue::String("sz-link".into()))
        );
        // Body: amqp-value map {"lock-tokens": array of uuid}.
        let Value::Map(map) = msg.body else {
            panic!("body is not an amqp-value map");
        };
        let tokens = map
            .get(&Value::String(LOCK_TOKENS.into()))
            .expect("lock-tokens");
        let Value::Array(Array(items)) = tokens else {
            panic!("lock-tokens is not an array: {tokens:?}");
        };
        assert_eq!(
            items,
            &vec![Value::Uuid(Uuid::from([
                3, 2, 1, 0, 5, 4, 7, 6, 8, 9, 10, 11, 12, 13, 14, 15
            ]))]
        );
    }

    /// On the wire the token list is an AMQP array (0xe0/0xf0) whose single
    /// element constructor is uuid (0x98), followed by the 16 RFC 4122 bytes.
    #[test]
    fn lock_tokens_encode_as_an_array_of_uuid() {
        let bytes = serde_amqp::to_vec(&request().encode_body()).expect("encode");
        let key = b"lock-tokens";
        let at = bytes
            .windows(key.len())
            .position(|w| w == key)
            .expect("key encoded")
            + key.len();
        let rest = &bytes[at..];
        // array8: 0xe0 size count ctor; array32: 0xf0 size(4) count(4) ctor.
        let (ctor, payload) = match rest[0] {
            0xe0 => (rest[3], &rest[4..]),
            0xf0 => (rest[9], &rest[10..]),
            other => panic!("not an array encoding: {other:#04x}"),
        };
        assert_eq!(ctor, 0x98, "element constructor must be uuid");
        assert_eq!(
            &payload[..16],
            &[3, 2, 1, 0, 5, 4, 7, 6, 8, 9, 10, 11, 12, 13, 14, 15]
        );
    }

    #[test]
    fn response_expirations_are_read_from_array_or_list() {
        let ts = |ms| Value::Timestamp(Timestamp::from_milliseconds(ms));
        for list in [
            Value::Array(Array(vec![ts(1), ts(2)])),
            Value::List(vec![ts(1), ts(2)]),
        ] {
            let mut map = OrderedMap::new();
            map.insert(Value::String(EXPIRATIONS.into()), list);
            assert_eq!(
                expirations(&Value::Map(map)),
                vec![
                    Timestamp::from_milliseconds(1),
                    Timestamp::from_milliseconds(2)
                ]
            );
        }
        assert!(expirations(&Value::Null).is_empty());
    }

    fn status(code: u16, description: Option<&str>) -> MgmtError {
        MgmtError::Status(StatusError {
            code: StatusCode(std::num::NonZeroU16::new(code).expect("non-zero")),
            description: description.map(str::to_string),
        })
    }

    #[test]
    fn lock_lost_is_410_or_a_4xx_naming_the_lock() {
        assert!(is_lock_lost(&status(410, None)));
        assert!(is_lock_lost(&status(
            400,
            Some("The lock supplied is invalid. Either the lock expired...")
        )));
        assert!(!is_lock_lost(&status(500, Some("lock service hiccup"))));
        assert!(!is_lock_lost(&status(503, None)));
        assert!(is_status_error(&status(503, None)));
        assert!(!is_status_error(&MgmtError::StatusCodeNotFound));
    }
}
