//! Shared Access Signature (SAS) tokens for the CBS `put-token` exchange.
//!
//! Algorithm (azure-sdk-for-python `azure/servicebus/_pyamqp/utils.py`
//! `generate_sas_token`, and Microsoft "Service Bus access control with Shared
//! Access Signatures"):
//!
//! ```text
//! encoded_uri = quote_plus(audience)
//! string_to_sign = encoded_uri + "\n" + expiry          (expiry: unix seconds)
//! sig = base64(HMAC-SHA256(key = the SharedAccessKey string's UTF-8 bytes,
//!                          msg = string_to_sign))
//! token = "SharedAccessSignature sr=<quote_plus(audience)>&sig=<quote_plus(sig)>
//!          &se=<expiry>&skn=<quote_plus(key name)>"
//! ```
//!
//! The key is used as-is (its UTF-8 bytes), NOT base64-decoded. The token is a
//! secret: it is never logged.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use hmac::{Hmac, Mac};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use sha2::Sha256;

/// CBS token type of a Service Bus SAS token (`put-token` `type`).
pub const SAS_TOKEN_TYPE: &str = "servicebus.windows.net:sastoken";

/// Python `urllib.parse.quote_plus` with its default `safe=''`: everything but
/// ASCII alphanumerics and `_.-~` is percent-encoded (upper-case hex); a space
/// becomes `+`.
const QUOTE_PLUS: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'_')
    .remove(b'.')
    .remove(b'-')
    .remove(b'~');

/// `quote_plus` (see [`QUOTE_PLUS`]).
pub fn quote_plus(s: &str) -> String {
    s.split(' ')
        .map(|part| utf8_percent_encode(part, QUOTE_PLUS).to_string())
        .collect::<Vec<_>>()
        .join("+")
}

/// A SAS token for `audience`, valid until `expiry` (unix seconds).
pub fn generate_sas_token(audience: &str, key_name: &str, key: &str, expiry: u64) -> String {
    let encoded_uri = quote_plus(audience);
    let string_to_sign = format!("{encoded_uri}\n{expiry}");
    // HMAC accepts a key of any length; new_from_slice cannot fail for HMAC.
    let mut mac = Hmac::<Sha256>::new_from_slice(key.as_bytes()).expect("HMAC takes any key size");
    mac.update(string_to_sign.as_bytes());
    let signature = BASE64.encode(mac.finalize().into_bytes());
    format!(
        "SharedAccessSignature sr={encoded_uri}&sig={}&se={expiry}&skn={}",
        quote_plus(&signature),
        quote_plus(key_name)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_plus_matches_python() {
        // Python: quote_plus("sb://ns.servicebus.windows.net/my queue/x~y_z.-")
        assert_eq!(
            quote_plus("sb://ns.servicebus.windows.net/my queue/x~y_z.-"),
            "sb%3A%2F%2Fns.servicebus.windows.net%2Fmy+queue%2Fx~y_z.-"
        );
        assert_eq!(quote_plus("a+b/c=="), "a%2Bb%2Fc%3D%3D");
    }

    /// Known vector computed with the azure-sdk-for-python 7.15.0
    /// `_pyamqp.utils.generate_sas_token` algorithm (copied verbatim into a
    /// standalone script: quote_plus / HMAC-SHA256 / b64encode / urlencode).
    #[test]
    fn sas_token_matches_the_azure_sdk_vector() {
        let token = generate_sas_token(
            "sb://contoso.servicebus.windows.net/senzing-input",
            "RootManageSharedAccessKey",
            "c2VjcmV0LWtleS1mb3ItdGVzdHMtb25seQ==",
            1_800_000_000,
        );
        assert_eq!(token, SDK_VECTOR);
    }

    const SDK_VECTOR: &str = "SharedAccessSignature \
        sr=sb%3A%2F%2Fcontoso.servicebus.windows.net%2Fsenzing-input\
        &sig=cv9%2F%2BEvUt8i7VJRx26Wpz8ownz5FP9GpxgfwGOYDejY%3D\
        &se=1800000000&skn=RootManageSharedAccessKey";
}
