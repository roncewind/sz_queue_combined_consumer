//! Peek-lock bookkeeping for lock renewal.
//!
//! Service Bus locks a message when it is delivered and unlocks it (and bumps
//! its DeliveryCount) when the lock expires unsettled — at most `lockDuration`
//! (max 5 min) after delivery. A record the engine is still working on must
//! have its lock renewed (`com.microsoft:renew-lock` on `<queue>/$management`)
//! or a second consumer gets it while the first is still adding it.
//!
//! The lock token of a delivery received over a receiver link is its AMQP
//! delivery-tag (Microsoft "AMQP 1.0 request/response operations": "Lock token
//! here refers to the `delivery-tag` property on the received AMQP message").
//! The tag holds the GUID in .NET byte order (azure-sdk-for-python
//! `ServiceBusReceivedMessage.lock_token`: `uuid.UUID(bytes_le=delivery_tag)`),
//! and the renew request carries it as an AMQP `uuid`, which is RFC 4122 /
//! big-endian bytes (`_pyamqp/_encode.py` `encode_uuid`: `value.bytes`).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

/// Never renew more often than this (also the floor of a schedule derived from
/// a very short lock).
pub const MIN_RENEW_DELAY: Duration = Duration::from_secs(1);

/// The RFC 4122 (AMQP `uuid`) bytes of the lock token carried in a 16-byte
/// delivery tag (.NET `Guid.ToByteArray()` order: the first three fields are
/// little-endian). `None` for a tag that is not 16 bytes.
pub fn lock_token_from_delivery_tag(tag: &[u8]) -> Option<[u8; 16]> {
    let le: [u8; 16] = tag.try_into().ok()?;
    let mut be = le;
    be[0..4].reverse();
    be[4..6].reverse();
    be[6..8].reverse();
    Some(be)
}

/// Hyphenated lower-case form of RFC 4122 bytes (for logs).
pub fn format_uuid(b: &[u8; 16]) -> String {
    let h: String = b.iter().map(|x| format!("{x:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &h[0..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..32]
    )
}

/// How long after a lock was (re)acquired to renew it: the configured
/// threshold, but no later than half of the lock's remaining time when the
/// broker told us when it expires (`x-opt-locked-until` on delivery, the
/// `expirations` of a renew response) — so a queue with a lockDuration
/// shorter than the threshold is still renewed in time. Floor
/// [`MIN_RENEW_DELAY`].
pub fn renew_delay(threshold: Duration, lock_remaining: Option<Duration>) -> Duration {
    let delay = match lock_remaining {
        Some(r) if !r.is_zero() => threshold.min(r / 2),
        _ => threshold,
    };
    delay.max(MIN_RENEW_DELAY)
}

/// The renewer's check cadence. Fixed and short: a due entry is renewed at
/// most this late whatever the queue's lockDuration (a scan is a mutex + a
/// walk over at most `--prefetch` entries).
pub const RENEW_TICK: Duration = Duration::from_secs(1);

/// Delay before retrying a renew that failed for a retryable reason.
pub const RENEW_RETRY: Duration = Duration::from_secs(5);

/// One unsettled, locked delivery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockEntry {
    /// RFC 4122 bytes of the lock token.
    pub token: [u8; 16],
    /// When the next renew is due.
    pub renew_at: Instant,
    /// Renewals so far (for the log).
    pub renewals: u32,
    /// Consecutive failed renew attempts (reset by a successful renew).
    pub failures: u32,
    /// Set once a renew failed for good (lock lost): no further attempts; the
    /// message is redelivered by the broker.
    pub lost: bool,
}

/// Unsettled locked deliveries by the transport's synthetic tag; shared
/// between the transport (insert on receive, remove on settle) and the
/// renewer task.
#[derive(Debug, Clone, Default)]
pub struct LockTable(Arc<Mutex<HashMap<u64, LockEntry>>>);

impl LockTable {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<u64, LockEntry>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn insert(&self, tag: u64, token: [u8; 16], renew_at: Instant) {
        self.lock().insert(
            tag,
            LockEntry {
                token,
                renew_at,
                renewals: 0,
                failures: 0,
                lost: false,
            },
        );
    }

    pub fn remove(&self, tag: u64) -> Option<LockEntry> {
        self.lock().remove(&tag)
    }

    /// `(tag, token)` of every live entry due at `now`, oldest first.
    pub fn due(&self, now: Instant) -> Vec<(u64, [u8; 16])> {
        let map = self.lock();
        let mut due: Vec<(Instant, u64, [u8; 16])> = map
            .iter()
            .filter(|(_, e)| !e.lost && e.renew_at <= now)
            .map(|(tag, e)| (e.renew_at, *tag, e.token))
            .collect();
        due.sort_unstable();
        due.into_iter()
            .map(|(_, tag, token)| (tag, token))
            .collect()
    }

    /// Records a successful renew of `tag` (a no-op if it was settled
    /// meanwhile); returns the renewal count.
    pub fn renewed(&self, tag: u64, next: Instant) -> Option<u32> {
        let mut map = self.lock();
        let e = map.get_mut(&tag)?;
        e.renew_at = next;
        e.renewals += 1;
        e.failures = 0;
        Some(e.renewals)
    }

    /// Marks `tag`'s lock as lost (no more renew attempts).
    pub fn mark_lost(&self, tag: u64) {
        if let Some(e) = self.lock().get_mut(&tag) {
            e.lost = true;
        }
    }

    /// Records a failed (retryable) renew of `tag`, retried at `next`;
    /// returns the consecutive failure count (0 if it was settled meanwhile).
    pub fn failed(&self, tag: u64, next: Instant) -> u32 {
        let mut map = self.lock();
        let Some(e) = map.get_mut(&tag) else {
            return 0;
        };
        e.renew_at = next;
        e.failures += 1;
        e.failures
    }

    /// Postpones `tag`'s next renew to `next` (no failure counted).
    pub fn retry_at(&self, tag: u64, next: Instant) {
        if let Some(e) = self.lock().get_mut(&tag) {
            e.renew_at = next;
        }
    }

    #[cfg(test)]
    pub fn get(&self, tag: u64) -> Option<LockEntry> {
        self.lock().get(&tag).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Vector from Python: `uuid.UUID(bytes_le=bytes(range(16)))` =
    /// 03020100-0504-0706-0809-0a0b0c0d0e0f, `.bytes` = 03 02 01 00 05 04 07
    /// 06 08 09 0a 0b 0c 0d 0e 0f.
    #[test]
    fn delivery_tag_is_a_dotnet_order_guid() {
        let tag: Vec<u8> = (0u8..16).collect();
        let be = lock_token_from_delivery_tag(&tag).expect("16 bytes");
        assert_eq!(be, [3, 2, 1, 0, 5, 4, 7, 6, 8, 9, 10, 11, 12, 13, 14, 15]);
        assert_eq!(format_uuid(&be), "03020100-0504-0706-0809-0a0b0c0d0e0f");
        assert!(lock_token_from_delivery_tag(&[1, 2, 3]).is_none());
        assert!(lock_token_from_delivery_tag(&[0; 17]).is_none());
    }

    #[test]
    fn renew_delay_uses_the_threshold_or_half_the_remaining_lock() {
        let t = Duration::from_secs(120);
        assert_eq!(renew_delay(t, None), t);
        assert_eq!(renew_delay(t, Some(Duration::from_secs(300))), t);
        // A 1-minute lock is renewed at 30 s, not after it expired.
        assert_eq!(
            renew_delay(t, Some(Duration::from_secs(60))),
            Duration::from_secs(30)
        );
        // Unknown / already-expired remaining time falls back to the threshold.
        assert_eq!(renew_delay(t, Some(Duration::ZERO)), t);
        // Floor.
        assert_eq!(
            renew_delay(t, Some(Duration::from_millis(500))),
            MIN_RENEW_DELAY
        );
        assert_eq!(renew_delay(Duration::ZERO, None), MIN_RENEW_DELAY);
    }

    #[test]
    fn table_reports_due_entries_oldest_first_and_skips_lost_ones() {
        let t = LockTable::default();
        let now = Instant::now();
        t.insert(1, [1; 16], now + Duration::from_secs(10));
        t.insert(2, [2; 16], now);
        t.insert(3, [3; 16], now - Duration::from_secs(1));
        t.insert(4, [4; 16], now - Duration::from_secs(5));
        t.mark_lost(4);
        assert_eq!(t.due(now), vec![(3, [3; 16]), (2, [2; 16])]);

        assert_eq!(t.renewed(3, now + Duration::from_secs(60)), Some(1));
        assert_eq!(t.due(now), vec![(2, [2; 16])]);
        assert_eq!(t.get(3).unwrap().renewals, 1);

        t.retry_at(2, now + Duration::from_secs(1));
        assert!(t.due(now).is_empty());
        assert_eq!(t.failed(2, now), 1);
        assert_eq!(t.failed(2, now), 2);
        assert_eq!(t.renewed(2, now + Duration::from_secs(9)), Some(1));
        assert_eq!(t.get(2).unwrap().failures, 0, "a renew resets the count");

        // Settled meanwhile: renew bookkeeping is a no-op.
        assert!(t.remove(2).is_some());
        assert_eq!(t.renewed(2, now), None);
        assert_eq!(t.failed(2, now), 0);
        t.mark_lost(2);
        t.retry_at(2, now);
        assert!(t.get(2).is_none());
        assert!(t.get(4).is_some_and(|e| e.lost));
    }
}
