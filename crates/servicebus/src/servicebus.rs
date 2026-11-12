//! Azure Service Bus [`Transport`] (AMQP 1.0, `fe2o3-amqp`) for the shared
//! core queue loop ([`queue_loop::run`]), which owns the engine worker pool,
//! the redo fetcher (redo% > 0), stats, the long-record monitor and the
//! bounded-grace shutdown. Only connect/auth, ingestion, settle and lock
//! renewal are Service Bus-specific; receive, credit and settle mirror the
//! ActiveMQ transport:
//!
//! | Concern      | ActiveMQ Artemis                   | Service Bus (here)                                  |
//! |--------------|------------------------------------|-----------------------------------------------------|
//! | auth         | SASL PLAIN                         | SASL ANONYMOUS + CBS `put-token` (SAS), refreshed    |
//! | ingest       | receiver link, `queue` capability  | receiver link on the queue, peek-lock               |
//! | backpressure | link credit = prefetch - unsettled | same                                                |
//! | ack success  | `accepted`                         | `accepted` (= complete)                             |
//! | dead-letter  | `rejected` -> dead-letter address  | `rejected` + `com.microsoft:dead-letter` -> `$DeadLetterQueue` |
//! | long record  | nothing (no ack timeout)           | renew-lock on `<queue>/$management`                 |
//! | release      | `released` (count unchanged)       | `released` (count unchanged on the emulator; see below) |
//!
//! ## Connection and auth (Microsoft "AMQP 1.0 in Azure Service Bus and
//! Event Hubs protocol guide")
//! TLS first on 5671 (`amqps`, alternative TLS establishment: no AMQP TLS
//! header), then SASL ANONYMOUS, then a CBS `put-token` on `$cbs` (type
//! `servicebus.windows.net:sastoken`, name = audience `sb://<ns>/<queue>`)
//! within 20 s of the connect. The token (1 h) is re-put at half its life on
//! the same connection; if it ever lapses Service Bus detaches the links,
//! which is a fatal receive error (orderly shutdown, exit 255). The
//! development emulator (`UseDevelopmentEmulator=true`) is plain AMQP on 5672.
//!
//! ## Peek-lock
//! The receiver link uses `snd-settle-mode = unsettled`, `rcv-settle-mode =
//! second` (azure-sdk-for-python PEEK_LOCK). Each delivery is locked for the
//! queue's lockDuration; the control task renews the lock of every delivery
//! still unsettled after `SENZING_SB_LOCK_RENEW_SECS` (or half its remaining
//! lock, whichever is sooner) and again on the same schedule. A lost lock
//! (renew answered 410, or a settle after expiry) is logged, never fatal: the
//! broker redelivers the message and `add_record` is idempotent.
//!
//! ## Receive errors
//! As ActiveMQ: per-message decode errors are dead-lettered as malformed;
//! every other receive error (connection, session or link gone; protocol
//! violation) is FATAL. Unsettled deliveries are unlocked by the broker when
//! the connection drops (or at lock expiry) and redelivered.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use fe2o3_amqp::connection::ConnectionHandle;
use fe2o3_amqp::link::RecvError;
use fe2o3_amqp::link::delivery::{Delivery as AmqpDelivery, DeliveryInfo};
use fe2o3_amqp::link::receiver::CreditMode;
use fe2o3_amqp::sasl_profile::SaslProfile;
use fe2o3_amqp::session::SessionHandle;
use fe2o3_amqp::types::definitions::{
    Error as AmqpErrorBody, ErrorCondition, Fields, ReceiverSettleMode, SenderSettleMode,
};
use fe2o3_amqp::types::messaging::annotations::OwnedKey;
use fe2o3_amqp::types::messaging::{Body, Message, Source};
use fe2o3_amqp::types::primitives::{Symbol, Timestamp, Value};
use fe2o3_amqp::{Connection, Receiver, Session};
use fe2o3_amqp_cbs::client::CbsClient;
use fe2o3_amqp_cbs::token::CbsToken;
use fe2o3_amqp_management::client::MgmtClient;
use sz_activemq_amqp::body_bytes;
use sz_rust_sdk::prelude::*;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use sz_combined_consumer_core::config::Config;
use sz_combined_consumer_core::queue_loop::{self, DeadLetterReason, Delivery, Policy, Transport};
use sz_combined_consumer_core::queue_run::RunOutcome;

use crate::INSTANCE_NAME;
use crate::conn_str::{Auth, ServiceBusParams};
use crate::lock::{
    LockTable, RENEW_RETRY, RENEW_TICK, format_uuid, lock_token_from_delivery_tag, renew_delay,
};
use crate::mgmt::{RenewLockRequest, is_lock_lost, is_status_error};
use crate::sas::{SAS_TOKEN_TYPE, generate_sas_token};

/// Lock renewal never dead-letters, so long records are only logged (the
/// renewer keeps the lock; SQS / ActiveMQ parity).
const SERVICEBUS_POLICY: Policy = Policy {
    dead_letter_long_records: false,
    stuck_records_label: "records",
};

/// `rejected` error condition that makes Service Bus dead-letter the message
/// (azure-sdk-for-python `DEADLETTERNAME` = `com.microsoft:dead-letter`).
pub const DEAD_LETTER_CONDITION: &str = "com.microsoft:dead-letter";
/// `rejected` error info key -> the DLQ message's `DeadLetterReason`.
pub const DEAD_LETTER_REASON: &str = "DeadLetterReason";
/// `rejected` error info key -> the DLQ message's `DeadLetterErrorDescription`.
pub const DEAD_LETTER_ERROR_DESCRIPTION: &str = "DeadLetterErrorDescription";
/// Cap on the dead-letter description, in bytes.
const DEAD_LETTER_DESCRIPTION_MAX_BYTES: usize = 1024;
/// Delivery message annotation: when the lock expires (AMQP timestamp).
const LOCKED_UNTIL: &str = "x-opt-locked-until";

/// Local idle timeout (a silent broker fails the connection, then the
/// receive: fatal). Service Bus heartbeats at half the advertised timeout.
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
/// Retry cadence for a failed token refresh (the old token is still valid
/// for at least half its life when the first refresh is attempted).
const TOKEN_RETRY: Duration = Duration::from_secs(30);
/// Bound on one management request/response (renew-lock, put-token) and the
/// server-side timeout it asks for.
const MGMT_TIMEOUT: Duration = Duration::from_secs(30);
/// Bound on the orderly link/session/connection close at shutdown.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);
/// Bound on one connect attempt (TCP + TLS + SASL + Open + CBS + attach).
const CONNECT_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(30);
/// Pause between connect attempts while the namespace is unreachable.
const CONNECT_RETRY_DELAY: Duration = Duration::from_secs(5);
/// A renew-lock that fails (not "lock lost") this many times in a row is given
/// up on (the lock has expired by then anyway).
const MAX_RENEW_FAILURES: u32 = 5;

/// Runs the Service Bus driver until SIGINT/SIGTERM/SIGHUP, a fatal engine
/// error or the loss of the broker link.
pub async fn run(
    config: &Config,
    params: ServiceBusParams,
    env: Arc<SzEnvironmentCore>,
    connect_timeout: Option<Duration>,
    lock_renew: Duration,
) -> Result<RunOutcome> {
    let threads = config.threads;
    let redo_pref = config.redo_pref_workers();
    info!(
        "Service Bus threads: {threads} (load-preferring: {}, redo-preferring: {redo_pref}, \
         redo%: {}, namespace: {}, queue: {}, prefetch: {}, lock renew after: {}s)",
        threads - redo_pref,
        config.redo_percent,
        params.url(),
        params.queue,
        config.prefetch,
        lock_renew.as_secs()
    );
    let cap = u32::try_from(config.prefetch).unwrap_or(u32::MAX);
    // The core loop spawns the engine pool BEFORE awaiting this connect.
    let connect = ServiceBusTransport::connect(params, cap, connect_timeout, lock_renew);
    queue_loop::run(config, env, connect, SERVICEBUS_POLICY).await
}

/// One AMQP connection + session + receiver link on the queue, plus the
/// control task (token refresh + lock renewal) on the same session.
pub struct ServiceBusTransport {
    connection: ConnectionHandle<()>,
    receiver: Receiver,
    /// Received deliveries the core loop has yet to settle, by synthetic
    /// tag; `None` when the link already settled it (oversized message).
    unsettled: HashMap<u64, Option<DeliveryInfo>>,
    locks: LockTable,
    lock_renew: Duration,
    next_tag: u64,
    /// Total in-flight cap (`--prefetch`).
    cap: u32,
    intake_stopped: bool,
    credit_closed: bool,
    control: Option<Control>,
}

/// The running control task: `stop` ends it; it hands the session back.
struct Control {
    stop: oneshot::Sender<()>,
    task: JoinHandle<SessionHandle<()>>,
}

/// One connection's worth of open handles (before the control task starts).
struct Opened {
    connection: ConnectionHandle<()>,
    session: SessionHandle<()>,
    receiver: Receiver,
    cbs: CbsClient,
    token_expiry: SystemTime,
}

impl ServiceBusTransport {
    async fn connect(
        params: ServiceBusParams,
        cap: u32,
        connect_timeout: Option<Duration>,
        lock_renew: Duration,
    ) -> Result<Self> {
        info!(
            "Connecting to Azure Service Bus at {} (queue {})",
            params.url(),
            params.queue
        );
        let link_name = format!("{INSTANCE_NAME}-{}-{}", params.queue, std::process::id());
        let mut opened =
            connect_with_retry(&params, &link_name, connect_timeout, CONNECT_RETRY_DELAY)
                .await
                .context("failed to connect to Azure Service Bus")?;
        opened
            .receiver
            .set_credit(cap)
            .await
            .context("failed to grant link credit")?;
        let locks = LockTable::default();
        let mut control = ControlTask {
            params: params.clone(),
            session: opened.session,
            cbs: Some(opened.cbs),
            mgmt: None,
            mgmt_generation: 0,
            link_name,
            locks: locks.clone(),
            lock_renew,
            token_expiry: opened.token_expiry,
        };
        // Attach the management client now so a broken management node is
        // seen at startup; a failure here is only logged (the renewer
        // re-attaches on its next tick).
        control.ensure_mgmt().await;
        let (stop, stop_rx) = oneshot::channel();
        let task = tokio::spawn(control.run(stop_rx));
        Ok(Self {
            connection: opened.connection,
            receiver: opened.receiver,
            unsettled: HashMap::new(),
            locks,
            lock_renew,
            next_tag: 1,
            cap,
            intake_stopped: false,
            credit_closed: false,
            control: Some(Control { stop, task }),
        })
    }

    /// Re-grants credit so `unsettled + credit == cap` (0 once intake has
    /// stopped).
    async fn replenish(&mut self) {
        if self.credit_closed {
            return;
        }
        let credit = if self.intake_stopped {
            self.credit_closed = true;
            0
        } else {
            let unsettled = u32::try_from(self.unsettled.len()).unwrap_or(u32::MAX);
            self.cap.saturating_sub(unsettled)
        };
        if let Err(e) = self.receiver.set_credit(credit).await {
            warn!("AMQP flow (credit {credit}) failed: {e}");
        }
    }

    /// Removes `tag` from the unsettled map and the lock table, logging an
    /// unknown tag. `None` also when the link already settled the delivery.
    fn take(&mut self, tag: u64, verb: &str) -> Option<DeliveryInfo> {
        let entry = self.unsettled.remove(&tag);
        if let Some(lock) = self.locks.remove(tag)
            && lock.lost
        {
            warn!(
                "settling delivery {tag} ({verb}) after its lock was lost (lock token {}): \
                 Service Bus ignores it and redelivers the message",
                format_uuid(&lock.token)
            );
        }
        if entry.is_none() {
            warn!("no unsettled AMQP delivery {tag}; cannot {verb}");
        }
        entry.flatten()
    }

    /// Registers a received delivery's lock for renewal.
    /// `lock_remaining`: from the delivery's `x-opt-locked-until`, if any.
    fn track_lock(&mut self, tag: u64, info: &DeliveryInfo, lock_remaining: Option<Duration>) {
        let Some(token) = lock_token_from_delivery_tag(info.delivery_tag()) else {
            warn!(
                "delivery {tag} has a {}-byte delivery tag, not a 16-byte lock token; its lock \
                 cannot be renewed",
                info.delivery_tag().len()
            );
            return;
        };
        let renew_at = Instant::now() + renew_delay(self.lock_renew, lock_remaining);
        self.locks.insert(tag, token, renew_at);
    }
}

/// The `x-opt-locked-until` message annotation, if present.
fn locked_until(message: &Message<Body<Value>>) -> Option<Timestamp> {
    let annotations = message.message_annotations.as_ref()?;
    match annotations.0.get(&OwnedKey::from(LOCKED_UNTIL))? {
        Value::Timestamp(t) => Some(t.clone()),
        _ => None,
    }
}

/// Time from now until `t` (`None` if `t` is in the past).
fn remaining_until(t: Timestamp) -> Option<Duration> {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()?
        .as_millis();
    let at_ms = u128::try_from(t.milliseconds()).ok()?;
    at_ms
        .checked_sub(now_ms)
        .map(|ms| Duration::from_millis(u64::try_from(ms).unwrap_or(u64::MAX)))
}

/// Maps one receive result: the delivery info to settle (`None`: the link
/// already settled it), the record bytes and the lock's remaining time (from
/// `x-opt-locked-until`). Per-message errors become an empty body
/// (dead-lettered as malformed); any other error is fatal.
type Received = (Option<DeliveryInfo>, Vec<u8>, Option<Duration>);

fn received(result: Result<AmqpDelivery<Body<Value>>, RecvError>) -> Result<Received> {
    match result {
        Ok(delivery) => {
            let (info, message) = delivery.into_parts();
            let lock_remaining = locked_until(&message).and_then(remaining_until);
            Ok((Some(info), body_bytes(message.body), lock_remaining))
        }
        Err(RecvError::MessageDecode(e)) => {
            warn!("undecodable AMQP message (dead-lettering it): {}", e.source);
            Ok((Some(e.info), Vec::new(), None))
        }
        Err(RecvError::MessageSizeExceeded(e)) => {
            warn!("oversized AMQP message (already rejected by the link): {e}");
            Ok((None, Vec::new(), None))
        }
        Err(e) => Err(anyhow!("Service Bus receive failed (link lost): {e}")),
    }
}

/// `s` cut on a char boundary to [`DEAD_LETTER_DESCRIPTION_MAX_BYTES`].
fn cap_description(s: &str) -> String {
    let mut end = s.len().min(DEAD_LETTER_DESCRIPTION_MAX_BYTES);
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    s[..end].to_string()
}

/// Short `DeadLetterReason` for each cause; the full text goes in
/// `DeadLetterErrorDescription`.
fn dead_letter_reason_label(reason: &DeadLetterReason) -> &'static str {
    match reason {
        DeadLetterReason::Malformed(_) => "MalformedRecord",
        DeadLetterReason::Rejected(_) => "SenzingRejected",
        DeadLetterReason::LongRecord => "LongRecord",
    }
}

/// The `rejected` outcome's error that dead-letters a message on Service Bus
/// (azure-sdk-for-python `settle_message_via_receiver_link`,
/// MESSAGE_DEAD_LETTER): condition `com.microsoft:dead-letter`, description =
/// the error description, info = {`DeadLetterReason`,
/// `DeadLetterErrorDescription`}. Service Bus copies the two info entries into
/// the dead-lettered message's application properties.
pub fn dead_letter_error(reason: &DeadLetterReason) -> AmqpErrorBody {
    let description = cap_description(&reason.to_string());
    let mut info = Fields::new();
    info.insert(
        Symbol::from(DEAD_LETTER_REASON),
        Value::String(dead_letter_reason_label(reason).to_string()),
    );
    info.insert(
        Symbol::from(DEAD_LETTER_ERROR_DESCRIPTION),
        Value::String(description.clone()),
    );
    AmqpErrorBody::new(
        ErrorCondition::Custom(Symbol::from(DEAD_LETTER_CONDITION)),
        Some(description),
        Some(info),
    )
}

impl Transport for ServiceBusTransport {
    /// `Receiver::recv` is cancel-safe; everything after it is synchronous.
    async fn recv(&mut self) -> Option<Result<Delivery>> {
        let (info, body, lock_remaining) = match received(self.receiver.recv().await) {
            Ok(r) => r,
            Err(e) => return Some(Err(e)),
        };
        let tag = self.next_tag;
        self.next_tag += 1;
        if let Some(info) = &info {
            self.track_lock(tag, info, lock_remaining);
        }
        self.unsettled.insert(tag, info);
        Some(Ok(Delivery { tag, body }))
    }

    /// `accepted` = complete: the message is removed from the queue.
    async fn ack(&mut self, tag: u64) {
        let Some(info) = self.take(tag, "complete") else {
            return;
        };
        if let Err(e) = self.receiver.accept(info).await {
            error!("AMQP accept failed for {tag}: {e}");
        }
        self.replenish().await;
    }

    /// `rejected` with `com.microsoft:dead-letter`: Service Bus moves the
    /// message to `<queue>/$DeadLetterQueue` with the reason properties.
    async fn dead_letter(&mut self, tag: u64, reason: DeadLetterReason) {
        let Some(info) = self.take(tag, "dead-letter") else {
            return;
        };
        if let Err(e) = self.receiver.reject(info, dead_letter_error(&reason)).await {
            error!("AMQP dead-letter failed for {tag} ({reason}): {e}");
        }
        self.replenish().await;
    }

    /// `released`: the lock is dropped and the message is immediately
    /// redeliverable (what azure-sdk-for-python sends for buffered messages
    /// when a peek-lock receiver closes). Verified on the Service Bus emulator:
    /// `released` leaves DeliveryCount unchanged, whereas the SDK's explicit
    /// abandon (`modified`, delivery-failed) and a lock that expires each add
    /// one — and MaxDeliveryCount dead-letters on that count. Not verified
    /// against a cloud namespace; if a cloud namespace does count releases,
    /// every rolling restart costs the in-flight messages one attempt.
    async fn release(&mut self, tag: u64) {
        self.replenish().await;
        let Some(info) = self.take(tag, "release") else {
            return;
        };
        if let Err(e) = self.receiver.release(info).await {
            warn!(
                "AMQP release failed for {tag}: {e}; Service Bus redelivers it after the \
                 connection closes or its lock expires"
            );
        }
    }

    fn stop_intake(&mut self) {
        self.intake_stopped = true;
    }

    /// No queue depth in v1 (the runtime count needs the ATOM management
    /// API, not AMQP).
    async fn depth(&mut self) -> Option<u32> {
        None
    }

    /// Stops the control task, then closes link, session and connection
    /// (bounded). Anything still unsettled is unlocked by the broker and
    /// redelivered.
    async fn close(mut self) {
        if !self.unsettled.is_empty() {
            info!(
                "leaving {} unsettled AMQP delivery(ies) for redelivery",
                self.unsettled.len()
            );
        }
        let control = self.control.take();
        let closing = async {
            if let Err(e) = self.receiver.close().await {
                warn!("error closing AMQP receiver: {e}");
            }
            if let Some(Control { stop, task }) = control {
                let _ = stop.send(());
                match task.await {
                    Ok(mut session) => {
                        if let Err(e) = session.end().await {
                            warn!("error ending AMQP session: {e}");
                        }
                    }
                    Err(e) => warn!("Service Bus control task failed: {e}"),
                }
            }
            if let Err(e) = self.connection.close().await {
                warn!("error closing AMQP connection: {e}");
            }
        };
        if tokio::time::timeout(CLOSE_TIMEOUT, closing).await.is_err() {
            warn!("AMQP close timed out after {CLOSE_TIMEOUT:?}");
        }
    }
}

/// Owns the session and the `$cbs` / `$management` clients: re-puts the SAS
/// token at half its life and renews due peek-locks. Every request runs to
/// completion inside a select arm (never cancelled midway), each bounded by
/// [`MGMT_TIMEOUT`]; a client that timed out or failed at the link level is
/// dropped and re-attached (fe2o3's management client does not correlate
/// responses, so a late reply must never reach the next request).
struct ControlTask {
    params: ServiceBusParams,
    session: SessionHandle<()>,
    cbs: Option<CbsClient>,
    mgmt: Option<MgmtClient>,
    mgmt_generation: u32,
    link_name: String,
    locks: LockTable,
    lock_renew: Duration,
    token_expiry: SystemTime,
}

impl ControlTask {
    async fn run(mut self, mut stop: oneshot::Receiver<()>) -> SessionHandle<()> {
        let mut renew = tokio::time::interval(RENEW_TICK);
        renew.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut refresh_at = tokio::time::Instant::now() + self.params.token_ttl / 2;
        loop {
            tokio::select! {
                biased;
                _ = &mut stop => break,
                _ = tokio::time::sleep_until(refresh_at) => {
                    refresh_at = tokio::time::Instant::now()
                        + if self.refresh_token().await { self.params.token_ttl / 2 } else { TOKEN_RETRY };
                }
                _ = renew.tick() => self.renew_due().await,
            }
        }
        if let Some(cbs) = self.cbs.take() {
            let _ = cbs.close().await;
        }
        if let Some(mgmt) = self.mgmt.take() {
            let _ = mgmt.close().await;
        }
        self.session
    }

    /// Puts a fresh SAS token; `true` on success.
    async fn refresh_token(&mut self) -> bool {
        if self.cbs.is_none() {
            match CbsClient::attach(&mut self.session).await {
                Ok(c) => self.cbs = Some(c),
                Err(e) => {
                    warn!("CBS re-attach failed: {e}; token refresh retried in {TOKEN_RETRY:?}");
                    return false;
                }
            }
        }
        let cbs = self.cbs.as_mut().expect("attached above");
        match tokio::time::timeout(MGMT_TIMEOUT, put_token(cbs, &self.params)).await {
            Ok(Ok(expiry)) => {
                self.token_expiry = expiry;
                info!(
                    "Service Bus SAS token refreshed (valid {}s)",
                    self.params.token_ttl.as_secs()
                );
                true
            }
            outcome => {
                let why = match outcome {
                    Ok(Err(e)) => format!("{e:#}"),
                    _ => format!("no answer within {MGMT_TIMEOUT:?}"),
                };
                // Drop the client: a late reply must not answer the next put.
                self.cbs = None;
                let left = self
                    .token_expiry
                    .duration_since(SystemTime::now())
                    .unwrap_or_default();
                warn!(
                    "Service Bus SAS token refresh failed: {why}; current token expires in \
                     {}s (Service Bus then detaches the link: fatal); retrying in {TOKEN_RETRY:?}",
                    left.as_secs()
                );
                false
            }
        }
    }

    /// Attaches the management client if it is not attached; `false` if that
    /// failed.
    async fn ensure_mgmt(&mut self) -> bool {
        if self.mgmt.is_some() {
            return true;
        }
        self.mgmt_generation += 1;
        let node = format!("{}/$management", self.params.queue);
        let client_node = format!("{}-mgmt-{}", self.link_name, self.mgmt_generation);
        let attach = MgmtClient::builder()
            .management_node_address(node.clone())
            .client_node_addr(client_node)
            .attach(&mut self.session);
        match tokio::time::timeout(MGMT_TIMEOUT, attach).await {
            Ok(Ok(c)) => {
                self.mgmt = Some(c);
                true
            }
            Ok(Err(e)) => {
                warn!("attach to {node} failed: {e}; lock renewal retried next tick");
                false
            }
            Err(_) => {
                warn!("attach to {node} timed out; lock renewal retried next tick");
                false
            }
        }
    }

    /// Renews every due lock, one request per token (a lost lock fails only
    /// its own request).
    async fn renew_due(&mut self) {
        let due = self.locks.due(Instant::now());
        if due.is_empty() || !self.ensure_mgmt().await {
            return;
        }
        let retry = Instant::now() + RENEW_RETRY;
        for (tag, token) in due {
            let Some(mgmt) = self.mgmt.as_mut() else {
                self.locks.retry_at(tag, retry);
                continue;
            };
            let request = RenewLockRequest {
                lock_tokens: vec![token],
                associated_link_name: self.link_name.clone(),
                server_timeout_ms: u32::try_from(MGMT_TIMEOUT.as_millis()).unwrap_or(u32::MAX),
            };
            let result = tokio::time::timeout(MGMT_TIMEOUT, mgmt.call(request)).await;
            let token_str = format_uuid(&token);
            match result {
                Ok(Ok(response)) => {
                    let remaining = response
                        .expirations
                        .first()
                        .cloned()
                        .and_then(remaining_until);
                    let next = Instant::now() + renew_delay(self.lock_renew, remaining);
                    if let Some(n) = self.locks.renewed(tag, next) {
                        info!(
                            "renewed the lock on delivery {tag} (lock token {token_str}, renewal \
                             {n}{})",
                            remaining
                                .map(|r| format!(", locked for another {}s", r.as_secs()))
                                .unwrap_or_default()
                        );
                    }
                }
                Ok(Err(e)) if is_lock_lost(&e) => {
                    self.locks.mark_lost(tag);
                    warn!(
                        "lock lost on delivery {tag} (lock token {token_str}): {e}; Service Bus \
                         redelivers the message (add_record is idempotent)"
                    );
                }
                Ok(Err(e)) if is_status_error(&e) => {
                    let failures = self.locks.failed(tag, retry);
                    warn!(
                        "renew-lock failed for delivery {tag} (lock token {token_str}, attempt \
                         {failures}): {e}"
                    );
                    if failures >= MAX_RENEW_FAILURES {
                        self.locks.mark_lost(tag);
                        warn!(
                            "giving up renewing delivery {tag}; it is redelivered at lock expiry"
                        );
                    }
                }
                outcome => {
                    let why = match outcome {
                        Ok(Err(e)) => format!("{e}"),
                        _ => format!("no answer within {MGMT_TIMEOUT:?}"),
                    };
                    warn!(
                        "renew-lock request failed for delivery {tag} (lock token {token_str}): \
                         {why}; re-attaching the management link"
                    );
                    self.locks.failed(tag, retry);
                    if let Some(old) = self.mgmt.take() {
                        drop(tokio::time::timeout(CLOSE_TIMEOUT, old.close()).await);
                    }
                    // Remaining due tokens wait for the next tick.
                    for (t, _) in self.locks.due(Instant::now()) {
                        self.locks.retry_at(t, retry);
                    }
                    return;
                }
            }
        }
    }
}

/// CBS `put-token` of a fresh SAS token for the queue's audience; returns its
/// expiry.
async fn put_token(cbs: &mut CbsClient, params: &ServiceBusParams) -> Result<SystemTime> {
    let Auth::SasKey { key_name, key } = &params.auth;
    let expiry = SystemTime::now() + params.token_ttl;
    let expiry_secs = expiry.duration_since(UNIX_EPOCH)?.as_secs();
    let audience = params.audience();
    let token = generate_sas_token(&audience, key_name, key, expiry_secs);
    let expiry_ts = Timestamp::from_milliseconds(i64::try_from(expiry_secs)? * 1000);
    cbs.put_token(
        audience.as_str(),
        CbsToken::new(token, SAS_TOKEN_TYPE, expiry_ts),
    )
    .await
    .map_err(|e| anyhow!("CBS put-token for {audience} rejected: {e}"))?;
    Ok(expiry)
}

/// Whether a connect error can be fixed by retrying (namespace unreachable /
/// restarting) — as opposed to bad credentials, an unknown queue or a bad URL.
#[derive(Debug)]
struct PermanentConnectError(anyhow::Error);

impl std::fmt::Display for PermanentConnectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#}", self.0)
    }
}

impl std::error::Error for PermanentConnectError {}

/// One connect attempt: TCP + TLS + SASL ANONYMOUS + Open, session, CBS
/// put-token, receiver attach.
async fn open_once(params: &ServiceBusParams, link_name: &str) -> Result<Opened> {
    let container_id = format!("{INSTANCE_NAME}-{}", std::process::id());
    let url = params.url();
    let mut connection = Connection::builder()
        .container_id(container_id)
        .alt_tls_establishment(params.tls)
        .sasl_profile(SaslProfile::Anonymous)
        .idle_time_out(u32::try_from(IDLE_TIMEOUT.as_millis()).unwrap_or(u32::MAX))
        .open(url.as_str())
        .await
        .map_err(|e| {
            let permanent = sz_activemq_amqp::open_error_is_permanent(&e);
            let err = anyhow!(e).context(format!("AMQP open of {url} failed"));
            if permanent {
                anyhow!(PermanentConnectError(err))
            } else {
                err
            }
        })?;
    let mut session = Session::begin(&mut connection)
        .await
        .context("failed to begin AMQP session")?;
    let mut cbs = CbsClient::attach(&mut session)
        .await
        .context("failed to attach to the $cbs node")?;
    // A rejected token (bad key / rule without Listen) cannot be fixed by
    // retrying.
    let token_expiry = put_token(&mut cbs, params)
        .await
        .map_err(|e| anyhow!(PermanentConnectError(e)))?;
    let receiver = Receiver::builder()
        .name(link_name.to_string())
        .source(Source::builder().address(params.queue.clone()).build())
        .target(link_name.to_string())
        .sender_settle_mode(SenderSettleMode::Unsettled)
        .receiver_settle_mode(ReceiverSettleMode::Second)
        .credit_mode(CreditMode::Manual)
        .auto_accept(false)
        .attach(&mut session)
        .await
        .map_err(|e| {
            anyhow!(PermanentConnectError(anyhow!(
                "failed to attach a receiver to queue '{}': {e}",
                params.queue
            )))
        })?;
    Ok(Opened {
        connection,
        session,
        receiver,
        cbs,
        token_expiry,
    })
}

/// Connects, retrying TRANSIENT failures every `delay` until `limit` has
/// elapsed (`None` = forever). Permanent failures (rejected token, unknown
/// queue, bad URL) return at once.
async fn connect_with_retry(
    params: &ServiceBusParams,
    link_name: &str,
    limit: Option<Duration>,
    delay: Duration,
) -> Result<Opened> {
    let start = std::time::Instant::now();
    let mut attempt: u32 = 0;
    loop {
        attempt += 1;
        let result = tokio::time::timeout(CONNECT_ATTEMPT_TIMEOUT, open_once(params, link_name))
            .await
            .map_err(|_| anyhow!("no AMQP handshake within {CONNECT_ATTEMPT_TIMEOUT:?}"))
            .and_then(|r| r);
        let err = match result {
            Ok(opened) => {
                if attempt > 1 {
                    info!(
                        "connected to Azure Service Bus after {attempt} attempts ({:.0}s)",
                        start.elapsed().as_secs_f64()
                    );
                }
                return Ok(opened);
            }
            Err(e) => e,
        };
        if err.downcast_ref::<PermanentConnectError>().is_some() {
            return Err(err);
        }
        if let Some(limit) = limit
            && start.elapsed() >= limit
        {
            return Err(err.context(format!(
                "Service Bus unreachable for {}s (--reconnect-timeout)",
                limit.as_secs()
            )));
        }
        warn!("Service Bus connect attempt {attempt} failed: {err:#}; retrying in {delay:?}");
        tokio::time::sleep(delay).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conn_str::parse_connection_string;
    use fe2o3_amqp::link::{LinkStateError, MessageSizeExceeded};

    #[test]
    fn dead_letter_error_carries_the_sdk_condition_and_reason_fields() {
        let e = dead_letter_error(&DeadLetterReason::Rejected("SENZ0023|bad".into()));
        assert_eq!(
            e.condition,
            ErrorCondition::Custom(Symbol::from("com.microsoft:dead-letter"))
        );
        assert_eq!(e.description.as_deref(), Some("SENZ0023|bad"));
        let info = e.info.expect("info");
        assert_eq!(
            info.get(&Symbol::from("DeadLetterReason")),
            Some(&Value::String("SenzingRejected".into()))
        );
        assert_eq!(
            info.get(&Symbol::from("DeadLetterErrorDescription")),
            Some(&Value::String("SENZ0023|bad".into()))
        );
        let m = dead_letter_error(&DeadLetterReason::Malformed("missing RECORD_ID".into()));
        assert_eq!(
            m.info.unwrap().get(&Symbol::from("DeadLetterReason")),
            Some(&Value::String("MalformedRecord".into()))
        );
        assert_eq!(
            m.description.as_deref(),
            Some("malformed record: missing RECORD_ID")
        );
    }

    #[test]
    fn dead_letter_description_is_capped_on_a_char_boundary() {
        let long = "é".repeat(DEAD_LETTER_DESCRIPTION_MAX_BYTES);
        let e = dead_letter_error(&DeadLetterReason::Rejected(long));
        let d = e.description.unwrap();
        assert!(d.len() <= DEAD_LETTER_DESCRIPTION_MAX_BYTES);
        assert!(d.chars().all(|c| c == 'é'));
    }

    #[test]
    fn oversized_message_is_a_poison_delivery_already_settled() {
        let oversized = RecvError::MessageSizeExceeded(MessageSizeExceeded {
            size: 2048,
            max_size: 1024,
        });
        let (info, body, lock) = received(Err(oversized)).expect("not fatal");
        assert!(info.is_none() && body.is_empty() && lock.is_none());
    }

    #[test]
    fn link_level_receive_errors_are_fatal() {
        for e in [
            RecvError::LinkStateError(LinkStateError::RemoteClosed),
            RecvError::LinkStateError(LinkStateError::RemoteDetached),
            RecvError::TransferLimitExceeded,
        ] {
            let err = received(Err(e)).expect_err("fatal");
            assert!(err.to_string().contains("link lost"), "{err}");
        }
    }

    #[test]
    fn remaining_until_is_none_in_the_past() {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        assert!(remaining_until(Timestamp::from_milliseconds(now_ms - 1000)).is_none());
        let r = remaining_until(Timestamp::from_milliseconds(now_ms + 60_000)).unwrap();
        assert!(
            r > Duration::from_secs(55) && r <= Duration::from_secs(60),
            "{r:?}"
        );
    }

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime")
    }

    /// A port nothing listens on: connects are REFUSED.
    fn refused_port() -> u16 {
        let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = l.local_addr().expect("addr").port();
        drop(l);
        port
    }

    /// Connection refused (namespace unreachable) is retried until the limit,
    /// then reported as unreachable.
    #[test]
    fn refused_connect_is_retried_until_the_limit() {
        let cs = format!(
            "Endpoint=sb://127.0.0.1:{}/;SharedAccessKeyName=n;SharedAccessKey=k;\
             UseDevelopmentEmulator=true",
            refused_port()
        );
        let p = parse_connection_string(&cs, Some("q")).unwrap();
        let start = std::time::Instant::now();
        let err = rt()
            .block_on(connect_with_retry(
                &p,
                "link",
                Some(Duration::from_millis(900)),
                Duration::from_millis(200),
            ))
            .err()
            .expect("nothing listens");
        let took = start.elapsed();
        assert!(took >= Duration::from_millis(900), "gave up after {took:?}");
        assert!(took < Duration::from_secs(5), "overran the limit: {took:?}");
        assert!(format!("{err:#}").contains("unreachable for"), "{err:#}");
    }
}
