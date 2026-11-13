//! Azure Service Bus end-to-end tests against a real engine and the LOCAL
//! Service Bus emulator (`mcr.microsoft.com/azure-messaging/servicebus-emulator`)
//! over AMQP 1.0.
//!
//! Gated on `SENZING_ENGINE_CONFIGURATION_JSON` (a repository with the `TEST`
//! data source) and `IT_SB_EMULATOR_CONNECTION_STRING` (the emulator's
//! connection string; it MUST contain `UseDevelopmentEmulator=true` — the
//! suite refuses to touch a real namespace). The emulator must be started
//! with `tests/fixtures/emulator-config.json` (it cannot create queues at
//! runtime). Without them each test prints `SKIP` and passes — or FAILS when
//! `IT_REQUIRE_INFRA=1`.
//!
//! What is proven here that unit tests cannot:
//! * CBS auth (SASL ANONYMOUS + put-token) and the peek-lock receiver work
//!   against the emulator; a Data-section body and an AmqpValue string both
//!   load;
//! * an engine reject and an unparseable body are dead-lettered to
//!   `<queue>/$DeadLetterQueue` with `DeadLetterReason` /
//!   `DeadLetterErrorDescription` set, and the driver keeps running;
//! * lock renewal: records that run 2.5x the queue's 10 s lockDuration are
//!   renewed (renew-lock on `<queue>/$management`), loaded exactly once and
//!   never redelivered;
//! * shutdown releases unsettled deliveries back to the queue (not
//!   dead-lettered), with DeliveryCount unchanged on the emulator.

use std::process::{Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use fe2o3_amqp::connection::ConnectionHandle;
use fe2o3_amqp::sasl_profile::SaslProfile;
use fe2o3_amqp::session::SessionHandle;
use fe2o3_amqp::types::messaging::{AmqpValue, Body, Data, Message};
use fe2o3_amqp::types::primitives::{Binary, SimpleValue, Value};
use fe2o3_amqp::{Connection, Receiver, Sender, Session};
use fe2o3_amqp_cbs::client::CbsClient;
use fe2o3_amqp_cbs::token::CbsToken;

#[allow(dead_code)]
#[path = "../src/conn_str.rs"]
mod conn_str;
#[allow(dead_code)]
#[path = "../src/sas.rs"]
mod sas;

use conn_str::{ServiceBusParams, parse_connection_string};

/// Queues of `tests/fixtures/emulator-config.json`.
const LOAD_QUEUE: &str = "sz-it-load";
const RENEW_QUEUE: &str = "sz-it-renew";
const RELEASE_QUEUE: &str = "sz-it-release";

// --------------------------------------------------------------------------
// Gating
// --------------------------------------------------------------------------

fn skip(reason: std::fmt::Arguments<'_>) {
    let required = std::env::var("IT_REQUIRE_INFRA").is_ok_and(|v| !v.is_empty() && v != "0");
    assert!(
        !required,
        "IT_REQUIRE_INFRA set but test cannot run: {reason}"
    );
    eprintln!("{reason}");
}

fn env_nonempty(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.is_empty())
}

/// The emulator connection string when the rig is configured, else `None`.
///
/// Opt-in real-namespace smoke run: `IT_SB_ALLOW_REAL_NAMESPACE=1` plus
/// `IT_SB_REAL_CONNECTION_STRING` (a Send+Listen rule on a THROWAWAY namespace
/// whose `sz-it-load` / `sz-it-renew` / `sz-it-release` queues were created with
/// the properties in `tests/fixtures/emulator-config.json`; the tests purge them).
fn gate() -> Option<String> {
    if env_nonempty("IT_SB_ALLOW_REAL_NAMESPACE").as_deref() == Some("1") {
        if env_nonempty("SENZING_ENGINE_CONFIGURATION_JSON").is_none() {
            skip(format_args!(
                "SKIP: SENZING_ENGINE_CONFIGURATION_JSON not set"
            ));
            return None;
        }
        let Some(cs) = env_nonempty("IT_SB_REAL_CONNECTION_STRING") else {
            skip(format_args!(
                "SKIP: IT_SB_ALLOW_REAL_NAMESPACE=1 but IT_SB_REAL_CONNECTION_STRING not set"
            ));
            return None;
        };
        eprintln!("NOTE: running against a REAL Service Bus namespace (opt-in)");
        return Some(cs);
    }
    for var in [
        "SENZING_ENGINE_CONFIGURATION_JSON",
        "IT_SB_EMULATOR_CONNECTION_STRING",
    ] {
        if env_nonempty(var).is_none() {
            skip(format_args!("SKIP: {var} not set"));
            return None;
        }
    }
    let cs = env_nonempty("IT_SB_EMULATOR_CONNECTION_STRING")?;
    assert!(
        cs.to_ascii_lowercase()
            .contains("usedevelopmentemulator=true"),
        "IT_SB_EMULATOR_CONNECTION_STRING must point at the local emulator \
         (UseDevelopmentEmulator=true); refusing to run against a real namespace"
    );
    Some(cs)
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime")
}

// --------------------------------------------------------------------------
// The test's own AMQP client (producer / DLQ reader)
// --------------------------------------------------------------------------

struct Client {
    connection: ConnectionHandle<()>,
    session: SessionHandle<()>,
    cbs: CbsClient,
    params: ServiceBusParams,
}

impl Client {
    async fn connect(cs: &str, queue: &str) -> Self {
        let params = parse_connection_string(cs, Some(queue)).expect("connection string");
        let mut connection = Connection::builder()
            .container_id(format!("sz-sb-e2e-{}", std::process::id()))
            .alt_tls_establishment(params.tls)
            .sasl_profile(SaslProfile::Anonymous)
            .open(params.url().as_str())
            .await
            .expect("test client connects to the emulator");
        let mut session = Session::begin(&mut connection).await.expect("session");
        let cbs = CbsClient::attach(&mut session).await.expect("cbs");
        let mut client = Self {
            connection,
            session,
            cbs,
            params,
        };
        client.authorize(queue).await;
        client.authorize(&format!("{queue}/$DeadLetterQueue")).await;
        client
    }

    async fn authorize(&mut self, entity: &str) {
        let conn_str::Auth::SasKey { key_name, key } = &self.params.auth;
        let mut p = self.params.clone();
        p.queue = entity.to_string();
        let audience = p.audience();
        let expiry = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600;
        let token = sas::generate_sas_token(&audience, key_name, key, expiry);
        self.cbs
            .put_token(
                audience.as_str(),
                CbsToken::new(token, sas::SAS_TOKEN_TYPE, None),
            )
            .await
            .expect("put-token");
    }

    async fn send(&mut self, queue: &str, bodies: &[Body<Value>]) {
        let mut sender = Sender::attach(&mut self.session, format!("e2e-send-{queue}"), queue)
            .await
            .expect("sender attach");
        for body in bodies {
            let msg = Message::builder().body(body.clone()).build();
            sender
                .send(msg)
                .await
                .expect("send")
                .accepted_or("not accepted")
                .expect("accepted");
        }
        sender.close().await.expect("sender close");
    }

    /// Receives (and completes) everything on `address` until it is idle for
    /// `idle`: the messages, for inspection.
    async fn drain(&mut self, address: &str, idle: Duration) -> Vec<Message<Body<Value>>> {
        let mut receiver =
            Receiver::attach(&mut self.session, format!("e2e-drain-{address}"), address)
                .await
                .expect("receiver attach");
        let mut out = Vec::new();
        while let Ok(r) = tokio::time::timeout(idle, receiver.recv::<Body<Value>>()).await {
            let delivery = r.expect("recv");
            receiver.accept(&delivery).await.expect("accept");
            out.push(delivery.into_message());
        }
        receiver.close().await.expect("receiver close");
        out
    }

    async fn close(self) {
        let _ = self.cbs.close().await;
        let mut session = self.session;
        let _ = session.end().await;
        let mut connection = self.connection;
        let _ = connection.close().await;
    }
}

fn data(s: &str) -> Body<Value> {
    Body::Data(vec![Data(Binary::from(s.as_bytes().to_vec()))].into())
}

fn text(s: &str) -> Body<Value> {
    Body::Value(AmqpValue(Value::String(s.to_string())))
}

fn record(id: &str) -> String {
    format!(r#"{{"DATA_SOURCE":"TEST","RECORD_ID":"SBIT-{id}","NAME_FULL":"Service Bus {id}"}}"#)
}

fn body_text(m: &Message<Body<Value>>) -> String {
    match &m.body {
        Body::Data(d) => d
            .iter()
            .map(|b| String::from_utf8_lossy(b.0.as_ref()).into_owned())
            .collect(),
        Body::Value(AmqpValue(Value::String(s))) => s.clone(),
        other => format!("{other:?}"),
    }
}

fn app_prop(m: &Message<Body<Value>>, key: &str) -> Option<String> {
    match m.application_properties.as_ref()?.0.get(key)? {
        SimpleValue::String(s) => Some(s.clone()),
        other => Some(format!("{other:?}")),
    }
}

/// Fresh, empty queue + DLQ (the emulator's queues outlive a run).
async fn purge(c: &mut Client, queue: &str) {
    c.drain(queue, Duration::from_secs(2)).await;
    c.drain(&format!("{queue}/$DeadLetterQueue"), Duration::from_secs(2))
        .await;
}

// --------------------------------------------------------------------------
// Driving the binary
// --------------------------------------------------------------------------

struct Driver {
    child: std::process::Child,
    out: std::path::PathBuf,
}

impl Driver {
    fn spawn(cs: &str, queue: &str, threads: usize, extra_env: &[(&str, String)]) -> Self {
        let out = std::env::temp_dir().join(format!(
            "sz-sb-e2e-{queue}-{}-{}.out",
            std::process::id(),
            Instant::now().elapsed().as_nanos()
        ));
        let stdout = std::fs::File::create(&out).expect("stdout file");
        let stderr = std::fs::File::create(out.with_extension("err")).expect("stderr file");
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_sz_servicebus_combined_consumer"));
        cmd.env("SENZING_AZURE_QUEUE_CONNECTION_STRING", cs)
            .env("SENZING_AZURE_QUEUE_NAME", queue)
            .env("SENZING_REDO_PERCENT", "0")
            .env("SENZING_THREADS_PER_PROCESS", threads.to_string())
            .env_remove("SENZING_PREFETCH")
            .env_remove("SENZING_RECORD_TRANSFORM_PLUGIN")
            .env_remove("SENZING_RECORD_TRANSFORM_CONFIG")
            .stdin(Stdio::null())
            .stdout(stdout)
            .stderr(stderr);
        for (k, v) in extra_env {
            cmd.env(k, v);
        }
        Self {
            child: cmd.spawn().expect("spawn driver"),
            out,
        }
    }

    fn stdout(&self) -> String {
        std::fs::read_to_string(&self.out).unwrap_or_default()
    }

    fn stderr(&self) -> String {
        std::fs::read_to_string(self.out.with_extension("err")).unwrap_or_default()
    }

    /// Waits until `pred(stdout + stderr)` holds (or `limit` passes).
    fn wait_for(&self, limit: Duration, pred: impl Fn(&str) -> bool) -> bool {
        let deadline = Instant::now() + limit;
        while Instant::now() < deadline {
            if pred(&(self.stdout() + &self.stderr())) {
                return true;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        false
    }

    /// SIGTERM, then wait (bounded): exit code + combined output.
    fn stop(mut self) -> (Option<i32>, String) {
        // SAFETY: plain kill(2) on our own child's pid.
        unsafe {
            libc::kill(self.child.id() as libc::pid_t, libc::SIGTERM);
        }
        let deadline = Instant::now() + Duration::from_secs(40);
        let code = loop {
            match self.child.try_wait().expect("try_wait") {
                Some(st) => break st.code(),
                None if Instant::now() >= deadline => {
                    let _ = self.child.kill();
                    let _ = self.child.wait();
                    break None;
                }
                None => std::thread::sleep(Duration::from_millis(100)),
            }
        };
        let text = self.stdout() + &self.stderr();
        let _ = std::fs::remove_file(&self.out);
        let _ = std::fs::remove_file(self.out.with_extension("err"));
        (code, text)
    }
}

/// The add / rejected counts from `Processed total of N adds, M redo records
/// (R rejected, ...`.
fn final_totals(output: &str) -> (usize, usize) {
    let line = output
        .lines()
        .find(|l| l.starts_with("Processed total of "))
        .unwrap_or_else(|| panic!("no final total line\n{output}"));
    let nums: Vec<usize> = line
        .split(|c: char| !c.is_ascii_digit())
        .filter(|s| !s.is_empty())
        .filter_map(|s| s.parse().ok())
        .collect();
    (nums[0], nums[2])
}

/// `LONG_RECORD=4`: the `Combined stats` line (with the add / reject counts
/// the tests wait on) every 2 s.
fn fast_stats() -> Vec<(&'static str, String)> {
    vec![("LONG_RECORD", "4".to_string())]
}

fn example_transform_plugin() -> String {
    let deps = std::env::current_exe()
        .expect("current_exe")
        .parent()
        .expect("deps dir")
        .to_path_buf();
    let p = deps.join(format!(
        "{}sz_record_transform_example.{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_EXTENSION
    ));
    assert!(p.exists(), "example transform plugin not built at {p:?}");
    p.to_string_lossy().into_owned()
}

fn sleep_plugin_env(sleep_ms: u64) -> Vec<(&'static str, String)> {
    vec![
        (
            "SENZING_RECORD_TRANSFORM_PLUGIN",
            example_transform_plugin(),
        ),
        (
            "SENZING_RECORD_TRANSFORM_CONFIG",
            format!(r#"{{"SLEEP_MS":{sleep_ms}}}"#),
        ),
    ]
}

// ==========================================================================
// Tests
// ==========================================================================

/// Data + AmqpValue bodies load; an engine reject and an unparseable body are
/// dead-lettered with the reason properties; the driver keeps running.
#[test]
fn e2e_load_and_dead_letter() {
    let Some(cs) = gate() else { return };
    let rt = rt();
    let mut c = rt.block_on(Client::connect(&cs, LOAD_QUEUE));
    rt.block_on(purge(&mut c, LOAD_QUEUE));
    let mut bodies: Vec<Body<Value>> = (0..10).map(|i| data(&record(&i.to_string()))).collect();
    bodies.push(text(&record("value")));
    bodies.push(data("{not json"));
    bodies.push(data(
        r#"{"DATA_SOURCE":"NO_SUCH_SOURCE","RECORD_ID":"SBIT-bad","NAME_FULL":"x"}"#,
    ));
    rt.block_on(c.send(LOAD_QUEUE, &bodies));

    let d = Driver::spawn(&cs, LOAD_QUEUE, 2, &fast_stats());
    let done = d.wait_for(Duration::from_secs(60), |o| {
        o.contains(r#""adds":11,"#) && o.contains(r#""adds_rejected":2,"#)
    });
    // Give the last settles time to land, then stop.
    std::thread::sleep(Duration::from_secs(2));
    let (code, out) = d.stop();
    assert!(done, "driver did not process the queue in time\n{out}");
    assert_eq!(code, Some(0), "{out}");
    assert_eq!(final_totals(&out), (11, 2), "{out}");

    let left = rt.block_on(c.drain(LOAD_QUEUE, Duration::from_secs(3)));
    assert!(left.is_empty(), "queue not drained: {} left", left.len());
    let dlq = rt.block_on(c.drain(
        &format!("{LOAD_QUEUE}/$DeadLetterQueue"),
        Duration::from_secs(3),
    ));
    assert_eq!(dlq.len(), 2, "two dead-lettered messages");
    let malformed = dlq
        .iter()
        .find(|m| body_text(m) == "{not json")
        .expect("malformed body dead-lettered verbatim");
    assert_eq!(
        app_prop(malformed, "DeadLetterReason").as_deref(),
        Some("MalformedRecord")
    );
    assert!(
        app_prop(malformed, "DeadLetterErrorDescription")
            .is_some_and(|d| d.starts_with("malformed record:")),
        "{:?}",
        malformed.application_properties
    );
    let rejected = dlq
        .iter()
        .find(|m| body_text(m).contains("SBIT-bad"))
        .expect("engine reject dead-lettered verbatim");
    assert_eq!(
        app_prop(rejected, "DeadLetterReason").as_deref(),
        Some("SenzingRejected")
    );
    assert!(
        app_prop(rejected, "DeadLetterErrorDescription").is_some_and(|d| d.contains("SENZ")),
        "{:?}",
        rejected.application_properties
    );
    rt.block_on(c.close());
}

/// Records that take 25 s on a queue with a 10 s lockDuration keep their
/// locks (renewed every ~5 s) and are loaded exactly once.
#[test]
fn e2e_lock_renewal_keeps_long_records_locked() {
    let Some(cs) = gate() else { return };
    let rt = rt();
    let mut c = rt.block_on(Client::connect(&cs, RENEW_QUEUE));
    rt.block_on(purge(&mut c, RENEW_QUEUE));
    let bodies: Vec<Body<Value>> = (0..2)
        .map(|i| data(&record(&format!("renew-{i}"))))
        .collect();
    rt.block_on(c.send(RENEW_QUEUE, &bodies));

    let mut env = sleep_plugin_env(25_000);
    env.extend(fast_stats());
    let d = Driver::spawn(&cs, RENEW_QUEUE, 2, &env);
    let finished = d.wait_for(Duration::from_secs(90), |o| o.contains(r#""adds":2,"#));
    std::thread::sleep(Duration::from_secs(2));
    let (code, out) = d.stop();
    assert!(finished, "records not loaded in time\n{out}");
    // 25 s on a 10 s lock: renewed at ~5 s intervals, 4+ times per record.
    let renewals = out.matches("renewed the lock on delivery").count();
    assert!(renewals >= 6, "only {renewals} lock renewals\n{out}");
    assert_eq!(code, Some(0), "{out}");
    assert_eq!(final_totals(&out), (2, 0), "loaded exactly once\n{out}");
    assert!(!out.contains("lock lost"), "{out}");

    let left = rt.block_on(c.drain(RENEW_QUEUE, Duration::from_secs(3)));
    assert!(
        left.is_empty(),
        "redelivered or not completed: {}",
        left.len()
    );
    let dlq = rt.block_on(c.drain(
        &format!("{RENEW_QUEUE}/$DeadLetterQueue"),
        Duration::from_secs(2),
    ));
    assert!(dlq.is_empty(), "nothing dead-lettered");
    rt.block_on(c.close());
}

/// SIGTERM with records still in workers and queued: every unsettled
/// delivery is released back to the queue (never dead-lettered, never
/// counted), and the emulator leaves DeliveryCount unchanged for `released`.
#[test]
fn e2e_shutdown_releases_unsettled_deliveries() {
    let Some(cs) = gate() else { return };
    let rt = rt();
    let mut c = rt.block_on(Client::connect(&cs, RELEASE_QUEUE));
    rt.block_on(purge(&mut c, RELEASE_QUEUE));
    let bodies: Vec<Body<Value>> = (0..4).map(|i| data(&record(&format!("rel-{i}")))).collect();
    rt.block_on(c.send(RELEASE_QUEUE, &bodies));

    let d = Driver::spawn(&cs, RELEASE_QUEUE, 2, &sleep_plugin_env(20_000));
    // Engine init + connect; then all 4 are in flight (2 in workers, 2 queued).
    std::thread::sleep(Duration::from_secs(8));
    let (code, out) = d.stop();
    assert_eq!(code, Some(0), "{out}");
    let (adds, rejected) = final_totals(&out);
    assert_eq!(rejected, 0, "{out}");

    let back = rt.block_on(c.drain(RELEASE_QUEUE, Duration::from_secs(3)));
    assert_eq!(
        back.len() + adds,
        4,
        "every unfinished record is back\n{out}"
    );
    for m in &back {
        let count = m.header.as_ref().map_or(0, |h| h.delivery_count);
        assert_eq!(count, 0, "released must not count as a delivery attempt");
    }
    let dlq = rt.block_on(c.drain(
        &format!("{RELEASE_QUEUE}/$DeadLetterQueue"),
        Duration::from_secs(2),
    ));
    assert!(dlq.is_empty(), "nothing dead-lettered");
    rt.block_on(c.close());
}
