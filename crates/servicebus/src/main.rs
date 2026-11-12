//! Entry point for the combined Azure Service Bus load + redo Senzing driver.
//!
//! The Service Bus analogue of `sz_activemq_combined_consumer`: it ingests
//! records from a Service Bus queue over AMQP 1.0 (`fe2o3-amqp`; CBS/SAS auth,
//! peek-lock with lock renewal), sharing the entire engine-processing core
//! (worker pool, redo fetcher, stats, live config reload, file loader) via
//! `sz_combined_consumer_core`. Neither `lapin` nor the AWS SDK is compiled
//! into this binary.
//!
//! Modes:
//! * `--file` — file loader + redo share (shared core path; no AMQP, no tokio).
//! * redo% = 100 — pure `std::thread` redoer (shared core path; no AMQP).
//! * redo% < 100 — the shared core queue loop with this bin's
//!   `ServiceBusTransport`.
//!
//! Env names are drop-in compatible with the Python Service Bus consumer
//! (`SENZING_AZURE_QUEUE_CONNECTION_STRING`, `SENZING_AZURE_QUEUE_NAME`).

use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use sz_rust_sdk::prelude::*;

use sz_combined_consumer_core::config::{
    CommonArgs, Config, DEFAULT_MQ_RECHECK_SECS, engine_config_from_env, resolve_prefetch,
};
use sz_combined_consumer_core::{queue_run, runtime};

mod conn_str;
mod lock;
mod mgmt;
mod sas;
mod servicebus;

use conn_str::{DEFAULT_TOKEN_TTL, ServiceBusParams, parse_connection_string};

/// Instance/module name passed to the Senzing environment; also the AMQP
/// container id and receiver link name prefix.
pub(crate) const INSTANCE_NAME: &str = "sz_servicebus_combined_consumer";

/// Default lock-renew threshold: well inside the 5-minute maximum
/// lockDuration (a shorter lock is renewed at half its remaining time anyway).
const DEFAULT_LOCK_RENEW_SECS: u64 = 120;

/// Shortest accepted SAS token lifetime (the refresh runs at half of it).
const MIN_TOKEN_TTL_SECS: u64 = 10;

#[derive(Parser, Debug, Clone)]
#[command(
    name = "sz_servicebus_combined_consumer",
    version = sz_combined_consumer_core::VERSION,
    about = "Combined Senzing driver (Azure Service Bus, AMQP 1.0): add_record from a Service \
             Bus queue and process_redo_record, split by one redo%% knob (0 = pure loader, \
             100 = pure redoer)",
    long_about = None
)]
struct Args {
    /// Service Bus connection string
    /// (`Endpoint=sb://<ns>.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=...`,
    /// optional `EntityPath`; `UseDevelopmentEmulator=true` for the local
    /// emulator). Required when redo% < 100 and --file is not set. Never
    /// logged.
    #[arg(
        long = "connection-string",
        env = "SENZING_AZURE_QUEUE_CONNECTION_STRING",
        hide_env_values = true
    )]
    connection_string: Option<String>,

    /// Source queue name (or the connection string's EntityPath).
    #[arg(short = 'q', long = "queue", env = "SENZING_AZURE_QUEUE_NAME")]
    queue: Option<String>,

    /// Total in-flight cap: deliveries received but not yet settled (the
    /// AMQP link credit is kept at this minus the unsettled count). Defaults to
    /// threads + 2. Below threads is raised to threads (with a warning).
    #[arg(long = "prefetch", env = "SENZING_PREFETCH")]
    prefetch: Option<usize>,

    /// Cadence of the diagnostic MQ depth probe. Service Bus exposes no queue
    /// depth over AMQP, so the depth is reported as unknown; kept for flag
    /// parity with the other backends.
    #[arg(
        long = "mq-recheck-secs",
        env = "SENZING_MQ_RECHECK_SECONDS",
        default_value_t = DEFAULT_MQ_RECHECK_SECS
    )]
    mq_recheck_secs: u64,

    /// Seconds to keep retrying an unreachable namespace at startup (0 =
    /// never give up). A rejected token, an unknown queue or a bad connection
    /// string still fail at once.
    #[arg(
        long = "reconnect-timeout",
        env = "SENZING_SB_RECONNECT_TIMEOUT",
        default_value_t = 300
    )]
    reconnect_timeout: u64,

    /// Renew the peek-lock of a delivery still unsettled this many seconds
    /// after it was received (and again every this many seconds); sooner if
    /// the broker's lock expires earlier (half the remaining lock). Keep it
    /// below the queue's lockDuration (max 300).
    #[arg(
        long = "lock-renew-secs",
        env = "SENZING_SB_LOCK_RENEW_SECS",
        default_value_t = DEFAULT_LOCK_RENEW_SECS
    )]
    lock_renew_secs: u64,

    /// Lifetime of each SAS token (seconds); it is re-put at half of it.
    /// Hidden: a tuning/test knob (default 3600, the Azure SDK's).
    #[arg(
        long = "sas-token-ttl-secs",
        env = "SENZING_SB_SAS_TOKEN_TTL_SECS",
        default_value_t = DEFAULT_TOKEN_TTL.as_secs(),
        hide = true
    )]
    sas_token_ttl_secs: u64,

    #[command(flatten)]
    common: CommonArgs,
}

fn main() -> ExitCode {
    runtime::init_logging();

    // Every check (engine JSON, shared + Service Bus params) runs before Sz_init.
    let args = Args::parse();
    let (config, params) = match engine_config_from_env().and_then(|ec| build(&args, ec)) {
        Ok(built) => built,
        Err(msg) => {
            eprintln!("{msg}");
            return ExitCode::from(1);
        }
    };

    let env = match runtime::init_environment(INSTANCE_NAME, &config) {
        Ok(e) => e,
        Err(code) => return code,
    };

    match params {
        Some(params) => {
            let connect_timeout =
                (args.reconnect_timeout > 0).then(|| Duration::from_secs(args.reconnect_timeout));
            let lock_renew = Duration::from_secs(args.lock_renew_secs);
            run_servicebus(config, params, env, connect_timeout, lock_renew)
        }
        None if config.input_file.is_some() => runtime::run_file_loader(&config, env),
        None => runtime::run_pure_redoer(&config, env),
    }
}

/// Resolves and validates everything before engine init: the shared config,
/// the depth-probe cadence, the in-flight cap and — only on the Service Bus
/// load path (redo% < 100, not file mode) — the connection params. Pure (no
/// engine).
fn build(args: &Args, engine_config: String) -> Result<(Config, Option<ServiceBusParams>), String> {
    let mut config = Config::from_common(&args.common, engine_config)?;
    config.mq_recheck_secs = args.mq_recheck_secs;
    config.prefetch = resolve_prefetch(
        args.prefetch,
        config.threads,
        config.threads.saturating_add(2),
    );
    let load_path = config.input_file.is_none() && config.redo_percent < 100;
    if !load_path {
        if config.input_file.is_some() && (args.connection_string.is_some() || args.queue.is_some())
        {
            eprintln!(
                "warning: --file is set; ignoring --connection-string/--queue (file input mode)"
            );
        }
        return Ok((config, None));
    }
    if args.lock_renew_secs == 0 {
        return Err("--lock-renew-secs / SENZING_SB_LOCK_RENEW_SECS must be >= 1".to_string());
    }
    if args.lock_renew_secs >= 300 {
        eprintln!(
            "warning: SENZING_SB_LOCK_RENEW_SECS={} is not below the 300 s maximum Service Bus \
             lockDuration; locks are still renewed at half their remaining time",
            args.lock_renew_secs
        );
    }
    let conn = args
        .connection_string
        .as_deref()
        .filter(|s| !s.is_empty())
        .ok_or(
            "No Service Bus connection string provided (use --connection-string or \
             SENZING_AZURE_QUEUE_CONNECTION_STRING); required when redo% < 100",
        )?;
    if args.sas_token_ttl_secs < MIN_TOKEN_TTL_SECS {
        return Err(format!(
            "SENZING_SB_SAS_TOKEN_TTL_SECS must be >= {MIN_TOKEN_TTL_SECS}"
        ));
    }
    let mut params = parse_connection_string(conn, args.queue.as_deref())?;
    params.token_ttl = Duration::from_secs(args.sas_token_ttl_secs);
    Ok((config, Some(params)))
}

/// redo% < 100: tokio runtime for the AMQP I/O layer only (engine calls run on
/// the dedicated `sz-worker` OS threads), then the shared use-after-free exit
/// discipline.
fn run_servicebus(
    config: Config,
    params: ServiceBusParams,
    env: Arc<SzEnvironmentCore>,
    connect_timeout: Option<Duration>,
    lock_renew: Duration,
) -> ! {
    queue_run::run_queue_mode(
        servicebus::run(&config, params, env, connect_timeout, lock_renew),
        &queue_run::ExitLogs {
            leak: "skipping Senzing environment destroy: a worker may still be in an \
                   engine call (leak-on-exit); forcing process exit",
            run_failed: "Service Bus run() failed; leak-on-exit, forcing process exit",
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const CS: &str = "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=r;\
                      SharedAccessKey=secret";

    /// Parses the binary's args from a CLI vector (clap); a broker field not
    /// given on that vector is cleared so an inherited SENZING_AZURE_* env
    /// cannot leak in.
    fn args(extra: &[&str]) -> Args {
        let mut argv = vec!["sz_servicebus_combined_consumer"];
        argv.extend_from_slice(extra);
        let mut a = Args::parse_from(argv);
        let given = |flag: &str| extra.contains(&flag);
        if !given("--connection-string") {
            a.connection_string = None;
        }
        if !given("-q") {
            a.queue = None;
        }
        if !given("--lock-renew-secs") {
            a.lock_renew_secs = DEFAULT_LOCK_RENEW_SECS;
        }
        if !given("--sas-token-ttl-secs") {
            a.sas_token_ttl_secs = DEFAULT_TOKEN_TTL.as_secs();
        }
        a
    }

    fn params(extra: &[&str]) -> Result<ServiceBusParams, String> {
        build(&args(extra), "{}".into()).map(|(_, p)| p.expect("load path builds params"))
    }

    #[test]
    fn build_requires_connection_string_and_queue_on_the_load_path() {
        let err = params(&["-q", "q"]).unwrap_err();
        assert!(err.contains("connection string"), "{err}");
        let err = params(&["--connection-string", CS]).unwrap_err();
        assert!(err.contains("No queue"), "{err}");
        let p = params(&["--connection-string", CS, "-q", "input"]).unwrap();
        assert_eq!(
            (p.host.as_str(), p.queue.as_str()),
            ("ns.servicebus.windows.net", "input")
        );
    }

    #[test]
    fn build_validates_before_engine_init() {
        let err = params(&["--connection-string", "nonsense", "-q", "q"]).unwrap_err();
        assert!(err.contains("malformed"), "{err}");
        let err = params(&[
            "--connection-string",
            CS,
            "-q",
            "q",
            "--lock-renew-secs",
            "0",
        ])
        .unwrap_err();
        assert!(err.contains("SENZING_SB_LOCK_RENEW_SECS"), "{err}");
        let err = params(&[
            "--connection-string",
            CS,
            "-q",
            "q",
            "--sas-token-ttl-secs",
            "5",
        ])
        .unwrap_err();
        assert!(err.contains("SENZING_SB_SAS_TOKEN_TTL_SECS"), "{err}");
        let p = params(&[
            "--connection-string",
            CS,
            "-q",
            "q",
            "--sas-token-ttl-secs",
            "60",
        ])
        .unwrap();
        assert_eq!(p.token_ttl, Duration::from_secs(60));
        assert_eq!(
            params(&["--connection-string", CS, "-q", "q"])
                .unwrap()
                .token_ttl,
            DEFAULT_TOKEN_TTL
        );
    }

    #[test]
    fn build_skips_broker_params_off_the_load_path() {
        for extra in [&["--file", "in.jsonl"][..], &["--redo-percent", "100"][..]] {
            let (_, p) = build(&args(extra), "{}".into()).expect("valid without a connection");
            assert!(p.is_none());
        }
    }

    #[test]
    fn prefetch_is_the_total_cap_defaulting_to_threads_plus_two() {
        let prefetch = |extra: &[&str]| {
            let mut a = vec![
                "--connection-string",
                CS,
                "-q",
                "q",
                "--threads-per-process",
                "4",
            ];
            a.extend_from_slice(extra);
            build(&args(&a), "{}".into()).expect("valid").0.prefetch
        };
        assert_eq!(prefetch(&[]), 6, "threads + 2");
        assert_eq!(prefetch(&["--prefetch", "9"]), 9, "the total, not extra");
        assert_eq!(prefetch(&["--prefetch", "2"]), 4, "raised to threads");
    }

    #[test]
    fn build_sets_mq_recheck_secs_and_leaves_rabbit_fields_inert() {
        let (c, _) = build(
            &args(&[
                "--connection-string",
                CS,
                "-q",
                "q",
                "--mq-recheck-secs",
                "1",
            ]),
            "{}".into(),
        )
        .unwrap();
        assert_eq!(c.mq_recheck_secs, 1);
        assert_eq!((c.url, c.queue), (None, None));
    }
}
