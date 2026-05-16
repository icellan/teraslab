//! Observability subscriber + OTLP exporter bootstrap (Phase 4).
//!
//! This module composes the `tracing` subscriber used by the server binary
//! and (optionally) wires an OpenTelemetry OTLP exporter behind it.
//!
//! # Design
//!
//! * Always install a JSON `fmt` layer so structured logs continue to flow
//!   (behavior inherited from Phase 3).
//! * When an OTLP endpoint is configured, additionally install a
//!   [`tracing_opentelemetry::OpenTelemetryLayer`] backed by a
//!   `BatchSpanProcessor`. The batch processor owns a background queue and
//!   a dedicated tokio runtime thread so the hot path never blocks on
//!   export.
//! * Sampling uses
//!   `ParentBased(TraceIdRatioBased(trace_sampling_ratio))` — simple,
//!   predictable, and well-supported by the opentelemetry_sdk 0.31 API.
//!   A "force-sample-on-error" sampler is NOT trivial in the current
//!   `ShouldSample` trait (the decision is made at span start, before any
//!   event is recorded), so we document the limitation here rather than
//!   ship a misleading implementation. Clients that need all errors can
//!   set `trace_sampling_ratio = 1.0`.
//! * Hot-path spans are declared at `level = "debug"` and the default
//!   `EnvFilter` is `info`, so hot-path spans short-circuit before any
//!   field is evaluated. This is load-bearing for the Phase 4 perf budget
//!   (≤ 5% throughput regression, ≤ 50 ns p99 per op).
//!
//! # Environment
//!
//! All fields under the `[observability]` TOML section can be overridden
//! by environment variables at startup — see [`ObservabilityConfig::apply_env_overrides`]:
//!
//! | TOML field              | Env var                         |
//! |-------------------------|---------------------------------|
//! | `otlp_endpoint`         | `TERASLAB_OTLP_ENDPOINT`        |
//! | `trace_sampling_ratio`  | `TERASLAB_TRACE_SAMPLING_RATIO` |
//! | `service_name`          | `TERASLAB_SERVICE_NAME`         |

use opentelemetry::KeyValue;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
use std::sync::atomic::{AtomicBool, Ordering};
use thiserror::Error;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry};

// Re-export the provider so the shutdown hook in `bin/server.rs` can flush it.
pub use opentelemetry_sdk::trace::SdkTracerProvider as OtelTracerProvider;

/// Tracks whether an OTLP exporter has been installed in the current
/// process. Used by tests to verify the "exporter disabled when endpoint
/// absent" contract without introspecting tokio runtime threads.
pub static OTLP_EXPORTER_STARTED: AtomicBool = AtomicBool::new(false);

/// Errors produced when parsing or installing the observability stack.
#[derive(Error, Debug)]
pub enum ObservabilityError {
    /// A configured value failed validation (e.g. sampling ratio outside [0, 1]).
    #[error("invalid observability config: {0}")]
    InvalidConfig(String),
    /// The OTLP exporter could not be constructed (e.g. bad endpoint).
    #[error("OTLP exporter: {0}")]
    OtlpExporter(String),
}

/// Observability configuration (Phase 4).
///
/// Loaded from the `[observability]` section of the server TOML; each
/// field can be individually overridden by a `TERASLAB_*` environment
/// variable. When `otlp_endpoint` is `None` the OTLP exporter is disabled
/// entirely — no background task is spawned and [`init_subscriber`] is a
/// cheap fmt-layer-only installation.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(default)]
pub struct ObservabilityConfig {
    /// gRPC OTLP endpoint (e.g. `http://localhost:4317`). When absent,
    /// OTLP export is disabled.
    pub otlp_endpoint: Option<String>,
    /// Head sampling ratio in `[0.0, 1.0]`. Default 0.01 (1 %).
    pub trace_sampling_ratio: f64,
    /// Resource `service.name`. Defaults to `"teraslab"` when absent.
    pub service_name: Option<String>,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            otlp_endpoint: None,
            trace_sampling_ratio: 0.01,
            service_name: None,
        }
    }
}

impl ObservabilityConfig {
    /// Env var name for `otlp_endpoint`.
    pub const ENV_OTLP_ENDPOINT: &'static str = "TERASLAB_OTLP_ENDPOINT";
    /// Env var name for `trace_sampling_ratio`.
    pub const ENV_SAMPLING_RATIO: &'static str = "TERASLAB_TRACE_SAMPLING_RATIO";
    /// Env var name for `service_name`.
    pub const ENV_SERVICE_NAME: &'static str = "TERASLAB_SERVICE_NAME";

    /// Apply environment-variable overrides on top of the TOML-loaded values.
    ///
    /// * `TERASLAB_OTLP_ENDPOINT` — empty string clears the endpoint (disables OTLP).
    /// * `TERASLAB_TRACE_SAMPLING_RATIO` — must parse as `f64`; invalid values return an error.
    /// * `TERASLAB_SERVICE_NAME` — empty string clears the service name (defaults to `"teraslab"`).
    ///
    /// F-G6-026: every observed override is logged at `info` so operators
    /// can confirm at startup whether their env-var-driven config landed.
    /// A typo (e.g. `TERASLAB_OTLP_ENDPONIT`) leaves the field untouched
    /// and the absence of the corresponding log line is the signal.
    pub fn apply_env_overrides(&mut self) -> Result<(), ObservabilityError> {
        if let Ok(v) = std::env::var(Self::ENV_OTLP_ENDPOINT) {
            tracing::info!(
                env_var = Self::ENV_OTLP_ENDPOINT,
                value_set = !v.is_empty(),
                "observability: env override applied (otlp_endpoint)",
            );
            self.otlp_endpoint = if v.is_empty() { None } else { Some(v) };
        }
        if let Ok(v) = std::env::var(Self::ENV_SAMPLING_RATIO) {
            let parsed: f64 = v.parse().map_err(|_| {
                ObservabilityError::InvalidConfig(format!(
                    "{}: {v:?} is not a valid f64",
                    Self::ENV_SAMPLING_RATIO
                ))
            })?;
            tracing::info!(
                env_var = Self::ENV_SAMPLING_RATIO,
                value = parsed,
                "observability: env override applied (trace_sampling_ratio)",
            );
            self.trace_sampling_ratio = parsed;
        }
        if let Ok(v) = std::env::var(Self::ENV_SERVICE_NAME) {
            tracing::info!(
                env_var = Self::ENV_SERVICE_NAME,
                value_set = !v.is_empty(),
                "observability: env override applied (service_name)",
            );
            self.service_name = if v.is_empty() { None } else { Some(v) };
        }
        Ok(())
    }

    /// Validate the merged config. Called by the startup validator chain.
    ///
    /// Rejects sampling ratios outside `[0.0, 1.0]` because those are
    /// silently clamped by opentelemetry_sdk and hide operator mistakes.
    pub fn validate(&self) -> Result<(), ObservabilityError> {
        if !(0.0..=1.0).contains(&self.trace_sampling_ratio) {
            return Err(ObservabilityError::InvalidConfig(format!(
                "trace_sampling_ratio = {} must be in [0.0, 1.0]",
                self.trace_sampling_ratio,
            )));
        }
        Ok(())
    }

    /// The effective service name — defaults to `"teraslab"`.
    pub fn effective_service_name(&self) -> &str {
        self.service_name.as_deref().unwrap_or("teraslab")
    }
}

/// Resource attributes applied to every exported span.
///
/// Callers pass `node_id` and `shard_count` because those are owned by
/// the cluster/engine, not the observability module itself.
pub fn build_resource(cfg: &ObservabilityConfig, node_id: u64, shard_count: u32) -> Resource {
    Resource::builder()
        .with_attributes([
            KeyValue::new("service.name", cfg.effective_service_name().to_string()),
            KeyValue::new("service.instance.id", node_id.to_string()),
            KeyValue::new("teraslab.shard_count", i64::from(shard_count)),
            KeyValue::new("teraslab.version", env!("CARGO_PKG_VERSION")),
        ])
        .build()
}

/// Install the global `tracing` subscriber composed with the JSON fmt
/// layer and (optionally) an OTLP tracing layer.
///
/// Returns an `Option<SdkTracerProvider>` — `Some` when OTLP is enabled,
/// so the caller can flush it on shutdown; `None` when it is disabled.
///
/// Safe to call multiple times in the same process (subsequent calls are
/// best-effort no-ops; the previously installed global subscriber is
/// retained). This matches the Phase 3 behavior.
pub fn init_subscriber(
    cfg: &ObservabilityConfig,
    node_id: u64,
    shard_count: u32,
) -> Result<Option<SdkTracerProvider>, ObservabilityError> {
    cfg.validate()?;

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let fmt_layer = tracing_subscriber::fmt::Layer::new()
        .json()
        .with_current_span(true)
        .with_span_list(false);

    match cfg.otlp_endpoint.as_deref() {
        Some(endpoint) => {
            let resource = build_resource(cfg, node_id, shard_count);
            let provider = build_otlp_provider(endpoint, cfg.trace_sampling_ratio, resource)?;
            let tracer = provider.tracer(cfg.effective_service_name().to_string());
            let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

            let subscriber = Registry::default()
                .with(filter)
                .with(fmt_layer)
                .with(otel_layer);
            let _ = subscriber.try_init();

            OTLP_EXPORTER_STARTED.store(true, Ordering::SeqCst);
            Ok(Some(provider))
        }
        None => {
            let subscriber = Registry::default().with(filter).with(fmt_layer);
            let _ = subscriber.try_init();
            Ok(None)
        }
    }
}

/// Build a `SdkTracerProvider` with a batch span processor exporting via
/// gRPC OTLP to `endpoint`.
///
/// F-G6-012: emit a startup warning when the endpoint scheme is `http://`
/// (plaintext gRPC). Span attributes today are limited (see F-G6-013
/// positive verification), but the embedded W3C trace context flows on
/// the cluster wire and operator-deployed collectors should encrypt the
/// transport. We do not refuse to construct the exporter — that lives
/// behind a future opt-in `require_tls` flag — but the operator must see
/// the weakening.
fn build_otlp_provider(
    endpoint: &str,
    sampling_ratio: f64,
    resource: Resource,
) -> Result<SdkTracerProvider, ObservabilityError> {
    if endpoint.starts_with("http://") {
        tracing::warn!(
            target: "teraslab::security",
            endpoint,
            "OTLP endpoint is plaintext http:// — span attributes and the W3C trace \
             context will travel unencrypted. Prefer https:// (or grpcs://) for \
             production deployments.",
        );
    }
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint.to_string())
        .with_timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| ObservabilityError::OtlpExporter(format!("build exporter: {e}")))?;

    let sampler = Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(sampling_ratio)));

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_sampler(sampler)
        .with_resource(resource)
        .build();

    Ok(provider)
}

/// Flush the OTLP provider on shutdown.
///
/// Blocks up to `timeout` waiting for the batch processor to drain. Logs
/// a warning on timeout; never panics.
pub fn shutdown(provider: &SdkTracerProvider, timeout: std::time::Duration) {
    let (tx, rx) = std::sync::mpsc::channel::<()>();
    let prov = provider.clone();
    std::thread::spawn(move || {
        // `shutdown()` is synchronous in opentelemetry_sdk 0.31 and
        // drains the batch processor before returning. Run it on a helper
        // thread so we can enforce a wall-clock timeout via `recv_timeout`.
        let _ = prov.shutdown();
        let _ = tx.send(());
    });
    match rx.recv_timeout(timeout) {
        Ok(()) => {
            tracing::info!("otlp tracer provider shutdown complete");
        }
        Err(_) => {
            tracing::warn!(
                timeout_ms = timeout.as_millis() as u64,
                "otlp tracer provider shutdown timed out",
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Trace context propagation on the replication wire
// ---------------------------------------------------------------------------

/// Binary-encoded W3C trace context carried in the replication batch header.
///
/// Layout (24 bytes, little-endian agnostic — the IDs are raw bytes):
/// * `[0..16]` — `trace_id`
/// * `[16..24]` — `span_id`
///
/// All-zero bytes indicate "no context" (either tracing is disabled or the
/// current span was not sampled). Decoders MUST treat an all-zero payload
/// as `None`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireTraceContext {
    /// 128-bit trace identifier, raw big-endian bytes.
    pub trace_id: [u8; 16],
    /// 64-bit span identifier, raw big-endian bytes.
    pub span_id: [u8; 8],
}

impl WireTraceContext {
    /// Fixed on-wire size in bytes.
    pub const SIZE: usize = 24;

    /// All-zero sentinel meaning "no trace context."
    pub const ZERO: Self = Self {
        trace_id: [0u8; 16],
        span_id: [0u8; 8],
    };

    /// Returns `true` if every byte is zero — the wire encoding of "no context".
    pub fn is_zero(&self) -> bool {
        self.trace_id.iter().all(|b| *b == 0) && self.span_id.iter().all(|b| *b == 0)
    }

    /// Encode this context into the given 24-byte buffer.
    pub fn write_to(&self, out: &mut [u8; Self::SIZE]) {
        out[..16].copy_from_slice(&self.trace_id);
        out[16..].copy_from_slice(&self.span_id);
    }

    /// Decode a 24-byte slice. Returns `None` when the slice is all zeros.
    ///
    /// F-G6-021: short slices used to panic. Now we return `None` so a
    /// malformed batch header can never crash the receiver thread; the
    /// caller is responsible for distinguishing "no trace context" from
    /// "bad payload" if needed (the wire format treats both the same).
    /// Prefer [`read_from_array`](Self::read_from_array) for callers
    /// that own a `[u8; SIZE]` and want the type system to enforce the
    /// length invariant.
    pub fn read_from(buf: &[u8]) -> Option<Self> {
        if buf.len() != Self::SIZE {
            return None;
        }
        let mut trace_id = [0u8; 16];
        trace_id.copy_from_slice(&buf[..16]);
        let mut span_id = [0u8; 8];
        span_id.copy_from_slice(&buf[16..]);
        let ctx = Self { trace_id, span_id };
        if ctx.is_zero() { None } else { Some(ctx) }
    }

    /// Decode a fixed-length 24-byte array. Type-system guarantees the
    /// length is correct, so this entry point can never fail for length
    /// reasons. Returns `None` only for the all-zero "no context"
    /// sentinel.
    pub fn read_from_array(buf: &[u8; Self::SIZE]) -> Option<Self> {
        let mut trace_id = [0u8; 16];
        trace_id.copy_from_slice(&buf[..16]);
        let mut span_id = [0u8; 8];
        span_id.copy_from_slice(&buf[16..]);
        let ctx = Self { trace_id, span_id };
        if ctx.is_zero() { None } else { Some(ctx) }
    }

    /// Extract the current `tracing` span's OpenTelemetry context, or
    /// return [`None`] when either tracing is disabled or the active
    /// span is not sampled.
    ///
    /// This is the function called by the replication sender immediately
    /// before pushing a batch on the wire. It is a zero-cost no-op when
    /// no subscriber is installed (the `Span::current()` handle is empty).
    pub fn from_current_span() -> Option<Self> {
        use tracing_opentelemetry::OpenTelemetrySpanExt;

        let span = tracing::Span::current();
        let cx = span.context();
        let span_ref = opentelemetry::trace::TraceContextExt::span(&cx);
        let sc = span_ref.span_context();
        if !sc.is_valid() {
            return None;
        }
        if !sc.is_sampled() {
            // Even if the ids are valid, don't propagate unsampled traces —
            // the receiver would otherwise create orphan spans with no
            // siblings to stitch against.
            return None;
        }
        Some(Self {
            trace_id: sc.trace_id().to_bytes(),
            span_id: sc.span_id().to_bytes(),
        })
    }

    /// Convert the wire context back into an OpenTelemetry `SpanContext`
    /// with the `remote` flag set and `sampled = true`.
    ///
    /// Returns `None` when the ids are not well-formed (all-zero bytes).
    pub fn to_span_context(&self) -> Option<opentelemetry::trace::SpanContext> {
        if self.is_zero() {
            return None;
        }
        let trace_id = opentelemetry::trace::TraceId::from_bytes(self.trace_id);
        let span_id = opentelemetry::trace::SpanId::from_bytes(self.span_id);
        if trace_id == opentelemetry::trace::TraceId::INVALID
            || span_id == opentelemetry::trace::SpanId::INVALID
        {
            return None;
        }
        Some(opentelemetry::trace::SpanContext::new(
            trace_id,
            span_id,
            opentelemetry::trace::TraceFlags::SAMPLED,
            true, // remote
            opentelemetry::trace::TraceState::default(),
        ))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_disables_otlp() {
        let cfg = ObservabilityConfig::default();
        assert!(cfg.otlp_endpoint.is_none());
        assert_eq!(cfg.trace_sampling_ratio, 0.01);
        assert_eq!(cfg.effective_service_name(), "teraslab");
    }

    #[test]
    fn validate_rejects_negative_sampling_ratio() {
        let cfg = ObservabilityConfig {
            trace_sampling_ratio: -0.1,
            ..ObservabilityConfig::default()
        };
        let err = cfg.validate().unwrap_err();
        assert!(
            matches!(err, ObservabilityError::InvalidConfig(_)),
            "expected InvalidConfig, got {err:?}"
        );
    }

    #[test]
    fn validate_rejects_sampling_ratio_above_one() {
        let cfg = ObservabilityConfig {
            trace_sampling_ratio: 1.5,
            ..ObservabilityConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_accepts_boundaries() {
        for v in [0.0, 0.5, 1.0] {
            let cfg = ObservabilityConfig {
                trace_sampling_ratio: v,
                ..ObservabilityConfig::default()
            };
            cfg.validate().expect("boundary value accepted");
        }
    }

    #[test]
    fn wire_trace_context_round_trip() {
        let ctx = WireTraceContext {
            trace_id: [0xA1u8; 16],
            span_id: [0xB2u8; 8],
        };
        let mut buf = [0u8; WireTraceContext::SIZE];
        ctx.write_to(&mut buf);
        let decoded = WireTraceContext::read_from(&buf).expect("non-zero");
        assert_eq!(decoded, ctx);
        // And every byte matches what we wrote.
        assert_eq!(buf[..16], ctx.trace_id);
        assert_eq!(buf[16..], ctx.span_id);
    }

    #[test]
    fn wire_trace_context_zero_is_none() {
        let buf = [0u8; WireTraceContext::SIZE];
        assert!(WireTraceContext::read_from(&buf).is_none());
    }

    #[test]
    fn wire_trace_context_to_span_context_sets_remote_and_sampled() {
        let ctx = WireTraceContext {
            trace_id: [
                0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D,
                0x1E, 0x1F,
            ],
            span_id: [0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27],
        };
        let sc = ctx
            .to_span_context()
            .expect("non-zero context should convert");
        assert!(sc.is_sampled());
        assert!(sc.is_remote());
        assert_eq!(sc.trace_id().to_bytes(), ctx.trace_id);
        assert_eq!(sc.span_id().to_bytes(), ctx.span_id);
    }

    #[test]
    fn wire_trace_context_zero_to_span_context_is_none() {
        assert!(WireTraceContext::ZERO.to_span_context().is_none());
    }

    /// When the config has no OTLP endpoint, `init_subscriber` returns
    /// `Ok(None)` and never flips the `OTLP_EXPORTER_STARTED` flag — no
    /// background tokio task is spawned. This test runs before any other
    /// test observes that flag because it keeps exactly one writer.
    #[test]
    fn otlp_layer_disabled_when_endpoint_absent() {
        let cfg = ObservabilityConfig {
            otlp_endpoint: None,
            ..ObservabilityConfig::default()
        };
        // Snapshot — another test may have set the flag; we check that
        // *this* call doesn't advance it when endpoint is absent.
        let before = OTLP_EXPORTER_STARTED.load(Ordering::SeqCst);
        let provider = init_subscriber(&cfg, 1, 16).expect("init succeeds");
        assert!(provider.is_none(), "no provider when endpoint is absent",);
        let after = OTLP_EXPORTER_STARTED.load(Ordering::SeqCst);
        assert_eq!(
            before, after,
            "OTLP_EXPORTER_STARTED must not advance when endpoint is None",
        );
    }

    /// Build an in-process provider using a capturing `SpanExporter`
    /// implementation, push 50 spans, then invoke the shutdown hook and
    /// assert all 50 reached the exporter before the hook returned.
    ///
    /// This proves the `shutdown(provider, timeout)` path from this
    /// module drains the batch processor synchronously — which is the
    /// property operators depend on for graceful shutdowns.
    #[test]
    fn shutdown_flushes_otlp_queue() {
        use opentelemetry::trace::{Tracer as _, TracerProvider as _};
        use opentelemetry_sdk::error::OTelSdkResult;
        use opentelemetry_sdk::trace::{SdkTracerProvider, SpanData, SpanExporter};
        use std::sync::{Arc, Mutex};

        #[derive(Debug, Clone, Default)]
        struct CollectingExporter {
            spans: Arc<Mutex<Vec<String>>>,
        }

        impl SpanExporter for CollectingExporter {
            async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
                let mut lock = self.spans.lock().unwrap();
                for span in batch {
                    lock.push(span.name.into_owned());
                }
                Ok(())
            }
        }

        let exporter = CollectingExporter::default();
        let collected = exporter.spans.clone();

        // Build the provider with a batch processor that has room for all
        // 50 spans so none are dropped on queue backpressure.
        let provider = SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .with_sampler(opentelemetry_sdk::trace::Sampler::AlwaysOn)
            .with_resource(
                opentelemetry_sdk::Resource::builder_empty()
                    .with_attributes([opentelemetry::KeyValue::new(
                        "service.name",
                        "teraslab-test",
                    )])
                    .build(),
            )
            .build();

        let tracer = provider.tracer("teraslab-test");
        for i in 0..50u32 {
            let mut span = tracer.start(format!("test-span-{i}"));
            opentelemetry::trace::Span::end(&mut span);
        }

        // Invoke the shutdown hook with a generous timeout. Synchronous
        // drain is the contract; the timeout is only a backstop.
        shutdown(&provider, std::time::Duration::from_secs(5));

        let seen = collected.lock().unwrap();
        assert_eq!(
            seen.len(),
            50,
            "shutdown hook must drain all 50 spans before returning (saw {})",
            seen.len(),
        );
    }
}
