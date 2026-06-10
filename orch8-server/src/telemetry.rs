//! OpenTelemetry OTLP trace export.
//!
//! Enabled ONLY when `ORCH8_OTLP_ENDPOINT` (config: `[telemetry] otlp_endpoint`)
//! is set. When unset, [`init`] returns an inert guard and the tracing stack is
//! byte-identical to a build without this module — no OpenTelemetry layer, no runtime
//! cost. Export failures (collector down, wrong port…) are non-fatal background
//! noise: the batch exporter retries/logs internally and never blocks steps.
//!
//! Spans exported include `orch8.step` (one per step-handler invocation, see
//! `orch8_engine::handlers::step`) which carries the `gen_ai.client.inference`
//! events emitted by the `llm_call` handler — pipe them to Langfuse / Datadog /
//! Grafana Tempo via any OTLP collector.
//!
//! The whole module is feature-gated (`otlp`, on by default). With
//! `--no-default-features` it compiles down to a no-op guard.

use orch8_types::config::TelemetryConfig;

#[cfg(feature = "otlp")]
pub use enabled::{init, OtelGuard};

#[cfg(not(feature = "otlp"))]
pub use disabled::{init, OtelGuard};

#[cfg(feature = "otlp")]
mod enabled {
    use anyhow::Context as _;
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_otlp::WithExportConfig as _;
    use opentelemetry_sdk::trace::SdkTracerProvider;
    use tracing_subscriber::Layer as _;

    use super::TelemetryConfig;

    /// Tracer name recorded on exported spans (`otel.library.name`).
    const TRACER_NAME: &str = "orch8-server";

    /// Per-layer filter type for the OpenTelemetry layer: drops events/spans emitted by
    /// the export pipeline itself (opentelemetry/tonic/h2/hyper) so a failing
    /// exporter can't feed its own error logs back into the exporter forever.
    type OtelLayer<S> = tracing_subscriber::filter::Filtered<
        tracing_opentelemetry::OpenTelemetryLayer<S, opentelemetry_sdk::trace::Tracer>,
        tracing_subscriber::filter::FilterFn<fn(&tracing::Metadata<'_>) -> bool>,
        S,
    >;

    fn not_export_pipeline(meta: &tracing::Metadata<'_>) -> bool {
        let target = meta.target();
        !(target.starts_with("opentelemetry")
            || target.starts_with("tonic")
            || target.starts_with("h2")
            || target.starts_with("hyper"))
    }

    /// Holds the tracer provider so the batch exporter can be flushed on
    /// shutdown. Inert (all methods no-ops) when no endpoint is configured.
    pub struct OtelGuard {
        provider: Option<SdkTracerProvider>,
    }

    impl OtelGuard {
        /// Whether OTLP export is active.
        pub fn is_enabled(&self) -> bool {
            self.provider.is_some()
        }

        /// Build the `tracing-opentelemetry` layer bridging tracing spans into
        /// the OTLP pipeline. `None` when export is disabled — `Option<Layer>`
        /// composes onto the subscriber as a no-op.
        pub fn layer<S>(&self) -> Option<OtelLayer<S>>
        where
            S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
        {
            self.provider.as_ref().map(|provider| {
                tracing_opentelemetry::layer()
                    .with_tracer(provider.tracer(TRACER_NAME))
                    .with_filter(tracing_subscriber::filter::filter_fn(
                        not_export_pipeline as fn(&tracing::Metadata<'_>) -> bool,
                    ))
            })
        }

        /// Flush and shut down the tracer provider. Blocking under the hood
        /// (drains the batch queue), so it runs on the blocking pool. Export
        /// errors are logged, never propagated — a dead collector must not
        /// turn a clean shutdown into a failure.
        pub async fn shutdown(self) {
            let Some(provider) = self.provider else {
                return;
            };
            match tokio::task::spawn_blocking(move || provider.shutdown()).await {
                Ok(Ok(())) => tracing::debug!("OTLP tracer provider shut down"),
                Ok(Err(e)) => tracing::warn!(error = %e, "OTLP tracer provider shutdown error"),
                Err(e) => tracing::warn!(error = %e, "OTLP shutdown task failed"),
            }
        }
    }

    /// Build the OTLP pipeline from config. Returns an inert guard when no
    /// endpoint is configured. Fails only on configuration-level errors (bad
    /// endpoint syntax) — runtime export failures are background noise.
    ///
    /// Must be called from within a Tokio runtime (the tonic exporter grabs
    /// the ambient runtime handle for its gRPC channel).
    pub fn init(config: &TelemetryConfig) -> anyhow::Result<OtelGuard> {
        if !config.otlp_enabled() {
            return Ok(OtelGuard { provider: None });
        }
        // `EngineConfig::validate()` already rejects non-grpc protocols at
        // startup; double-check here so direct callers (tests) fail loudly too.
        anyhow::ensure!(
            matches!(config.otlp_protocol.as_str(), "grpc" | ""),
            "telemetry.otlp_protocol: only \"grpc\" is supported (got {:?})",
            config.otlp_protocol
        );

        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(&config.otlp_endpoint)
            .build()
            .context("failed to build OTLP span exporter")?;

        // `Resource::builder()` includes the standard env detectors, so
        // OTEL_RESOURCE_ATTRIBUTES is honored. service.name defaults to
        // "orch8-server" but yields to an explicit OTEL_SERVICE_NAME.
        let mut resource = opentelemetry_sdk::Resource::builder().with_attribute(
            opentelemetry::KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
        );
        if std::env::var("OTEL_SERVICE_NAME").is_err() {
            resource = resource.with_service_name("orch8-server");
        }

        let provider = SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .with_resource(resource.build())
            .build();

        // NOTE: no tracing::info! here — init runs BEFORE the subscriber is
        // installed (the subscriber needs this guard's layer). main logs the
        // "OTLP trace export enabled" line after init_logging.
        Ok(OtelGuard {
            provider: Some(provider),
        })
    }
}

#[cfg(not(feature = "otlp"))]
mod disabled {
    #![allow(clippy::unused_self, clippy::unused_async)]

    use super::TelemetryConfig;

    /// No-op guard for builds without the `otlp` feature.
    pub struct OtelGuard;

    impl OtelGuard {
        pub fn is_enabled(&self) -> bool {
            false
        }

        /// Identity layer placeholder — `None` composes as a no-op.
        /// Non-generic (unlike the enabled variant): `Identity` implements
        /// `Layer<S>` for every subscriber, so no type parameter is needed
        /// and call-site inference stays happy.
        pub fn layer(&self) -> Option<tracing_subscriber::layer::Identity> {
            None
        }

        pub async fn shutdown(self) {}
    }

    /// Without the `otlp` feature an endpoint cannot be honored — warn instead
    /// of silently dropping traces the operator asked for.
    // Result-wrapped for signature parity with the `otlp`-enabled variant.
    #[allow(clippy::unnecessary_wraps)]
    pub fn init(config: &TelemetryConfig) -> anyhow::Result<OtelGuard> {
        if config.otlp_enabled() {
            eprintln!(
                "WARNING: ORCH8_OTLP_ENDPOINT is set but this binary was built without the \
                 `otlp` feature — trace export is disabled"
            );
        }
        Ok(OtelGuard)
    }
}

#[cfg(all(test, feature = "otlp"))]
mod tests {
    use super::*;

    #[test]
    fn init_without_endpoint_is_inert() {
        type Registry = tracing_subscriber::Registry;
        let guard = init(&TelemetryConfig::default()).expect("init must succeed");
        assert!(!guard.is_enabled());
        assert!(guard.layer::<Registry>().is_none());
    }

    #[test]
    fn init_rejects_non_grpc_protocol() {
        let cfg = TelemetryConfig {
            otlp_endpoint: "http://127.0.0.1:4317".into(),
            otlp_protocol: "http".into(),
        };
        // Outside a runtime this must fail on the protocol check, BEFORE any
        // exporter construction.
        let Err(err) = init(&cfg) else {
            panic!("non-grpc protocol must be rejected");
        };
        assert!(err.to_string().contains("otlp_protocol"));
    }

    /// Smoke test: subscriber initializes with the OTLP layer pointed at a
    /// non-listening local address. Initialization must not panic or block;
    /// export failures are non-fatal background noise swallowed by the batch
    /// exporter. Shutdown (which flushes into the dead endpoint) must also
    /// return without panicking.
    #[tokio::test(flavor = "multi_thread")]
    async fn init_with_unreachable_endpoint_does_not_panic_or_block() {
        use tracing_subscriber::layer::SubscriberExt as _;

        let cfg = TelemetryConfig {
            // Port 1 is essentially guaranteed closed → immediate refusal,
            // exercising the export-failure path without slow timeouts.
            otlp_endpoint: "http://127.0.0.1:1".into(),
            otlp_protocol: "grpc".into(),
        };
        let guard = init(&cfg).expect("init must succeed even if collector is down");
        assert!(guard.is_enabled());

        // Compose a scoped subscriber with the OTel layer and emit a span
        // through it — must not panic even though every export will fail.
        let subscriber = tracing_subscriber::registry().with(guard.layer());
        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("smoke", test = true);
            let _entered = span.enter();
            tracing::info!("event inside exported span");
        });

        // Flush against the dead endpoint: errors are logged, not returned.
        guard.shutdown().await;
    }
}
