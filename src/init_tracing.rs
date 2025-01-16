use opentelemetry::global;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::KeyValue;
use tracing_opentelemetry::{MetricsLayer, OpenTelemetryLayer};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Registry;

use tracing_log::LogTracer;

use opentelemetry_sdk::{
    metrics::{MeterProviderBuilder, PeriodicReader, SdkMeterProvider},
    runtime,
    trace::{RandomIdGenerator, Sampler, TracerProvider},
    Resource,
};

use opentelemetry_semantic_conventions::{
    attribute::{SERVICE_NAME, SERVICE_VERSION},
    SCHEMA_URL,
};

pub struct OtelGuard {
    tracer_provider: TracerProvider,
    meter_provider: SdkMeterProvider,
}

// Create a Resource that captures information about the entity for which
// telemetry is recorded.
fn resource() -> Resource {
    Resource::from_schema_url(
        [
            KeyValue::new(SERVICE_NAME, env!("CARGO_PKG_NAME")),
            KeyValue::new(SERVICE_VERSION, env!("CARGO_PKG_VERSION")),
        ],
        SCHEMA_URL,
    )
}

impl Drop for OtelGuard {
    fn drop(&mut self) {
        if let Err(err) = self.tracer_provider.shutdown() {
            eprintln!("{err:?}");
        }
        if let Err(err) = self.meter_provider.shutdown() {
            eprintln!("{err:?}");
        }
    }
}

// Construct MeterProvider for MetricsLayer
fn init_meter_provider() -> SdkMeterProvider {
    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_temporality(opentelemetry_sdk::metrics::Temporality::default())
        .build()
        .unwrap();

    let reader = PeriodicReader::builder(exporter, runtime::Tokio)
        .with_interval(std::time::Duration::from_secs(30))
        .build();

    // For debugging in development
    let stdout_reader = PeriodicReader::builder(
        opentelemetry_stdout::MetricExporter::default(),
        runtime::Tokio,
    )
    .build();

    let meter_provider = MeterProviderBuilder::default()
        .with_resource(resource())
        .with_reader(reader)
        .with_reader(stdout_reader)
        .build();

    global::set_meter_provider(meter_provider.clone());

    meter_provider
}

// Construct TracerProvider for OpenTelemetryLayer
fn init_tracer_provider() -> TracerProvider {
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .build()
        .unwrap();

    TracerProvider::builder()
        // Customize sampling strategy
        .with_sampler(Sampler::ParentBased(Box::new(
            Sampler::TraceIdRatioBased(1.0),
        )))
        .with_id_generator(RandomIdGenerator::default())
        .with_resource(resource())
        .with_batch_exporter(exporter, runtime::Tokio)
        .build()
}

pub fn setup_tracing(_level: log::LevelFilter) -> OtelGuard {
    // Setup tracing-log to emit log records as tracing spans
    LogTracer::init().expect("Failed to set default logger");

    let tracer_provider = init_tracer_provider();
    let meter_provider = init_meter_provider();

    let tracer = tracer_provider.tracer(env!("CARGO_PKG_NAME"));

    let env_filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))
        .unwrap();

    let subscriber = Registry::default()
        .with(tracing_subscriber::fmt::layer())
        // .with(otel_logs_layer)
        .with(OpenTelemetryLayer::new(tracer))
        .with(MetricsLayer::new(meter_provider.clone()))
        .with(env_filter);

    tracing::subscriber::set_global_default(subscriber)
        .expect("Failed to set default tracing subscriber");

    OtelGuard {
        tracer_provider,
        meter_provider,
    }
}
