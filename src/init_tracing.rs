//! Sets up logging and, with the `otel` feature, OpenTelemetry traces and
//! metrics exported over OTLP/gRPC (configured by the standard `OTEL_*`
//! environment variables).

use std::io::IsTerminal;

use tracing_subscriber::EnvFilter;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::layer::SubscriberExt;

#[cfg(feature = "otel")]
use opentelemetry::{KeyValue, trace::TracerProvider as _};
#[cfg(feature = "otel")]
use opentelemetry_sdk::{
    Resource,
    metrics::{PeriodicReader, SdkMeterProvider},
    trace::{RandomIdGenerator, Sampler, SdkTracerProvider},
};
#[cfg(feature = "otel")]
use opentelemetry_semantic_conventions::attribute::SERVICE_VERSION;
#[cfg(feature = "otel")]
use tracing_opentelemetry::OpenTelemetryLayer;

/// The target of this crate's own spans and events.
const CRATE_TARGET: &str = env!("CARGO_CRATE_NAME");

/// Builds the log filter. Everything logs at `info` unless `rust_log` (the
/// value of `RUST_LOG`) says otherwise. If `level` (from `--log-level`) is
/// given, this crate logs at it, unless `rust_log` names this crate itself.
///
/// Directives in `rust_log` that don't parse are skipped with a warning on
/// stderr, since a typo shouldn't stop the server from starting.
pub fn env_filter(
    level: Option<LevelFilter>,
    rust_log: Option<&str>,
) -> EnvFilter {
    let mut filter = match level {
        Some(level) => EnvFilter::new(format!("info,{CRATE_TARGET}={level}")),
        None => EnvFilter::new("info"),
    };

    for directive in rust_log.unwrap_or_default().split(',') {
        let directive = directive.trim();
        if directive.is_empty() {
            continue;
        }
        match directive.parse() {
            Ok(directive) => filter = filter.add_directive(directive),
            Err(err) => {
                eprintln!(
                    "Ignoring invalid RUST_LOG directive '{directive}': {err}"
                )
            }
        }
    }

    filter
}

/// Flushes and shuts down the exporters when dropped. Keep it alive until
/// after the last event that should be exported.
#[must_use]
pub struct Guard {
    #[cfg(feature = "otel")]
    tracer_provider: SdkTracerProvider,
    #[cfg(feature = "otel")]
    meter_provider: SdkMeterProvider,
}

#[cfg(feature = "otel")]
impl Drop for Guard {
    fn drop(&mut self) {
        if let Err(err) = self.tracer_provider.shutdown() {
            eprintln!("Failed to shut down the tracer provider: {err}");
        }
        if let Err(err) = self.meter_provider.shutdown() {
            eprintln!("Failed to shut down the meter provider: {err}");
        }
    }
}

/// Installs the global subscriber.
pub fn setup_tracing(
    level: Option<LevelFilter>,
) -> Result<Guard, Box<dyn std::error::Error>> {
    // Forward records from crates that use `log` instead of `tracing`.
    tracing_log::LogTracer::init()?;

    let filter = env_filter(level, std::env::var("RUST_LOG").ok().as_deref());

    // Color codes are only useful on a terminal. In a pod they end up in the
    // log lines, and Loki can't detect the level.
    let fmt = tracing_subscriber::fmt::layer()
        .with_ansi(std::io::stdout().is_terminal());

    let subscriber = tracing_subscriber::registry().with(fmt);

    #[cfg(feature = "otel")]
    {
        let tracer_provider = tracer_provider()?;
        let meter_provider = meter_provider()?;
        let tracer = tracer_provider.tracer(CRATE_TARGET);

        let subscriber = subscriber
            .with(OpenTelemetryLayer::new(tracer))
            .with(filter);
        tracing::subscriber::set_global_default(subscriber)?;

        Ok(Guard {
            tracer_provider,
            meter_provider,
        })
    }

    #[cfg(not(feature = "otel"))]
    {
        tracing::subscriber::set_global_default(subscriber.with(filter))?;
        Ok(Guard {})
    }
}

/// Describes this service. `OTEL_SERVICE_NAME` and
/// `OTEL_RESOURCE_ATTRIBUTES` are also honored.
#[cfg(feature = "otel")]
fn resource() -> Resource {
    Resource::builder()
        .with_service_name(env!("CARGO_PKG_NAME"))
        .with_attribute(KeyValue::new(
            SERVICE_VERSION,
            env!("CARGO_PKG_VERSION"),
        ))
        .build()
}

#[cfg(feature = "otel")]
fn tracer_provider() -> Result<SdkTracerProvider, Box<dyn std::error::Error>> {
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .build()?;

    Ok(SdkTracerProvider::builder()
        .with_sampler(Sampler::ParentBased(Box::new(Sampler::AlwaysOn)))
        .with_id_generator(RandomIdGenerator::default())
        .with_resource(resource())
        .with_batch_exporter(exporter)
        .build())
}

#[cfg(feature = "otel")]
fn meter_provider() -> Result<SdkMeterProvider, Box<dyn std::error::Error>> {
    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .build()?;

    let reader = PeriodicReader::builder(exporter)
        .with_interval(std::time::Duration::from_secs(60))
        .build();

    let meter_provider = SdkMeterProvider::builder()
        .with_resource(resource())
        .with_reader(reader)
        .build();

    opentelemetry::global::set_meter_provider(meter_provider.clone());
    register_stats(&meter_provider);

    Ok(meter_provider)
}

/// Exports the counters in [`STATS`](lfs_rs::stats::STATS). Request latency is
/// a histogram recorded by the request logger instead.
#[cfg(feature = "otel")]
fn register_stats(meter_provider: &SdkMeterProvider) {
    use lfs_rs::stats::{STATS, Snapshot};
    use opentelemetry::metrics::MeterProvider as _;

    type Read = fn(&Snapshot) -> Vec<(u64, Vec<KeyValue>)>;

    let meter = meter_provider.meter(CRATE_TARGET);

    // The instruments stay registered after these builders' handles drop.
    let counter = |name: &'static str, unit: &'static str, read: Read| {
        meter
            .u64_observable_counter(name)
            .with_unit(unit)
            .with_callback(move |observer| {
                for (value, attributes) in read(&STATS.snapshot()) {
                    observer.observe(value, &attributes);
                }
            })
            .build();
    };
    let gauge = |name: &'static str, unit: &'static str, read: Read| {
        meter
            .u64_observable_gauge(name)
            .with_unit(unit)
            .with_callback(move |observer| {
                for (value, attributes) in read(&STATS.snapshot()) {
                    observer.observe(value, &attributes);
                }
            })
            .build();
    };
    counter("lfs.transfer.bytes", "By", |s| {
        vec![
            (s.bytes_uploaded, vec![KeyValue::new("direction", "upload")]),
            (
                s.bytes_downloaded,
                vec![KeyValue::new("direction", "download")],
            ),
        ]
    });
    counter("lfs.presigned_urls", "{url}", |s| {
        vec![
            (
                s.presigned_uploads,
                vec![KeyValue::new("operation", "upload")],
            ),
            (
                s.presigned_downloads,
                vec![KeyValue::new("operation", "download")],
            ),
        ]
    });
    counter("lfs.cache.lookups", "{lookup}", |s| {
        let lookup = |cache, result| {
            vec![
                KeyValue::new("cache", cache),
                KeyValue::new("result", result),
            ]
        };
        vec![
            (s.disk_cache_hits, lookup("disk", "hit")),
            (s.disk_cache_misses, lookup("disk", "miss")),
            (s.s3_size_cache_hits, lookup("s3_size", "hit")),
            (s.s3_size_cache_misses, lookup("s3_size", "miss")),
            (s.github_auth_cache_hits, lookup("github_auth", "hit")),
        ]
    });
    counter("lfs.github.api_calls", "{call}", |s| {
        vec![(s.github_api_calls, vec![])]
    });
    gauge("lfs.disk_cache.usage", "By", |s| {
        vec![(s.disk_cache_bytes, vec![])]
    });
    // 0 means unlimited, which is better left unreported than reported as 0.
    gauge("lfs.disk_cache.limit", "By", |s| match s.disk_cache_limit {
        0 => vec![],
        limit => vec![(limit, vec![])],
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing::Level;

    /// Which of (this crate at debug, another crate at debug) the filter
    /// lets through.
    fn debug_enabled(filter: EnvFilter) -> (bool, bool) {
        let subscriber = tracing_subscriber::registry().with(filter);
        tracing::subscriber::with_default(subscriber, || {
            (
                tracing::enabled!(target: "lfs_rs", Level::DEBUG),
                tracing::enabled!(target: "hyper", Level::DEBUG),
            )
        })
    }

    #[test]
    fn log_level_applies_to_this_crate_only() {
        let filter = env_filter(Some(LevelFilter::DEBUG), None);
        assert_eq!(debug_enabled(filter), (true, false));

        let filter = env_filter(Some(LevelFilter::INFO), None);
        assert_eq!(debug_enabled(filter), (false, false));
    }

    #[test]
    fn rust_log_sets_the_rest() {
        let filter = env_filter(Some(LevelFilter::INFO), Some("hyper=debug"));
        assert_eq!(debug_enabled(filter), (false, true));

        let filter = env_filter(Some(LevelFilter::INFO), Some("debug"));
        assert_eq!(debug_enabled(filter), (false, true));
    }

    /// Without `--log-level`, this crate follows RUST_LOG like everything else.
    #[test]
    fn rust_log_alone_applies_to_this_crate() {
        let filter = env_filter(None, Some("debug"));
        assert_eq!(debug_enabled(filter), (true, true));

        let filter = env_filter(None, None);
        assert_eq!(debug_enabled(filter), (false, false));
    }

    #[test]
    fn rust_log_naming_this_crate_wins() {
        let filter = env_filter(Some(LevelFilter::INFO), Some("lfs_rs=debug"));
        assert_eq!(debug_enabled(filter), (true, false));

        let filter = env_filter(Some(LevelFilter::DEBUG), Some("lfs_rs=info"));
        assert_eq!(debug_enabled(filter), (false, false));
    }

    #[test]
    fn invalid_directives_are_skipped() {
        let filter = env_filter(
            Some(LevelFilter::INFO),
            Some("hyper=debug,=bogus=,lfs_rs"),
        );
        assert_eq!(debug_enabled(filter), (true, true));
    }
}
