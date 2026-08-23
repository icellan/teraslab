//! Test-only `tracing` capture, shared by the modules that pin the log lines a
//! DELETING path owes its operator.
//!
//! The rule those tests enforce — "a path that deletes data must never be
//! silent" — is not local to one module: it is owed by the orphan reclaim
//! (`cluster::coordinator`) and by the migration-completion prune
//! (`server::dispatch`) alike. Both need the same subscriber plumbing, so it
//! lives here instead of being copied.

use std::sync::Arc;

/// Capture every `tracing` event at exactly `level` emitted by `f` on THIS
/// thread, flattened to one `message field=value ...` line per event.
///
/// Thread-scoped (`with_default`), so concurrent tests do not interfere.
pub(crate) fn capture_tracing_lines(level: tracing::Level, f: impl FnOnce()) -> Vec<String> {
    use std::sync::Mutex as StdMutex;
    use tracing::Event;
    use tracing::field::{Field, Visit};
    use tracing_subscriber::Layer;
    use tracing_subscriber::layer::Context;
    use tracing_subscriber::prelude::*;
    use tracing_subscriber::registry::LookupSpan;

    #[derive(Default)]
    struct LineVisitor {
        rendered: String,
    }

    impl Visit for LineVisitor {
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            if field.name() == "message" {
                self.rendered.insert_str(0, &format!("{value:?} "));
            } else {
                self.rendered
                    .push_str(&format!("{}={value:?} ", field.name()));
            }
        }
    }

    struct CaptureLayer {
        want: tracing::Level,
        lines: Arc<StdMutex<Vec<String>>>,
    }

    impl<S> Layer<S> for CaptureLayer
    where
        S: tracing::Subscriber + for<'a> LookupSpan<'a>,
    {
        fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
            if event.metadata().level() != &self.want {
                return;
            }
            let mut visitor = LineVisitor::default();
            event.record(&mut visitor);
            self.lines
                .lock()
                .expect("capture lock")
                .push(visitor.rendered);
        }
    }

    let lines = Arc::new(StdMutex::new(Vec::new()));
    // TRACE lets the filter pass everything through to the level test above.
    let subscriber = tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new("trace"))
        .with(CaptureLayer {
            want: level,
            lines: lines.clone(),
        });
    tracing::subscriber::with_default(subscriber, f);
    let captured = lines.lock().expect("capture lock");
    captured.clone()
}
