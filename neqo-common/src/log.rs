// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::sync::Once;

use env_logger::Builder;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{
    Layer as _, filter::Targets, layer::SubscriberExt as _, util::SubscriberInitExt as _,
};

const fn to_tracing_level_filter(filter: log::LevelFilter) -> LevelFilter {
    match filter {
        log::LevelFilter::Off => LevelFilter::OFF,
        log::LevelFilter::Error => LevelFilter::ERROR,
        log::LevelFilter::Warn => LevelFilter::WARN,
        log::LevelFilter::Info => LevelFilter::INFO,
        log::LevelFilter::Debug => LevelFilter::DEBUG,
        log::LevelFilter::Trace => LevelFilter::TRACE,
    }
}

/// Initialize the logging system with optional level filtering.
///
/// Neqo emits through `tracing`, printed by `tracing-subscriber`'s default
/// formatter. Dependencies that still use `log` are printed by `env_logger`.
/// Both read `RUST_LOG`. It can be called multiple times safely.
pub fn init(level_filter: Option<log::LevelFilter>) {
    static INIT_ONCE: Once = Once::new();

    INIT_ONCE.call_once(|| {
        init_tracing(level_filter.map(to_tracing_level_filter));
        init_log(level_filter);
    });
}

fn init_tracing(level_filter: Option<LevelFilter>) {
    if tracing::level_filters::STATIC_MAX_LEVEL == LevelFilter::OFF {
        return;
    }
    let mut targets = std::env::var("RUST_LOG")
        .ok()
        .and_then(|directives| {
            directives
                .parse::<Targets>()
                .map_err(|e| eprintln!("Invalid RUST_LOG {directives:?}: {e}"))
                .ok()
        })
        .unwrap_or_else(|| Targets::new().with_default(LevelFilter::ERROR));
    if let Some(filter) = level_filter {
        targets = targets.with_default(filter);
    }
    let layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_filter(targets);
    if let Err(e) = tracing_subscriber::registry().with(layer).try_init() {
        eprintln!("Tracing initialization error {e:?}");
    } else {
        tracing::debug!("Logging initialized");
    }
}

fn init_log(level_filter: Option<log::LevelFilter>) {
    if ::log::STATIC_MAX_LEVEL == ::log::LevelFilter::Off {
        return;
    }
    let mut builder = Builder::from_env("RUST_LOG");
    if let Some(filter) = level_filter {
        builder.filter_level(filter);
    }
    if let Err(e) = builder.try_init() {
        eprintln!("Logging initialization error {e:?}");
    }
}
