use std::time::Duration;

/// Severity of a recorded coercion. `Info` = Tier 1 lossless. `Warn` = Tier 2 lossy-structural.
/// Tier 3 (lossy-semantic) coercions are not recorded — they fail the run via `ElError::Coercion`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoercionLevel {
    Info,
    Warn,
}

/// A single type coercion applied during a run.
#[derive(Debug, Clone)]
pub struct Coercion {
    pub column: String,
    pub from: String,
    pub to: String,
    pub level: CoercionLevel,
}

/// Post-run statistics returned by `Transfer::run()`.
#[derive(Debug, Default, Clone)]
pub struct RunReport {
    pub rows: u64,
    pub bytes_written: u64,
    pub duration: Duration,
    pub coercions: Vec<Coercion>,
}
