// SPDX-License-Identifier: MPL-2.0
//! Exact nearest-rank percentiles, without discarding failures or raw samples.
use super::protocol::Batch;
use serde::Serialize;
use std::collections::BTreeSet;
#[derive(Debug, Serialize)]
pub struct Summary {
    pub successes: usize,
    pub failures: usize,
    pub throughput_per_second: f64,
    pub distinct_log_indices: usize,
    pub commands_per_log_index: Option<f64>,
    pub min_us: Option<u64>,
    pub p50_us: Option<u64>,
    pub p95_us: Option<u64>,
    pub p99_us: Option<u64>,
    pub max_us: Option<u64>,
}
fn percentile(sorted: &[u64], percentage: usize) -> Option<u64> {
    let rank = sorted.len().checked_mul(percentage)?.div_ceil(100);
    sorted.get(rank.checked_sub(1)?).copied()
}
#[allow(clippy::cast_precision_loss)] // Presentation only; raw integer samples remain in the report.
pub fn summarize(batch: &Batch) -> Summary {
    let mut values: Vec<_> = batch
        .samples
        .iter()
        .filter(|s| s.error.is_none())
        .map(|s| s.latency_us)
        .collect();
    values.sort_unstable();
    let indices: BTreeSet<_> = batch
        .samples
        .iter()
        .filter(|s| s.error.is_none())
        .filter_map(|s| s.log_index)
        .collect();
    Summary {
        successes: values.len(),
        failures: batch.samples.len().saturating_sub(values.len()),
        distinct_log_indices: indices.len(),
        commands_per_log_index: if indices.is_empty() {
            None
        } else {
            Some(values.len() as f64 / indices.len() as f64)
        },
        throughput_per_second: if batch.elapsed_us == 0 {
            0.0
        } else {
            values.len() as f64 * 1_000_000.0 / batch.elapsed_us as f64
        },
        min_us: values.first().copied(),
        p50_us: percentile(&values, 50),
        p95_us: percentile(&values, 95),
        p99_us: percentile(&values, 99),
        max_us: values.last().copied(),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn nearest_rank_handles_empty_and_small_samples() {
        assert_eq!(percentile(&[], 99), None);
        assert_eq!(percentile(&[7], 99), Some(7));
        assert_eq!(percentile(&[1, 2, 3, 4], 50), Some(2));
        assert_eq!(percentile(&[1, 2, 3, 4], 95), Some(4));
    }
}

#[cfg(test)]
mod failure_tests {
    use super::*;
    use crate::protocol::Sample;
    #[test]
    fn failed_requests_are_counted_and_excluded_from_success_percentiles() {
        let batch = Batch {
            elapsed_us: 1_000_000,
            samples: vec![
                Sample {
                    sequence: 0,
                    latency_us: 25,
                    command_json_bytes: 100,
                    log_index: Some(1),
                    error: None,
                    replayed: false,
                },
                Sample {
                    sequence: 1,
                    latency_us: 1000,
                    command_json_bytes: 100,
                    log_index: None,
                    error: Some("timeout".into()),
                    replayed: false,
                },
            ],
        };
        let summary = summarize(&batch);
        assert_eq!(summary.successes, 1);
        assert_eq!(summary.failures, 1);
        assert_eq!(summary.p99_us, Some(25));
        assert!((summary.throughput_per_second - 1.0).abs() < f64::EPSILON);
    }
}
