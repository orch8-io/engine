//! Bounded event-time windows over durable continuity stream frames.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use orch8_types::continuity::{StreamFrame, StreamFrameState, StreamWindow, StreamWindowKind};

const MAX_WINDOWS: usize = 10_000;
const MAX_SLIDING_FANOUT: u64 = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowSpec {
    Tumbling { width_ms: u64, offset_ms: i64 },
    Sliding { width_ms: u64, slide_ms: u64 },
    Session { gap_ms: u64 },
}

/// Evaluate windows deterministically from server-assigned frame timestamps.
/// Retracted frames never contribute. Inputs and fan-out are bounded by the
/// caller and validated again here to prevent an accidental allocation cliff.
pub fn evaluate(frames: &[StreamFrame], spec: WindowSpec) -> Result<Vec<StreamWindow>, String> {
    validate(spec)?;
    let mut committed: Vec<StreamFrame> = frames
        .iter()
        .filter(|frame| frame.state == StreamFrameState::Committed)
        .cloned()
        .collect();
    committed.sort_by_key(|frame| (frame.created_at, frame.sequence));
    match spec {
        WindowSpec::Tumbling {
            width_ms,
            offset_ms,
        } => fixed_windows(&committed, width_ms, width_ms, offset_ms, false),
        WindowSpec::Sliding { width_ms, slide_ms } => {
            fixed_windows(&committed, width_ms, slide_ms, 0, true)
        }
        WindowSpec::Session { gap_ms } => session_windows(&committed, gap_ms),
    }
}

fn validate(spec: WindowSpec) -> Result<(), String> {
    match spec {
        WindowSpec::Tumbling { width_ms: 0, .. } => {
            Err("tumbling window width_ms must be positive".into())
        }
        WindowSpec::Tumbling { offset_ms, .. }
            if !(-86_400_000..=86_400_000).contains(&offset_ms) =>
        {
            Err("tumbling window offset_ms must be within one day".into())
        }
        WindowSpec::Sliding { width_ms, slide_ms }
            if width_ms == 0 || slide_ms == 0 || width_ms < slide_ms =>
        {
            Err("sliding windows require width_ms >= slide_ms > 0".into())
        }
        WindowSpec::Sliding { width_ms, slide_ms }
            if width_ms.div_ceil(slide_ms) > MAX_SLIDING_FANOUT =>
        {
            Err(format!(
                "sliding window fan-out exceeds {MAX_SLIDING_FANOUT} windows per frame"
            ))
        }
        WindowSpec::Session { gap_ms: 0 } => Err("session window gap_ms must be positive".into()),
        _ => Ok(()),
    }
}

fn fixed_windows(
    frames: &[StreamFrame],
    width_ms: u64,
    slide_ms: u64,
    offset_ms: i64,
    overlapping: bool,
) -> Result<Vec<StreamWindow>, String> {
    let width = i64::try_from(width_ms).map_err(|_| "width_ms is too large")?;
    let slide = i64::try_from(slide_ms).map_err(|_| "slide_ms is too large")?;
    let mut buckets: BTreeMap<i64, Vec<StreamFrame>> = BTreeMap::new();
    for frame in frames {
        let timestamp = frame.created_at.timestamp_millis();
        let latest = (timestamp - offset_ms).div_euclid(slide) * slide + offset_ms;
        let fanout = if overlapping {
            width_ms.div_ceil(slide_ms)
        } else {
            1
        };
        for index in 0..fanout {
            let index = i64::try_from(index).map_err(|_| "window index overflow")?;
            let start = latest
                .checked_sub(index.saturating_mul(slide))
                .ok_or("window timestamp overflow")?;
            let end = start
                .checked_add(width)
                .ok_or("window timestamp overflow")?;
            if timestamp >= start && timestamp < end {
                buckets.entry(start).or_default().push(frame.clone());
                if buckets.len() > MAX_WINDOWS {
                    return Err(format!("window count exceeds {MAX_WINDOWS}"));
                }
            }
        }
    }
    buckets
        .into_iter()
        .map(|(start, frames)| {
            let end = start
                .checked_add(width)
                .ok_or("window timestamp overflow")?;
            build_window(
                if overlapping {
                    StreamWindowKind::Sliding
                } else {
                    StreamWindowKind::Tumbling
                },
                start,
                end,
                frames,
            )
        })
        .collect()
}

fn session_windows(frames: &[StreamFrame], gap_ms: u64) -> Result<Vec<StreamWindow>, String> {
    let gap = i64::try_from(gap_ms).map_err(|_| "gap_ms is too large")?;
    let mut groups: Vec<Vec<StreamFrame>> = Vec::new();
    for frame in frames {
        let starts_new = groups
            .last()
            .and_then(|group| group.last())
            .is_some_and(|last| {
                frame
                    .created_at
                    .signed_duration_since(last.created_at)
                    .num_milliseconds()
                    > gap
            });
        if groups.is_empty() || starts_new {
            if groups.len() >= MAX_WINDOWS {
                return Err(format!("window count exceeds {MAX_WINDOWS}"));
            }
            groups.push(Vec::new());
        }
        groups
            .last_mut()
            .expect("group was just created")
            .push(frame.clone());
    }
    groups
        .into_iter()
        .map(|frames| {
            let start = frames[0].created_at.timestamp_millis();
            let end = frames
                .last()
                .expect("session is non-empty")
                .created_at
                .timestamp_millis()
                .checked_add(gap)
                .ok_or("window timestamp overflow")?;
            build_window(StreamWindowKind::Session, start, end, frames)
        })
        .collect()
}

fn build_window(
    kind: StreamWindowKind,
    start_ms: i64,
    end_ms: i64,
    frames: Vec<StreamFrame>,
) -> Result<StreamWindow, String> {
    let starts_at = DateTime::<Utc>::from_timestamp_millis(start_ms)
        .ok_or_else(|| "window start is outside the supported timestamp range".to_string())?;
    let ends_at = DateTime::<Utc>::from_timestamp_millis(end_ms)
        .ok_or_else(|| "window end is outside the supported timestamp range".to_string())?;
    let first_sequence = frames.iter().map(|frame| frame.sequence).min().unwrap_or(0);
    let last_sequence = frames.iter().map(|frame| frame.sequence).max().unwrap_or(0);
    Ok(StreamWindow {
        kind,
        starts_at,
        ends_at,
        first_sequence,
        last_sequence,
        frames,
    })
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use orch8_types::continuity::{ContinuityId, ExecutionEpoch, StreamFrameState, StreamId};
    use orch8_types::ids::TenantId;

    use super::*;

    fn frame(sequence: u64, millis: i64, state: StreamFrameState) -> StreamFrame {
        StreamFrame {
            stream_id: StreamId::new(),
            tenant_id: TenantId::unchecked("tenant-a"),
            continuity_id: ContinuityId::new(),
            epoch: ExecutionEpoch::initial(),
            sequence,
            checkpoint_sha256: "0".repeat(64),
            state,
            payload: serde_json::json!({"sequence": sequence}),
            created_at: Utc.timestamp_millis_opt(millis).unwrap(),
            expires_at: Utc.timestamp_millis_opt(millis + 60_000).unwrap(),
        }
    }

    #[test]
    fn tumbling_windows_partition_and_ignore_retractions() {
        let frames = vec![
            frame(0, 100, StreamFrameState::Committed),
            frame(1, 900, StreamFrameState::Committed),
            frame(2, 1_100, StreamFrameState::Committed),
            frame(3, 1_200, StreamFrameState::Retracted),
        ];
        let windows = evaluate(
            &frames,
            WindowSpec::Tumbling {
                width_ms: 1_000,
                offset_ms: 0,
            },
        )
        .unwrap();
        assert_eq!(windows.len(), 2);
        assert_eq!(windows[0].frames.len(), 2);
        assert_eq!(windows[1].frames.len(), 1);
        assert_eq!(windows[1].last_sequence, 2);
    }

    #[test]
    fn sliding_windows_overlap_without_duplicates_inside_a_window() {
        let frames = vec![frame(7, 1_500, StreamFrameState::Committed)];
        let windows = evaluate(
            &frames,
            WindowSpec::Sliding {
                width_ms: 2_000,
                slide_ms: 1_000,
            },
        )
        .unwrap();
        assert_eq!(windows.len(), 2);
        assert!(windows.iter().all(|window| window.frames.len() == 1));
        assert_eq!(windows[0].starts_at.timestamp_millis(), 0);
        assert_eq!(windows[1].starts_at.timestamp_millis(), 1_000);
    }

    #[test]
    fn session_windows_split_after_gap() {
        let frames = vec![
            frame(0, 0, StreamFrameState::Committed),
            frame(1, 500, StreamFrameState::Committed),
            frame(2, 2_000, StreamFrameState::Committed),
        ];
        let windows = evaluate(&frames, WindowSpec::Session { gap_ms: 1_000 }).unwrap();
        assert_eq!(windows.len(), 2);
        assert_eq!(windows[0].frames.len(), 2);
        assert_eq!(windows[0].ends_at.timestamp_millis(), 1_500);
    }

    #[test]
    fn invalid_or_explosive_specs_are_rejected() {
        assert!(evaluate(&[], WindowSpec::Session { gap_ms: 0 }).is_err());
        assert!(
            evaluate(
                &[],
                WindowSpec::Sliding {
                    width_ms: 101,
                    slide_ms: 1,
                }
            )
            .is_err()
        );
    }
}
