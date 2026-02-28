// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! EventStats and EventTracker FFI bridge.
//!
//! This implements event tracking statistics matching Ray's event_stats.h

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Count, queueing, and execution statistics for a given event.
#[derive(Clone, Debug, Default)]
pub struct EventStats {
    pub cum_count: i64,
    pub curr_count: i64,
    pub cum_execution_time: i64,
    pub cum_queue_time: i64,
    pub min_queue_time: i64,
    pub max_queue_time: i64,
    pub running_count: i64,
}

impl EventStats {
    pub fn new() -> Self {
        Self {
            cum_count: 0,
            curr_count: 0,
            cum_execution_time: 0,
            cum_queue_time: 0,
            min_queue_time: i64::MAX,
            max_queue_time: -1,
            running_count: 0,
        }
    }
}

/// Global stats across all events.
#[derive(Clone, Debug, Default)]
pub struct GlobalStats {
    pub cum_queue_time: i64,
    pub min_queue_time: i64,
    pub max_queue_time: i64,
}

impl GlobalStats {
    pub fn new() -> Self {
        Self {
            cum_queue_time: 0,
            min_queue_time: i64::MAX,
            max_queue_time: -1,
        }
    }
}

/// Internal handle for tracking an event.
pub struct StatsHandleInner {
    pub event_name: String,
    pub start_time: Instant,
    pub end_recorded: bool,
}

/// Stats handle returned by RecordStart.
pub struct RustStatsHandle {
    handle_id: u64,
}

/// Event tracker that manages event statistics.
pub struct RustEventTracker {
    inner: Mutex<EventTrackerInner>,
}

struct EventTrackerInner {
    event_stats: HashMap<String, EventStats>,
    global_stats: GlobalStats,
    handles: HashMap<u64, StatsHandleInner>,
    next_handle_id: u64,
}

impl RustEventTracker {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(EventTrackerInner {
                event_stats: HashMap::new(),
                global_stats: GlobalStats::new(),
                handles: HashMap::new(),
                next_handle_id: 0,
            }),
        }
    }

    pub fn record_start(&self, name: &str) -> u64 {
        let mut inner = self.inner.lock().unwrap();

        // Get or create stats for this event
        let stats = inner
            .event_stats
            .entry(name.to_string())
            .or_insert_with(EventStats::new);
        stats.cum_count += 1;
        stats.curr_count += 1;

        // Create handle
        let handle_id = inner.next_handle_id;
        inner.next_handle_id += 1;

        inner.handles.insert(
            handle_id,
            StatsHandleInner {
                event_name: name.to_string(),
                start_time: Instant::now(),
                end_recorded: false,
            },
        );

        handle_id
    }

    pub fn record_end(&self, handle_id: u64) {
        let mut inner = self.inner.lock().unwrap();

        if let Some(handle) = inner.handles.get_mut(&handle_id) {
            if handle.end_recorded {
                return;
            }
            handle.end_recorded = true;

            let elapsed = handle.start_time.elapsed().as_nanos() as i64;
            let event_name = handle.event_name.clone();

            if let Some(stats) = inner.event_stats.get_mut(&event_name) {
                stats.curr_count -= 1;
                stats.cum_execution_time += elapsed;
            }
        }
    }

    pub fn record_execution_start(&self, handle_id: u64) {
        let mut inner = self.inner.lock().unwrap();

        if let Some(handle) = inner.handles.get(&handle_id) {
            let queue_time = handle.start_time.elapsed().as_nanos() as i64;
            let event_name = handle.event_name.clone();

            if let Some(stats) = inner.event_stats.get_mut(&event_name) {
                stats.running_count += 1;
                stats.cum_queue_time += queue_time;
                stats.min_queue_time = stats.min_queue_time.min(queue_time);
                stats.max_queue_time = stats.max_queue_time.max(queue_time);
            }

            inner.global_stats.cum_queue_time += queue_time;
            inner.global_stats.min_queue_time =
                inner.global_stats.min_queue_time.min(queue_time);
            inner.global_stats.max_queue_time =
                inner.global_stats.max_queue_time.max(queue_time);
        }
    }

    pub fn record_execution_end(&self, handle_id: u64, execution_time_ns: i64) {
        let mut inner = self.inner.lock().unwrap();

        if let Some(handle) = inner.handles.get_mut(&handle_id) {
            if handle.end_recorded {
                return;
            }
            handle.end_recorded = true;

            let event_name = handle.event_name.clone();

            if let Some(stats) = inner.event_stats.get_mut(&event_name) {
                stats.curr_count -= 1;
                stats.running_count -= 1;
                stats.cum_execution_time += execution_time_ns;
            }
        }
    }

    pub fn get_event_stats(&self, name: &str) -> Option<EventStats> {
        let inner = self.inner.lock().unwrap();
        inner.event_stats.get(name).cloned()
    }

    pub fn get_global_stats(&self) -> GlobalStats {
        let inner = self.inner.lock().unwrap();
        inner.global_stats.clone()
    }
}

impl Default for RustEventTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cxx::bridge(namespace = "ray::ffi")]
mod ffi {
    /// Event statistics exposed to C++.
    pub struct CxxEventStats {
        pub cum_count: i64,
        pub curr_count: i64,
        pub cum_execution_time: i64,
        pub cum_queue_time: i64,
        pub running_count: i64,
    }

    extern "Rust" {
        type RustEventTracker;

        fn event_tracker_new() -> Box<RustEventTracker>;
        fn event_tracker_record_start(tracker: &RustEventTracker, name: &str) -> u64;
        fn event_tracker_record_end(tracker: &RustEventTracker, handle_id: u64);
        fn event_tracker_record_execution_start(tracker: &RustEventTracker, handle_id: u64);
        fn event_tracker_record_execution_end(
            tracker: &RustEventTracker,
            handle_id: u64,
            execution_time_ns: i64,
        );
        fn event_tracker_get_stats(tracker: &RustEventTracker, name: &str) -> CxxEventStats;
        fn event_tracker_has_stats(tracker: &RustEventTracker, name: &str) -> bool;
    }
}

fn event_tracker_new() -> Box<RustEventTracker> {
    Box::new(RustEventTracker::new())
}

fn event_tracker_record_start(tracker: &RustEventTracker, name: &str) -> u64 {
    tracker.record_start(name)
}

fn event_tracker_record_end(tracker: &RustEventTracker, handle_id: u64) {
    tracker.record_end(handle_id);
}

fn event_tracker_record_execution_start(tracker: &RustEventTracker, handle_id: u64) {
    tracker.record_execution_start(handle_id);
}

fn event_tracker_record_execution_end(
    tracker: &RustEventTracker,
    handle_id: u64,
    execution_time_ns: i64,
) {
    tracker.record_execution_end(handle_id, execution_time_ns);
}

fn event_tracker_get_stats(tracker: &RustEventTracker, name: &str) -> ffi::CxxEventStats {
    let stats = tracker.get_event_stats(name).unwrap_or_default();
    ffi::CxxEventStats {
        cum_count: stats.cum_count,
        curr_count: stats.curr_count,
        cum_execution_time: stats.cum_execution_time,
        cum_queue_time: stats.cum_queue_time,
        running_count: stats.running_count,
    }
}

fn event_tracker_has_stats(tracker: &RustEventTracker, name: &str) -> bool {
    tracker.get_event_stats(name).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_record_start_end() {
        let tracker = RustEventTracker::new();

        let handle = tracker.record_start("method");
        let stats = tracker.get_event_stats("method").unwrap();
        assert_eq!(stats.cum_count, 1);
        assert_eq!(stats.curr_count, 1);

        thread::sleep(Duration::from_millis(10));
        tracker.record_end(handle);

        let stats = tracker.get_event_stats("method").unwrap();
        assert_eq!(stats.cum_count, 1);
        assert_eq!(stats.curr_count, 0);
        assert!(stats.cum_execution_time >= 10_000_000); // At least 10ms in nanoseconds
    }
}
