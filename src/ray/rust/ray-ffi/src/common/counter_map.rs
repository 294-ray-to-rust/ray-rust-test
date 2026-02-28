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

//! CounterMap FFI bridge.
//!
//! This implements a counter map matching Ray's counter_map.h

use std::collections::HashMap;
use std::sync::Mutex;

/// A map that tracks counts for string keys.
pub struct RustCounterMap {
    inner: Mutex<CounterMapInner>,
}

struct CounterMapInner {
    counters: HashMap<String, i64>,
    total: i64,
    pending_callbacks: Vec<String>,
}

impl RustCounterMap {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(CounterMapInner {
                counters: HashMap::new(),
                total: 0,
                pending_callbacks: Vec::new(),
            }),
        }
    }

    pub fn increment(&self, key: &str, value: i64) {
        let mut inner = self.inner.lock().unwrap();
        let entry = inner.counters.entry(key.to_string()).or_insert(0);
        *entry += value;
        inner.total += value;
        inner.pending_callbacks.push(key.to_string());
    }

    pub fn decrement(&self, key: &str, value: i64) {
        let mut inner = self.inner.lock().unwrap();
        let should_remove = if let Some(entry) = inner.counters.get_mut(key) {
            *entry -= value;
            *entry == 0
        } else {
            return;
        };
        inner.total -= value;
        inner.pending_callbacks.push(key.to_string());
        if should_remove {
            inner.counters.remove(key);
        }
    }

    pub fn get(&self, key: &str) -> i64 {
        let inner = self.inner.lock().unwrap();
        *inner.counters.get(key).unwrap_or(&0)
    }

    pub fn total(&self) -> i64 {
        let inner = self.inner.lock().unwrap();
        inner.total
    }

    pub fn size(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.counters.len()
    }

    pub fn swap(&self, from: &str, to: &str, value: i64) {
        let mut inner = self.inner.lock().unwrap();

        // Decrement from
        if let Some(entry) = inner.counters.get_mut(from) {
            let swap_val = value.min(*entry);
            *entry -= swap_val;
            if *entry == 0 {
                inner.counters.remove(from);
            }

            // Increment to
            let to_entry = inner.counters.entry(to.to_string()).or_insert(0);
            *to_entry += swap_val;

            inner.pending_callbacks.push(from.to_string());
            inner.pending_callbacks.push(to.to_string());
        }
    }

    pub fn num_pending_callbacks(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.pending_callbacks.len()
    }

    pub fn flush_callbacks(&self) -> Vec<String> {
        let mut inner = self.inner.lock().unwrap();
        std::mem::take(&mut inner.pending_callbacks)
    }

    pub fn keys(&self) -> Vec<String> {
        let inner = self.inner.lock().unwrap();
        inner.counters.keys().cloned().collect()
    }
}

impl Default for RustCounterMap {
    fn default() -> Self {
        Self::new()
    }
}

#[cxx::bridge(namespace = "ray::ffi")]
mod ffi {
    extern "Rust" {
        type RustCounterMap;

        fn counter_map_new() -> Box<RustCounterMap>;
        fn counter_map_increment(map: &RustCounterMap, key: &str, value: i64);
        fn counter_map_decrement(map: &RustCounterMap, key: &str, value: i64);
        fn counter_map_get(map: &RustCounterMap, key: &str) -> i64;
        fn counter_map_total(map: &RustCounterMap) -> i64;
        fn counter_map_size(map: &RustCounterMap) -> usize;
        fn counter_map_swap(map: &RustCounterMap, from: &str, to: &str, value: i64);
        fn counter_map_num_pending_callbacks(map: &RustCounterMap) -> usize;
        fn counter_map_flush_callbacks(map: &RustCounterMap) -> Vec<String>;
        fn counter_map_keys(map: &RustCounterMap) -> Vec<String>;
    }
}

fn counter_map_new() -> Box<RustCounterMap> {
    Box::new(RustCounterMap::new())
}

fn counter_map_increment(map: &RustCounterMap, key: &str, value: i64) {
    map.increment(key, value);
}

fn counter_map_decrement(map: &RustCounterMap, key: &str, value: i64) {
    map.decrement(key, value);
}

fn counter_map_get(map: &RustCounterMap, key: &str) -> i64 {
    map.get(key)
}

fn counter_map_total(map: &RustCounterMap) -> i64 {
    map.total()
}

fn counter_map_size(map: &RustCounterMap) -> usize {
    map.size()
}

fn counter_map_swap(map: &RustCounterMap, from: &str, to: &str, value: i64) {
    map.swap(from, to, value);
}

fn counter_map_num_pending_callbacks(map: &RustCounterMap) -> usize {
    map.num_pending_callbacks()
}

fn counter_map_flush_callbacks(map: &RustCounterMap) -> Vec<String> {
    map.flush_callbacks()
}

fn counter_map_keys(map: &RustCounterMap) -> Vec<String> {
    map.keys()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic() {
        let c = RustCounterMap::new();
        c.increment("k1", 1);
        c.increment("k1", 1);
        c.increment("k2", 1);
        assert_eq!(c.get("k1"), 2);
        assert_eq!(c.get("k2"), 1);
        assert_eq!(c.get("k3"), 0);
        assert_eq!(c.total(), 3);
        assert_eq!(c.size(), 2);

        c.decrement("k1", 1);
        c.decrement("k2", 1);
        assert_eq!(c.get("k1"), 1);
        assert_eq!(c.get("k2"), 0);
        assert_eq!(c.total(), 1);
        assert_eq!(c.size(), 1);
    }
}
