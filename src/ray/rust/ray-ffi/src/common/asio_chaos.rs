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

//! FFI bindings for Asio chaos testing utilities.
//!
//! Implements delay configuration parsing for testing asynchronous operations.

use parking_lot::RwLock;
use rand::Rng;
use std::collections::HashMap;
use std::sync::Arc;

/// Delay range configuration for a method.
#[derive(Clone, Debug)]
struct DelayRange {
    min_us: i64,
    max_us: i64,
}

/// Global delay configuration storage.
struct DelayConfig {
    methods: HashMap<String, DelayRange>,
    global: Option<DelayRange>,
}

lazy_static::lazy_static! {
    static ref DELAY_CONFIG: Arc<RwLock<DelayConfig>> = Arc::new(RwLock::new(DelayConfig {
        methods: HashMap::new(),
        global: None,
    }));
}

#[cxx::bridge(namespace = "ray::ffi")]
mod ffi {
    extern "Rust" {
        /// Initialize delay configuration from a string.
        /// Format: "method1=min:max,method2=min:max,*=min:max"
        fn asio_chaos_init(config: &str) -> bool;

        /// Get a random delay for a method in microseconds.
        fn asio_chaos_get_delay_us(method_name: &str) -> i64;

        /// Clear the delay configuration.
        fn asio_chaos_clear();
    }
}

/// Parse delay configuration string and initialize the global config.
/// Format: "method1=min:max,method2=min:max,*=min:max"
fn asio_chaos_init(config: &str) -> bool {
    let mut methods = HashMap::new();
    let mut global = None;

    if config.is_empty() {
        let mut cfg = DELAY_CONFIG.write();
        cfg.methods = methods;
        cfg.global = global;
        return true;
    }

    for entry in config.split(',') {
        let parts: Vec<&str> = entry.splitn(2, '=').collect();
        if parts.len() != 2 {
            return false;
        }

        let method_name = parts[0].trim();
        let range_str = parts[1].trim();

        let range_parts: Vec<&str> = range_str.split(':').collect();
        if range_parts.len() != 2 {
            return false;
        }

        let min_us: i64 = match range_parts[0].parse() {
            Ok(v) => v,
            Err(_) => return false,
        };
        let max_us: i64 = match range_parts[1].parse() {
            Ok(v) => v,
            Err(_) => return false,
        };

        let range = DelayRange { min_us, max_us };

        if method_name == "*" {
            global = Some(range);
        } else {
            methods.insert(method_name.to_string(), range);
        }
    }

    let mut cfg = DELAY_CONFIG.write();
    cfg.methods = methods;
    cfg.global = global;
    true
}

/// Get a random delay for a method in microseconds.
fn asio_chaos_get_delay_us(method_name: &str) -> i64 {
    let cfg = DELAY_CONFIG.read();

    let range = if let Some(range) = cfg.methods.get(method_name) {
        range.clone()
    } else if let Some(ref global) = cfg.global {
        global.clone()
    } else {
        return 0;
    };

    if range.min_us == range.max_us {
        return range.min_us;
    }

    let mut rng = rand::thread_rng();
    rng.gen_range(range.min_us..=range.max_us)
}

/// Clear the delay configuration.
fn asio_chaos_clear() {
    let mut cfg = DELAY_CONFIG.write();
    cfg.methods.clear();
    cfg.global = None;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init_and_get_delay() {
        asio_chaos_init("method1=10:100,method2=20:30");

        for _ in 0..100 {
            let delay = asio_chaos_get_delay_us("method1");
            assert!(delay >= 10 && delay <= 100);
        }

        for _ in 0..100 {
            let delay = asio_chaos_get_delay_us("method2");
            assert!(delay >= 20 && delay <= 30);
        }

        // Unknown method should return 0
        assert_eq!(asio_chaos_get_delay_us("unknown"), 0);

        asio_chaos_clear();
    }

    #[test]
    fn test_with_global() {
        asio_chaos_init("method1=10:10,method2=20:30,*=100:200");

        // Specific methods
        assert_eq!(asio_chaos_get_delay_us("method1"), 10);

        for _ in 0..100 {
            let delay = asio_chaos_get_delay_us("method2");
            assert!(delay >= 20 && delay <= 30);
        }

        // Global fallback
        for _ in 0..100 {
            let delay = asio_chaos_get_delay_us("others");
            assert!(delay >= 100 && delay <= 200);
        }

        asio_chaos_clear();
    }
}
