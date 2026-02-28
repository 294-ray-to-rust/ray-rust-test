// Copyright 2025 The Ray Authors.
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

//! ThreadChecker FFI bridge.
//!
//! Utility to verify that calls are happening on the same thread
//! that created the ThreadChecker.

use std::thread::ThreadId;

/// Thread checker that validates calls happen on the creating thread.
pub struct RustThreadChecker {
    thread_id: ThreadId,
}

impl RustThreadChecker {
    /// Create a new thread checker, capturing the current thread ID.
    pub fn new() -> Self {
        Self {
            thread_id: std::thread::current().id(),
        }
    }

    /// Check if the current call is on the same thread that created this checker.
    pub fn is_on_same_thread(&self) -> bool {
        std::thread::current().id() == self.thread_id
    }
}

impl Default for RustThreadChecker {
    fn default() -> Self {
        Self::new()
    }
}

#[cxx::bridge(namespace = "ffi")]
mod ffi {
    extern "Rust" {
        type RustThreadChecker;

        fn thread_checker_new() -> Box<RustThreadChecker>;
        fn thread_checker_is_on_same_thread(checker: &RustThreadChecker) -> bool;
    }
}

fn thread_checker_new() -> Box<RustThreadChecker> {
    Box::new(RustThreadChecker::new())
}

fn thread_checker_is_on_same_thread(checker: &RustThreadChecker) -> bool {
    checker.is_on_same_thread()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_basic() {
        let checker = RustThreadChecker::new();

        // Pass at initialization
        assert!(checker.is_on_same_thread());

        // Pass when invoked on same thread
        assert!(checker.is_on_same_thread());

        // Fail when invoked on different thread
        let result = thread::spawn(move || checker.is_on_same_thread())
            .join()
            .unwrap();
        assert!(!result);
    }
}
