// Copyright 2024 The Ray Authors.
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

//! Utility types FFI bridge for C++ interop.
//!
//! This module provides FFI bindings for utility types like ExponentialBackoff
//! and StringToInt.

use ray_common::status::{RayError, StatusCode as RustStatusCode};

/// Default maximum backoff in milliseconds (1 minute).
const DEFAULT_MAX_BACKOFF_MS: u64 = 60 * 1000;

/// Exponential backoff counter for throttling.
pub struct RustExponentialBackoff {
    curr_value: u64,
    initial_value: u64,
    max_value: u64,
    multiplier: f64,
}

impl RustExponentialBackoff {
    /// Create a new exponential backoff counter.
    pub fn new(initial_value: u64, multiplier: f64, max_value: u64) -> Self {
        Self {
            curr_value: initial_value,
            initial_value,
            max_value,
            multiplier,
        }
    }

    /// Get the next backoff value.
    pub fn next(&mut self) -> u64 {
        let ret = self.curr_value;
        let next_val = (self.curr_value as f64 * self.multiplier) as u64;
        self.curr_value = next_val.min(self.max_value);
        ret
    }

    /// Get the current backoff value.
    pub fn current(&self) -> u64 {
        self.curr_value
    }

    /// Reset to the initial value.
    pub fn reset(&mut self) {
        self.curr_value = self.initial_value;
    }

    /// Compute backoff delay using formula: min(base * 2^attempt, max_backoff)
    pub fn get_backoff_ms(attempt: u64, base_ms: u64, max_backoff_ms: u64) -> u64 {
        if base_ms == 0 {
            return 0;
        }

        // Prevent overflow: if attempt >= 64, the shift would overflow
        if attempt >= 64 {
            return max_backoff_ms;
        }

        // Calculate 2^attempt using checked arithmetic
        let multiplier = 1u64.checked_shl(attempt as u32);

        match multiplier {
            Some(m) => {
                // Calculate base_ms * 2^attempt with overflow check
                match base_ms.checked_mul(m) {
                    Some(backoff) => backoff.min(max_backoff_ms),
                    None => max_backoff_ms, // Overflow, return max
                }
            }
            None => max_backoff_ms, // Shift overflow, return max
        }
    }
}

/// Result type for string parsing.
pub struct RustParseResult {
    value: Option<i64>,
    error: Option<RayError>,
}

impl RustParseResult {
    pub fn ok(value: i64) -> Self {
        Self {
            value: Some(value),
            error: None,
        }
    }

    pub fn err(message: &str) -> Self {
        Self {
            value: None,
            error: Some(RayError::new(RustStatusCode::InvalidArgument, message)),
        }
    }

    pub fn is_ok(&self) -> bool {
        self.value.is_some()
    }

    pub fn value(&self) -> i64 {
        self.value.unwrap_or(0)
    }
}

/// Parse a string to an i64.
fn string_to_i64_impl(s: &str) -> RustParseResult {
    // Check for empty string
    if s.is_empty() {
        return RustParseResult::err("Cannot parse empty string to integer");
    }

    // Check for leading/trailing whitespace
    if s.starts_with(' ') || s.ends_with(' ') || s.contains(' ') {
        return RustParseResult::err("String contains spaces");
    }

    // Try to parse
    match s.parse::<i64>() {
        Ok(v) => RustParseResult::ok(v),
        Err(_) => RustParseResult::err("Failed to parse string to integer"),
    }
}

/// Parse a string to an i32.
fn string_to_i32_impl(s: &str) -> RustParseResult {
    // Check for empty string
    if s.is_empty() {
        return RustParseResult::err("Cannot parse empty string to integer");
    }

    // Check for leading/trailing whitespace
    if s.starts_with(' ') || s.ends_with(' ') || s.contains(' ') {
        return RustParseResult::err("String contains spaces");
    }

    // Try to parse
    match s.parse::<i32>() {
        Ok(v) => RustParseResult::ok(v as i64),
        Err(_) => RustParseResult::err("Failed to parse string to integer"),
    }
}

/// Parse a string to an i8.
fn string_to_i8_impl(s: &str) -> RustParseResult {
    // Check for empty string
    if s.is_empty() {
        return RustParseResult::err("Cannot parse empty string to integer");
    }

    // Check for leading/trailing whitespace
    if s.starts_with(' ') || s.ends_with(' ') || s.contains(' ') {
        return RustParseResult::err("String contains spaces");
    }

    // Try to parse
    match s.parse::<i8>() {
        Ok(v) => RustParseResult::ok(v as i64),
        Err(_) => RustParseResult::err("Failed to parse string to integer"),
    }
}

#[cxx::bridge(namespace = "ray::ffi")]
mod ffi {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    #[repr(i8)]
    enum StatusCode {
        Ok = 0,
        InvalidArgument = 34,
    }

    extern "Rust" {
        // ExponentialBackoff
        type RustExponentialBackoff;

        fn exponential_backoff_new(
            initial_value: u64,
            multiplier: f64,
            max_value: u64,
        ) -> Box<RustExponentialBackoff>;
        fn exponential_backoff_next(backoff: &mut RustExponentialBackoff) -> u64;
        fn exponential_backoff_current(backoff: &RustExponentialBackoff) -> u64;
        fn exponential_backoff_reset(backoff: &mut RustExponentialBackoff);
        fn exponential_backoff_get_backoff_ms(
            attempt: u64,
            base_ms: u64,
            max_backoff_ms: u64,
        ) -> u64;
        fn exponential_backoff_get_backoff_ms_default(attempt: u64, base_ms: u64) -> u64;

        // StringToInt parsing
        type RustParseResult;

        fn string_to_i64(s: &str) -> Box<RustParseResult>;
        fn string_to_i32(s: &str) -> Box<RustParseResult>;
        fn string_to_i8(s: &str) -> Box<RustParseResult>;
        fn parse_result_is_ok(result: &RustParseResult) -> bool;
        fn parse_result_value(result: &RustParseResult) -> i64;
        fn parse_result_code(result: &RustParseResult) -> StatusCode;

        // Size literals (for verification - these are compile-time in C++)
        fn size_mib(value: u64) -> u64;
        fn size_kb(value: u64) -> u64;
        fn size_gb(value: u64) -> u64;
    }
}

use ffi::StatusCode;

// ExponentialBackoff FFI implementations

fn exponential_backoff_new(
    initial_value: u64,
    multiplier: f64,
    max_value: u64,
) -> Box<RustExponentialBackoff> {
    Box::new(RustExponentialBackoff::new(initial_value, multiplier, max_value))
}

fn exponential_backoff_next(backoff: &mut RustExponentialBackoff) -> u64 {
    backoff.next()
}

fn exponential_backoff_current(backoff: &RustExponentialBackoff) -> u64 {
    backoff.current()
}

fn exponential_backoff_reset(backoff: &mut RustExponentialBackoff) {
    backoff.reset()
}

fn exponential_backoff_get_backoff_ms(attempt: u64, base_ms: u64, max_backoff_ms: u64) -> u64 {
    RustExponentialBackoff::get_backoff_ms(attempt, base_ms, max_backoff_ms)
}

fn exponential_backoff_get_backoff_ms_default(attempt: u64, base_ms: u64) -> u64 {
    RustExponentialBackoff::get_backoff_ms(attempt, base_ms, DEFAULT_MAX_BACKOFF_MS)
}

// StringToInt FFI implementations

fn string_to_i64(s: &str) -> Box<RustParseResult> {
    Box::new(string_to_i64_impl(s))
}

fn string_to_i32(s: &str) -> Box<RustParseResult> {
    Box::new(string_to_i32_impl(s))
}

fn string_to_i8(s: &str) -> Box<RustParseResult> {
    Box::new(string_to_i8_impl(s))
}

fn parse_result_is_ok(result: &RustParseResult) -> bool {
    result.is_ok()
}

fn parse_result_value(result: &RustParseResult) -> i64 {
    result.value()
}

fn parse_result_code(result: &RustParseResult) -> StatusCode {
    if result.is_ok() {
        StatusCode::Ok
    } else {
        StatusCode::InvalidArgument
    }
}

// Size literal implementations

fn size_mib(value: u64) -> u64 {
    value * 1024 * 1024
}

fn size_kb(value: u64) -> u64 {
    value * 1000
}

fn size_gb(value: u64) -> u64 {
    value * 1_000_000_000
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exponential_backoff_basic() {
        let mut backoff = RustExponentialBackoff::new(1, 2.0, 9);
        assert_eq!(backoff.next(), 1);
        assert_eq!(backoff.next(), 2);
        assert_eq!(backoff.next(), 4);
        assert_eq!(backoff.next(), 8);
        assert_eq!(backoff.next(), 9); // Capped at max
        assert_eq!(backoff.next(), 9);
    }

    #[test]
    fn test_exponential_backoff_reset() {
        let mut backoff = RustExponentialBackoff::new(1, 2.0, 9);
        backoff.next();
        backoff.next();
        backoff.reset();
        assert_eq!(backoff.next(), 1);
    }

    #[test]
    fn test_get_backoff_ms() {
        assert_eq!(
            RustExponentialBackoff::get_backoff_ms(0, 157, 60000),
            157 * 1
        );
        assert_eq!(
            RustExponentialBackoff::get_backoff_ms(1, 157, 60000),
            157 * 2
        );
        assert_eq!(
            RustExponentialBackoff::get_backoff_ms(2, 157, 60000),
            157 * 4
        );
        assert_eq!(
            RustExponentialBackoff::get_backoff_ms(3, 157, 60000),
            157 * 8
        );
    }

    #[test]
    fn test_get_backoff_ms_overflow() {
        // High attempt numbers should return max
        for i in 64..100 {
            assert_eq!(
                RustExponentialBackoff::get_backoff_ms(i, 1, 1234),
                1234
            );
        }
    }

    #[test]
    fn test_string_to_i64() {
        let result = string_to_i64_impl("123");
        assert!(result.is_ok());
        assert_eq!(result.value(), 123);

        let result = string_to_i64_impl("-4294967296");
        assert!(result.is_ok());
        assert_eq!(result.value(), -4294967296);

        let result = string_to_i64_impl("4294967296");
        assert!(result.is_ok());
        assert_eq!(result.value(), 4294967296);
    }

    #[test]
    fn test_string_to_int_errors() {
        // Non-number
        let result = string_to_i64_impl("imanumber");
        assert!(!result.is_ok());

        // Empty string
        let result = string_to_i64_impl("");
        assert!(!result.is_ok());

        // Spaces
        let result = string_to_i64_impl(" 1");
        assert!(!result.is_ok());

        let result = string_to_i64_impl("1 ");
        assert!(!result.is_ok());

        let result = string_to_i64_impl("1 2");
        assert!(!result.is_ok());
    }

    #[test]
    fn test_string_to_i8_overflow() {
        // 4294967296 overflows i8
        let result = string_to_i8_impl("4294967296");
        assert!(!result.is_ok());
    }

    #[test]
    fn test_size_literals() {
        assert_eq!(size_mib(2), 2 * 1024 * 1024);
        assert_eq!(size_kb(2), 2000);
        assert_eq!(size_gb(4), 4_000_000_000);
    }
}
