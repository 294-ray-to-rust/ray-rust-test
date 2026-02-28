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

//! StatusOr FFI bridge for C++ interop.
//!
//! This module provides FFI types that represent Result<T, RayError> for various T types.
//! In Rust, we use Result<T, RayError> naturally, but for C++ interop via CXX, we need
//! concrete types since CXX doesn't support generics.

use ray_common::status::{RayError, StatusCode as RustStatusCode};

/// Result type for i32 values.
pub struct RustResultI32 {
    value: Option<i32>,
    error: Option<RayError>,
}

impl RustResultI32 {
    pub fn ok(value: i32) -> Self {
        Self {
            value: Some(value),
            error: None,
        }
    }

    pub fn err(error: RayError) -> Self {
        Self {
            value: None,
            error: Some(error),
        }
    }

    pub fn is_ok(&self) -> bool {
        self.value.is_some()
    }

    pub fn value(&self) -> i32 {
        self.value.unwrap_or(0)
    }

    pub fn value_or(&self, default: i32) -> i32 {
        self.value.unwrap_or(default)
    }

    pub fn value_or_default(&self) -> i32 {
        self.value.unwrap_or_default()
    }
}

/// Result type for i64 values.
pub struct RustResultI64 {
    value: Option<i64>,
    error: Option<RayError>,
}

impl RustResultI64 {
    pub fn ok(value: i64) -> Self {
        Self {
            value: Some(value),
            error: None,
        }
    }

    pub fn err(error: RayError) -> Self {
        Self {
            value: None,
            error: Some(error),
        }
    }

    pub fn is_ok(&self) -> bool {
        self.value.is_some()
    }

    pub fn value(&self) -> i64 {
        self.value.unwrap_or(0)
    }

    pub fn value_or(&self, default: i64) -> i64 {
        self.value.unwrap_or(default)
    }

    pub fn value_or_default(&self) -> i64 {
        self.value.unwrap_or_default()
    }
}

/// Result type for String values.
pub struct RustResultString {
    value: Option<String>,
    error: Option<RayError>,
}

impl RustResultString {
    pub fn ok(value: String) -> Self {
        Self {
            value: Some(value),
            error: None,
        }
    }

    pub fn err(error: RayError) -> Self {
        Self {
            value: None,
            error: Some(error),
        }
    }

    pub fn is_ok(&self) -> bool {
        self.value.is_some()
    }

    pub fn value(&self) -> &str {
        self.value.as_deref().unwrap_or("")
    }

    pub fn value_or(&self, default: &str) -> String {
        self.value.clone().unwrap_or_else(|| default.to_string())
    }

    pub fn value_or_default(&self) -> String {
        self.value.clone().unwrap_or_default()
    }
}

#[cxx::bridge(namespace = "ray::ffi")]
mod ffi {
    // Re-export StatusCode from status module
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    #[repr(i8)]
    enum StatusCode {
        Ok = 0,
        OutOfMemory = 1,
        KeyError = 2,
        TypeError = 3,
        Invalid = 4,
        IOError = 5,
        UnknownError = 9,
        NotImplemented = 10,
        RedisError = 11,
        TimedOut = 12,
        Interrupted = 13,
        IntentionalSystemExit = 14,
        UnexpectedSystemExit = 15,
        CreationTaskError = 16,
        NotFound = 17,
        Disconnected = 18,
        SchedulingCancelled = 19,
        AlreadyExists = 20,
        ObjectExists = 21,
        ObjectNotFound = 22,
        ObjectAlreadySealed = 23,
        ObjectStoreFull = 24,
        TransientObjectStoreFull = 25,
        OutOfDisk = 28,
        ObjectUnknownOwner = 29,
        RpcError = 30,
        OutOfResource = 31,
        ObjectRefEndOfStream = 32,
        Unauthenticated = 33,
        InvalidArgument = 34,
        ChannelError = 35,
        ChannelTimeoutError = 36,
        PermissionDenied = 37,
    }

    extern "Rust" {
        // Result<i32, RayError>
        type RustResultI32;

        fn result_i32_ok(value: i32) -> Box<RustResultI32>;
        fn result_i32_error(code: StatusCode, msg: &str) -> Box<RustResultI32>;
        fn result_i32_is_ok(result: &RustResultI32) -> bool;
        fn result_i32_code(result: &RustResultI32) -> StatusCode;
        fn result_i32_message(result: &RustResultI32) -> &str;
        fn result_i32_value(result: &RustResultI32) -> i32;
        fn result_i32_value_or(result: &RustResultI32, default_val: i32) -> i32;
        fn result_i32_value_or_default(result: &RustResultI32) -> i32;
        fn result_i32_clone(result: &RustResultI32) -> Box<RustResultI32>;

        // Result<i64, RayError>
        type RustResultI64;

        fn result_i64_ok(value: i64) -> Box<RustResultI64>;
        fn result_i64_error(code: StatusCode, msg: &str) -> Box<RustResultI64>;
        fn result_i64_is_ok(result: &RustResultI64) -> bool;
        fn result_i64_code(result: &RustResultI64) -> StatusCode;
        fn result_i64_message(result: &RustResultI64) -> &str;
        fn result_i64_value(result: &RustResultI64) -> i64;
        fn result_i64_value_or(result: &RustResultI64, default_val: i64) -> i64;
        fn result_i64_value_or_default(result: &RustResultI64) -> i64;
        fn result_i64_clone(result: &RustResultI64) -> Box<RustResultI64>;

        // Result<String, RayError>
        type RustResultString;

        fn result_string_ok(value: &str) -> Box<RustResultString>;
        fn result_string_error(code: StatusCode, msg: &str) -> Box<RustResultString>;
        fn result_string_is_ok(result: &RustResultString) -> bool;
        fn result_string_code(result: &RustResultString) -> StatusCode;
        fn result_string_message(result: &RustResultString) -> &str;
        fn result_string_value(result: &RustResultString) -> &str;
        fn result_string_value_or(result: &RustResultString, default_val: &str) -> String;
        fn result_string_value_or_default(result: &RustResultString) -> String;
        fn result_string_clone(result: &RustResultString) -> Box<RustResultString>;

        // Utility functions that return results (for testing)
        fn get_value_or_error(return_error: bool) -> Box<RustResultI32>;
        fn divide(a: i32, b: i32) -> Box<RustResultI32>;
        fn parse_int(s: &str) -> Box<RustResultI32>;
    }
}

use ffi::StatusCode;

/// Convert FFI StatusCode to Rust StatusCode.
fn ffi_to_rust_code(code: StatusCode) -> RustStatusCode {
    match code {
        StatusCode::Ok => RustStatusCode::Ok,
        StatusCode::OutOfMemory => RustStatusCode::OutOfMemory,
        StatusCode::KeyError => RustStatusCode::KeyError,
        StatusCode::TypeError => RustStatusCode::TypeError,
        StatusCode::Invalid => RustStatusCode::Invalid,
        StatusCode::IOError => RustStatusCode::IOError,
        StatusCode::UnknownError => RustStatusCode::UnknownError,
        StatusCode::NotImplemented => RustStatusCode::NotImplemented,
        StatusCode::RedisError => RustStatusCode::RedisError,
        StatusCode::TimedOut => RustStatusCode::TimedOut,
        StatusCode::Interrupted => RustStatusCode::Interrupted,
        StatusCode::IntentionalSystemExit => RustStatusCode::IntentionalSystemExit,
        StatusCode::UnexpectedSystemExit => RustStatusCode::UnexpectedSystemExit,
        StatusCode::CreationTaskError => RustStatusCode::CreationTaskError,
        StatusCode::NotFound => RustStatusCode::NotFound,
        StatusCode::Disconnected => RustStatusCode::Disconnected,
        StatusCode::SchedulingCancelled => RustStatusCode::SchedulingCancelled,
        StatusCode::AlreadyExists => RustStatusCode::AlreadyExists,
        StatusCode::ObjectExists => RustStatusCode::ObjectExists,
        StatusCode::ObjectNotFound => RustStatusCode::ObjectNotFound,
        StatusCode::ObjectAlreadySealed => RustStatusCode::ObjectAlreadySealed,
        StatusCode::ObjectStoreFull => RustStatusCode::ObjectStoreFull,
        StatusCode::TransientObjectStoreFull => RustStatusCode::TransientObjectStoreFull,
        StatusCode::OutOfDisk => RustStatusCode::OutOfDisk,
        StatusCode::ObjectUnknownOwner => RustStatusCode::ObjectUnknownOwner,
        StatusCode::RpcError => RustStatusCode::RpcError,
        StatusCode::OutOfResource => RustStatusCode::OutOfResource,
        StatusCode::ObjectRefEndOfStream => RustStatusCode::ObjectRefEndOfStream,
        StatusCode::Unauthenticated => RustStatusCode::Unauthenticated,
        StatusCode::InvalidArgument => RustStatusCode::InvalidArgument,
        StatusCode::ChannelError => RustStatusCode::ChannelError,
        StatusCode::ChannelTimeoutError => RustStatusCode::ChannelTimeoutError,
        StatusCode::PermissionDenied => RustStatusCode::PermissionDenied,
        _ => RustStatusCode::UnknownError,
    }
}

/// Convert Rust StatusCode to FFI StatusCode.
fn rust_to_ffi_code(code: RustStatusCode) -> StatusCode {
    match code {
        RustStatusCode::Ok => StatusCode::Ok,
        RustStatusCode::OutOfMemory => StatusCode::OutOfMemory,
        RustStatusCode::KeyError => StatusCode::KeyError,
        RustStatusCode::TypeError => StatusCode::TypeError,
        RustStatusCode::Invalid => StatusCode::Invalid,
        RustStatusCode::IOError => StatusCode::IOError,
        RustStatusCode::UnknownError => StatusCode::UnknownError,
        RustStatusCode::NotImplemented => StatusCode::NotImplemented,
        RustStatusCode::RedisError => StatusCode::RedisError,
        RustStatusCode::TimedOut => StatusCode::TimedOut,
        RustStatusCode::Interrupted => StatusCode::Interrupted,
        RustStatusCode::IntentionalSystemExit => StatusCode::IntentionalSystemExit,
        RustStatusCode::UnexpectedSystemExit => StatusCode::UnexpectedSystemExit,
        RustStatusCode::CreationTaskError => StatusCode::CreationTaskError,
        RustStatusCode::NotFound => StatusCode::NotFound,
        RustStatusCode::Disconnected => StatusCode::Disconnected,
        RustStatusCode::SchedulingCancelled => StatusCode::SchedulingCancelled,
        RustStatusCode::AlreadyExists => StatusCode::AlreadyExists,
        RustStatusCode::ObjectExists => StatusCode::ObjectExists,
        RustStatusCode::ObjectNotFound => StatusCode::ObjectNotFound,
        RustStatusCode::ObjectAlreadySealed => StatusCode::ObjectAlreadySealed,
        RustStatusCode::ObjectStoreFull => StatusCode::ObjectStoreFull,
        RustStatusCode::TransientObjectStoreFull => StatusCode::TransientObjectStoreFull,
        RustStatusCode::OutOfDisk => StatusCode::OutOfDisk,
        RustStatusCode::ObjectUnknownOwner => StatusCode::ObjectUnknownOwner,
        RustStatusCode::RpcError => StatusCode::RpcError,
        RustStatusCode::OutOfResource => StatusCode::OutOfResource,
        RustStatusCode::ObjectRefEndOfStream => StatusCode::ObjectRefEndOfStream,
        RustStatusCode::Unauthenticated => StatusCode::Unauthenticated,
        RustStatusCode::InvalidArgument => StatusCode::InvalidArgument,
        RustStatusCode::ChannelError => StatusCode::ChannelError,
        RustStatusCode::ChannelTimeoutError => StatusCode::ChannelTimeoutError,
        RustStatusCode::PermissionDenied => StatusCode::PermissionDenied,
    }
}

// RustResultI32 FFI implementations

fn result_i32_ok(value: i32) -> Box<RustResultI32> {
    Box::new(RustResultI32::ok(value))
}

fn result_i32_error(code: StatusCode, msg: &str) -> Box<RustResultI32> {
    let rust_code = ffi_to_rust_code(code);
    Box::new(RustResultI32::err(RayError::new(rust_code, msg)))
}

fn result_i32_is_ok(result: &RustResultI32) -> bool {
    result.is_ok()
}

fn result_i32_code(result: &RustResultI32) -> StatusCode {
    match &result.error {
        None => StatusCode::Ok,
        Some(err) => rust_to_ffi_code(err.code),
    }
}

fn result_i32_message(result: &RustResultI32) -> &str {
    match &result.error {
        None => "",
        Some(err) => &err.message,
    }
}

fn result_i32_value(result: &RustResultI32) -> i32 {
    result.value()
}

fn result_i32_value_or(result: &RustResultI32, default_val: i32) -> i32 {
    result.value_or(default_val)
}

fn result_i32_value_or_default(result: &RustResultI32) -> i32 {
    result.value_or_default()
}

fn result_i32_clone(result: &RustResultI32) -> Box<RustResultI32> {
    Box::new(RustResultI32 {
        value: result.value,
        error: result.error.clone(),
    })
}

// RustResultI64 FFI implementations

fn result_i64_ok(value: i64) -> Box<RustResultI64> {
    Box::new(RustResultI64::ok(value))
}

fn result_i64_error(code: StatusCode, msg: &str) -> Box<RustResultI64> {
    let rust_code = ffi_to_rust_code(code);
    Box::new(RustResultI64::err(RayError::new(rust_code, msg)))
}

fn result_i64_is_ok(result: &RustResultI64) -> bool {
    result.is_ok()
}

fn result_i64_code(result: &RustResultI64) -> StatusCode {
    match &result.error {
        None => StatusCode::Ok,
        Some(err) => rust_to_ffi_code(err.code),
    }
}

fn result_i64_message(result: &RustResultI64) -> &str {
    match &result.error {
        None => "",
        Some(err) => &err.message,
    }
}

fn result_i64_value(result: &RustResultI64) -> i64 {
    result.value()
}

fn result_i64_value_or(result: &RustResultI64, default_val: i64) -> i64 {
    result.value_or(default_val)
}

fn result_i64_value_or_default(result: &RustResultI64) -> i64 {
    result.value_or_default()
}

fn result_i64_clone(result: &RustResultI64) -> Box<RustResultI64> {
    Box::new(RustResultI64 {
        value: result.value,
        error: result.error.clone(),
    })
}

// RustResultString FFI implementations

fn result_string_ok(value: &str) -> Box<RustResultString> {
    Box::new(RustResultString::ok(value.to_string()))
}

fn result_string_error(code: StatusCode, msg: &str) -> Box<RustResultString> {
    let rust_code = ffi_to_rust_code(code);
    Box::new(RustResultString::err(RayError::new(rust_code, msg)))
}

fn result_string_is_ok(result: &RustResultString) -> bool {
    result.is_ok()
}

fn result_string_code(result: &RustResultString) -> StatusCode {
    match &result.error {
        None => StatusCode::Ok,
        Some(err) => rust_to_ffi_code(err.code),
    }
}

fn result_string_message(result: &RustResultString) -> &str {
    match &result.error {
        None => "",
        Some(err) => &err.message,
    }
}

fn result_string_value(result: &RustResultString) -> &str {
    result.value()
}

fn result_string_value_or(result: &RustResultString, default_val: &str) -> String {
    result.value_or(default_val)
}

fn result_string_value_or_default(result: &RustResultString) -> String {
    result.value_or_default()
}

fn result_string_clone(result: &RustResultString) -> Box<RustResultString> {
    Box::new(RustResultString {
        value: result.value.clone(),
        error: result.error.clone(),
    })
}

// Utility functions that return results (for testing monadic operations)

fn get_value_or_error(return_error: bool) -> Box<RustResultI32> {
    if return_error {
        result_i32_error(StatusCode::Invalid, "Invalid error status.")
    } else {
        result_i32_ok(1)
    }
}

fn divide(a: i32, b: i32) -> Box<RustResultI32> {
    if b == 0 {
        result_i32_error(StatusCode::Invalid, "Division by zero")
    } else {
        result_i32_ok(a / b)
    }
}

fn parse_int(s: &str) -> Box<RustResultI32> {
    match s.parse::<i32>() {
        Ok(v) => result_i32_ok(v),
        Err(_) => result_i32_error(StatusCode::Invalid, "Failed to parse integer"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_result_i32_ok() {
        let result = result_i32_ok(42);
        assert!(result_i32_is_ok(&result));
        assert_eq!(result_i32_value(&result), 42);
        assert_eq!(result_i32_code(&result), StatusCode::Ok);
    }

    #[test]
    fn test_result_i32_error() {
        let result = result_i32_error(StatusCode::InvalidArgument, "bad arg");
        assert!(!result_i32_is_ok(&result));
        assert_eq!(result_i32_code(&result), StatusCode::InvalidArgument);
        assert_eq!(result_i32_message(&result), "bad arg");
    }

    #[test]
    fn test_result_i32_value_or() {
        let ok_result = result_i32_ok(42);
        assert_eq!(result_i32_value_or(&ok_result, 100), 42);

        let err_result = result_i32_error(StatusCode::Invalid, "error");
        assert_eq!(result_i32_value_or(&err_result, 100), 100);
    }

    #[test]
    fn test_divide() {
        let result = divide(10, 2);
        assert!(result_i32_is_ok(&result));
        assert_eq!(result_i32_value(&result), 5);

        let result = divide(10, 0);
        assert!(!result_i32_is_ok(&result));
        assert_eq!(result_i32_code(&result), StatusCode::Invalid);
    }

    #[test]
    fn test_parse_int() {
        let result = parse_int("123");
        assert!(result_i32_is_ok(&result));
        assert_eq!(result_i32_value(&result), 123);

        let result = parse_int("not a number");
        assert!(!result_i32_is_ok(&result));
    }
}
