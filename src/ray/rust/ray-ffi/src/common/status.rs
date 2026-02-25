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

//! Status FFI bridge for C++ interop.
//!
//! This module provides a CXX bridge that exposes the Rust Status
//! implementation to C++ code.

use ray_common::status::{RayError, StatusCode as RustStatusCode};

/// Wrapper type for RayError that can be used across the FFI boundary.
pub struct RustStatus {
    inner: Option<RayError>,
}

impl RustStatus {
    /// Create an OK status.
    pub fn ok() -> Self {
        Self { inner: None }
    }

    /// Create an error status.
    pub fn error(error: RayError) -> Self {
        Self { inner: Some(error) }
    }

    /// Check if status is OK.
    pub fn is_ok(&self) -> bool {
        self.inner.is_none()
    }

    /// Get the status code.
    pub fn code(&self) -> i8 {
        match &self.inner {
            None => 0, // OK
            Some(err) => err.code as i8,
        }
    }

    /// Get the error message.
    pub fn message(&self) -> &str {
        match &self.inner {
            None => "",
            Some(err) => &err.message,
        }
    }

    /// Get the RPC error code (if applicable).
    pub fn rpc_code(&self) -> i32 {
        match &self.inner {
            None => -1,
            Some(err) => err.rpc_code.unwrap_or(-1),
        }
    }

    /// Convert to string representation.
    pub fn to_string(&self) -> String {
        match &self.inner {
            None => "OK".to_string(),
            Some(err) => err.to_string(),
        }
    }
}

#[cxx::bridge(namespace = "ray::ffi")]
mod ffi {
    /// Status codes exposed to C++.
    /// These must match the C++ StatusCode enum values.
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
        type RustStatus;

        /// Create an OK status.
        fn status_ok() -> Box<RustStatus>;

        /// Create an error status with the given code and message.
        fn status_error(code: StatusCode, msg: &str) -> Box<RustStatus>;

        /// Create an RPC error status.
        fn status_rpc_error(msg: &str, rpc_code: i32) -> Box<RustStatus>;

        /// Check if status is OK.
        fn status_is_ok(status: &RustStatus) -> bool;

        /// Get the status code.
        fn status_code(status: &RustStatus) -> StatusCode;

        /// Get the error message.
        fn status_message(status: &RustStatus) -> &str;

        /// Get the RPC error code.
        fn status_rpc_code(status: &RustStatus) -> i32;

        /// Convert status to string.
        fn status_to_string(status: &RustStatus) -> String;

        /// Clone a status.
        fn status_clone(status: &RustStatus) -> Box<RustStatus>;
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

// FFI function implementations

fn status_ok() -> Box<RustStatus> {
    Box::new(RustStatus::ok())
}

fn status_error(code: StatusCode, msg: &str) -> Box<RustStatus> {
    let rust_code = ffi_to_rust_code(code);
    if rust_code == RustStatusCode::Ok {
        return Box::new(RustStatus::ok());
    }
    Box::new(RustStatus::error(RayError::new(rust_code, msg)))
}

fn status_rpc_error(msg: &str, rpc_code: i32) -> Box<RustStatus> {
    Box::new(RustStatus::error(RayError::rpc_error(msg, rpc_code)))
}

fn status_is_ok(status: &RustStatus) -> bool {
    status.is_ok()
}

fn status_code(status: &RustStatus) -> StatusCode {
    match &status.inner {
        None => StatusCode::Ok,
        Some(err) => rust_to_ffi_code(err.code),
    }
}

fn status_message(status: &RustStatus) -> &str {
    status.message()
}

fn status_rpc_code(status: &RustStatus) -> i32 {
    status.rpc_code()
}

fn status_to_string(status: &RustStatus) -> String {
    status.to_string()
}

fn status_clone(status: &RustStatus) -> Box<RustStatus> {
    Box::new(RustStatus {
        inner: status.inner.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_status_ok() {
        let status = status_ok();
        assert!(status_is_ok(&status));
        assert_eq!(status_code(&status), StatusCode::Ok);
        assert_eq!(status_message(&status), "");
    }

    #[test]
    fn test_status_error() {
        let status = status_error(StatusCode::KeyError, "Key not found");
        assert!(!status_is_ok(&status));
        assert_eq!(status_code(&status), StatusCode::KeyError);
        assert_eq!(status_message(&status), "Key not found");
    }

    #[test]
    fn test_status_rpc_error() {
        let status = status_rpc_error("RPC failed", 14);
        assert!(!status_is_ok(&status));
        assert_eq!(status_code(&status), StatusCode::RpcError);
        assert_eq!(status_rpc_code(&status), 14);
    }

    #[test]
    fn test_status_clone() {
        let original = status_error(StatusCode::IOError, "IO error");
        let cloned = status_clone(&original);
        assert_eq!(status_code(&original), status_code(&cloned));
        assert_eq!(status_message(&original), status_message(&cloned));
    }
}
