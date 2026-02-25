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

//! Status types for Ray operations.
//!
//! This module provides Rust equivalents of Ray's C++ Status types,
//! enabling functional equivalence testing between implementations.

use std::fmt;
use thiserror::Error;

/// Status codes matching the C++ StatusCode enum in status.h.
///
/// The numeric values match the C++ enum for FFI compatibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i8)]
pub enum StatusCode {
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

impl StatusCode {
    /// Returns the string representation of the status code.
    pub fn as_str(&self) -> &'static str {
        match self {
            StatusCode::Ok => "OK",
            StatusCode::OutOfMemory => "OutOfMemory",
            StatusCode::KeyError => "KeyError",
            StatusCode::TypeError => "TypeError",
            StatusCode::Invalid => "Invalid",
            StatusCode::IOError => "IOError",
            StatusCode::UnknownError => "UnknownError",
            StatusCode::NotImplemented => "NotImplemented",
            StatusCode::RedisError => "RedisError",
            StatusCode::TimedOut => "TimedOut",
            StatusCode::Interrupted => "Interrupted",
            StatusCode::IntentionalSystemExit => "IntentionalSystemExit",
            StatusCode::UnexpectedSystemExit => "UnexpectedSystemExit",
            StatusCode::CreationTaskError => "CreationTaskError",
            StatusCode::NotFound => "NotFound",
            StatusCode::Disconnected => "Disconnected",
            StatusCode::SchedulingCancelled => "SchedulingCancelled",
            StatusCode::AlreadyExists => "AlreadyExists",
            StatusCode::ObjectExists => "ObjectExists",
            StatusCode::ObjectNotFound => "ObjectNotFound",
            StatusCode::ObjectAlreadySealed => "ObjectAlreadySealed",
            StatusCode::ObjectStoreFull => "ObjectStoreFull",
            StatusCode::TransientObjectStoreFull => "TransientObjectStoreFull",
            StatusCode::OutOfDisk => "OutOfDisk",
            StatusCode::ObjectUnknownOwner => "ObjectUnknownOwner",
            StatusCode::RpcError => "RpcError",
            StatusCode::OutOfResource => "OutOfResource",
            StatusCode::ObjectRefEndOfStream => "ObjectRefEndOfStream",
            StatusCode::Unauthenticated => "Unauthenticated",
            StatusCode::InvalidArgument => "InvalidArgument",
            StatusCode::ChannelError => "ChannelError",
            StatusCode::ChannelTimeoutError => "ChannelTimeoutError",
            StatusCode::PermissionDenied => "PermissionDenied",
        }
    }

    /// Parse a status code from its string representation.
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "OK" => Some(StatusCode::Ok),
            "OutOfMemory" => Some(StatusCode::OutOfMemory),
            "KeyError" => Some(StatusCode::KeyError),
            "TypeError" => Some(StatusCode::TypeError),
            "Invalid" => Some(StatusCode::Invalid),
            "IOError" => Some(StatusCode::IOError),
            "UnknownError" => Some(StatusCode::UnknownError),
            "NotImplemented" => Some(StatusCode::NotImplemented),
            "RedisError" => Some(StatusCode::RedisError),
            "TimedOut" => Some(StatusCode::TimedOut),
            "Interrupted" => Some(StatusCode::Interrupted),
            "IntentionalSystemExit" => Some(StatusCode::IntentionalSystemExit),
            "UnexpectedSystemExit" => Some(StatusCode::UnexpectedSystemExit),
            "CreationTaskError" => Some(StatusCode::CreationTaskError),
            "NotFound" => Some(StatusCode::NotFound),
            "Disconnected" => Some(StatusCode::Disconnected),
            "SchedulingCancelled" => Some(StatusCode::SchedulingCancelled),
            "AlreadyExists" => Some(StatusCode::AlreadyExists),
            "ObjectExists" => Some(StatusCode::ObjectExists),
            "ObjectNotFound" => Some(StatusCode::ObjectNotFound),
            "ObjectAlreadySealed" => Some(StatusCode::ObjectAlreadySealed),
            "ObjectStoreFull" => Some(StatusCode::ObjectStoreFull),
            "TransientObjectStoreFull" => Some(StatusCode::TransientObjectStoreFull),
            "OutOfDisk" => Some(StatusCode::OutOfDisk),
            "ObjectUnknownOwner" => Some(StatusCode::ObjectUnknownOwner),
            "RpcError" => Some(StatusCode::RpcError),
            "OutOfResource" => Some(StatusCode::OutOfResource),
            "ObjectRefEndOfStream" => Some(StatusCode::ObjectRefEndOfStream),
            "Unauthenticated" => Some(StatusCode::Unauthenticated),
            "InvalidArgument" => Some(StatusCode::InvalidArgument),
            "ChannelError" => Some(StatusCode::ChannelError),
            "ChannelTimeoutError" => Some(StatusCode::ChannelTimeoutError),
            "PermissionDenied" => Some(StatusCode::PermissionDenied),
            _ => None,
        }
    }
}

impl fmt::Display for StatusCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Error type for Ray operations.
///
/// This is the Rust equivalent of the C++ `ray::Status` class when it
/// represents an error condition.
#[derive(Debug, Clone, Error)]
#[error("{code}: {message}")]
pub struct RayError {
    /// The status code indicating the type of error.
    pub code: StatusCode,
    /// A human-readable error message.
    pub message: String,
    /// Optional RPC error code (for RpcError status).
    pub rpc_code: Option<i32>,
}

impl RayError {
    /// Create a new error with the given code and message.
    pub fn new(code: StatusCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            rpc_code: None,
        }
    }

    /// Create a new RPC error with the given message and RPC code.
    pub fn rpc_error(message: impl Into<String>, rpc_code: i32) -> Self {
        Self {
            code: StatusCode::RpcError,
            message: message.into(),
            rpc_code: Some(rpc_code),
        }
    }

    /// Create an OutOfMemory error.
    pub fn out_of_memory(message: impl Into<String>) -> Self {
        Self::new(StatusCode::OutOfMemory, message)
    }

    /// Create a KeyError.
    pub fn key_error(message: impl Into<String>) -> Self {
        Self::new(StatusCode::KeyError, message)
    }

    /// Create a TypeError.
    pub fn type_error(message: impl Into<String>) -> Self {
        Self::new(StatusCode::TypeError, message)
    }

    /// Create an Invalid error.
    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new(StatusCode::Invalid, message)
    }

    /// Create an IOError.
    pub fn io_error(message: impl Into<String>) -> Self {
        Self::new(StatusCode::IOError, message)
    }

    /// Create an UnknownError.
    pub fn unknown_error(message: impl Into<String>) -> Self {
        Self::new(StatusCode::UnknownError, message)
    }

    /// Create a NotImplemented error.
    pub fn not_implemented(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NotImplemented, message)
    }

    /// Create a RedisError.
    pub fn redis_error(message: impl Into<String>) -> Self {
        Self::new(StatusCode::RedisError, message)
    }

    /// Create a TimedOut error.
    pub fn timed_out(message: impl Into<String>) -> Self {
        Self::new(StatusCode::TimedOut, message)
    }

    /// Create an Interrupted error.
    pub fn interrupted(message: impl Into<String>) -> Self {
        Self::new(StatusCode::Interrupted, message)
    }

    /// Create an IntentionalSystemExit error.
    pub fn intentional_system_exit(message: impl Into<String>) -> Self {
        Self::new(StatusCode::IntentionalSystemExit, message)
    }

    /// Create an UnexpectedSystemExit error.
    pub fn unexpected_system_exit(message: impl Into<String>) -> Self {
        Self::new(StatusCode::UnexpectedSystemExit, message)
    }

    /// Create a CreationTaskError.
    pub fn creation_task_error(message: impl Into<String>) -> Self {
        Self::new(StatusCode::CreationTaskError, message)
    }

    /// Create a NotFound error.
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NotFound, message)
    }

    /// Create a Disconnected error.
    pub fn disconnected(message: impl Into<String>) -> Self {
        Self::new(StatusCode::Disconnected, message)
    }

    /// Create a SchedulingCancelled error.
    pub fn scheduling_cancelled(message: impl Into<String>) -> Self {
        Self::new(StatusCode::SchedulingCancelled, message)
    }

    /// Create an AlreadyExists error.
    pub fn already_exists(message: impl Into<String>) -> Self {
        Self::new(StatusCode::AlreadyExists, message)
    }

    /// Create an ObjectExists error.
    pub fn object_exists(message: impl Into<String>) -> Self {
        Self::new(StatusCode::ObjectExists, message)
    }

    /// Create an ObjectNotFound error.
    pub fn object_not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::ObjectNotFound, message)
    }

    /// Create an ObjectAlreadySealed error.
    pub fn object_already_sealed(message: impl Into<String>) -> Self {
        Self::new(StatusCode::ObjectAlreadySealed, message)
    }

    /// Create an ObjectStoreFull error.
    pub fn object_store_full(message: impl Into<String>) -> Self {
        Self::new(StatusCode::ObjectStoreFull, message)
    }

    /// Create a TransientObjectStoreFull error.
    pub fn transient_object_store_full(message: impl Into<String>) -> Self {
        Self::new(StatusCode::TransientObjectStoreFull, message)
    }

    /// Create an OutOfDisk error.
    pub fn out_of_disk(message: impl Into<String>) -> Self {
        Self::new(StatusCode::OutOfDisk, message)
    }

    /// Create an ObjectUnknownOwner error.
    pub fn object_unknown_owner(message: impl Into<String>) -> Self {
        Self::new(StatusCode::ObjectUnknownOwner, message)
    }

    /// Create an OutOfResource error.
    pub fn out_of_resource(message: impl Into<String>) -> Self {
        Self::new(StatusCode::OutOfResource, message)
    }

    /// Create an ObjectRefEndOfStream error.
    pub fn object_ref_end_of_stream(message: impl Into<String>) -> Self {
        Self::new(StatusCode::ObjectRefEndOfStream, message)
    }

    /// Create an Unauthenticated error.
    pub fn unauthenticated(message: impl Into<String>) -> Self {
        Self::new(StatusCode::Unauthenticated, message)
    }

    /// Create an InvalidArgument error.
    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::new(StatusCode::InvalidArgument, message)
    }

    /// Create a ChannelError.
    pub fn channel_error(message: impl Into<String>) -> Self {
        Self::new(StatusCode::ChannelError, message)
    }

    /// Create a ChannelTimeoutError.
    pub fn channel_timeout_error(message: impl Into<String>) -> Self {
        Self::new(StatusCode::ChannelTimeoutError, message)
    }

    /// Create a PermissionDenied error.
    pub fn permission_denied(message: impl Into<String>) -> Self {
        Self::new(StatusCode::PermissionDenied, message)
    }

    /// Check if this is a specific error type.
    pub fn is_out_of_memory(&self) -> bool {
        self.code == StatusCode::OutOfMemory
    }

    pub fn is_key_error(&self) -> bool {
        self.code == StatusCode::KeyError
    }

    pub fn is_type_error(&self) -> bool {
        self.code == StatusCode::TypeError
    }

    pub fn is_invalid(&self) -> bool {
        self.code == StatusCode::Invalid
    }

    pub fn is_io_error(&self) -> bool {
        self.code == StatusCode::IOError
    }

    pub fn is_unknown_error(&self) -> bool {
        self.code == StatusCode::UnknownError
    }

    pub fn is_not_implemented(&self) -> bool {
        self.code == StatusCode::NotImplemented
    }

    pub fn is_redis_error(&self) -> bool {
        self.code == StatusCode::RedisError
    }

    pub fn is_timed_out(&self) -> bool {
        self.code == StatusCode::TimedOut
    }

    pub fn is_interrupted(&self) -> bool {
        self.code == StatusCode::Interrupted
    }

    pub fn is_intentional_system_exit(&self) -> bool {
        self.code == StatusCode::IntentionalSystemExit
    }

    pub fn is_unexpected_system_exit(&self) -> bool {
        self.code == StatusCode::UnexpectedSystemExit
    }

    pub fn is_creation_task_error(&self) -> bool {
        self.code == StatusCode::CreationTaskError
    }

    pub fn is_not_found(&self) -> bool {
        self.code == StatusCode::NotFound
    }

    pub fn is_disconnected(&self) -> bool {
        self.code == StatusCode::Disconnected
    }

    pub fn is_scheduling_cancelled(&self) -> bool {
        self.code == StatusCode::SchedulingCancelled
    }

    pub fn is_already_exists(&self) -> bool {
        self.code == StatusCode::AlreadyExists
    }

    pub fn is_object_exists(&self) -> bool {
        self.code == StatusCode::ObjectExists
    }

    pub fn is_object_not_found(&self) -> bool {
        self.code == StatusCode::ObjectNotFound
    }

    pub fn is_object_already_sealed(&self) -> bool {
        self.code == StatusCode::ObjectAlreadySealed
    }

    pub fn is_object_store_full(&self) -> bool {
        self.code == StatusCode::ObjectStoreFull
    }

    pub fn is_transient_object_store_full(&self) -> bool {
        self.code == StatusCode::TransientObjectStoreFull
    }

    pub fn is_out_of_disk(&self) -> bool {
        self.code == StatusCode::OutOfDisk
    }

    pub fn is_object_unknown_owner(&self) -> bool {
        self.code == StatusCode::ObjectUnknownOwner
    }

    pub fn is_rpc_error(&self) -> bool {
        self.code == StatusCode::RpcError
    }

    pub fn is_out_of_resource(&self) -> bool {
        self.code == StatusCode::OutOfResource
    }

    pub fn is_object_ref_end_of_stream(&self) -> bool {
        self.code == StatusCode::ObjectRefEndOfStream
    }

    pub fn is_unauthenticated(&self) -> bool {
        self.code == StatusCode::Unauthenticated
    }

    pub fn is_invalid_argument(&self) -> bool {
        self.code == StatusCode::InvalidArgument
    }

    pub fn is_channel_error(&self) -> bool {
        self.code == StatusCode::ChannelError
    }

    pub fn is_channel_timeout_error(&self) -> bool {
        self.code == StatusCode::ChannelTimeoutError
    }

    pub fn is_permission_denied(&self) -> bool {
        self.code == StatusCode::PermissionDenied
    }

    /// Returns a string representation matching C++ Status::ToString().
    pub fn to_string(&self) -> String {
        format!("{}: {}", self.code.as_str(), self.message)
    }
}

/// Result type for Ray operations.
pub type Result<T> = std::result::Result<T, RayError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_status_code_values() {
        // Verify numeric values match C++ enum
        assert_eq!(StatusCode::Ok as i8, 0);
        assert_eq!(StatusCode::OutOfMemory as i8, 1);
        assert_eq!(StatusCode::KeyError as i8, 2);
        assert_eq!(StatusCode::TypeError as i8, 3);
        assert_eq!(StatusCode::Invalid as i8, 4);
        assert_eq!(StatusCode::IOError as i8, 5);
        assert_eq!(StatusCode::UnknownError as i8, 9);
        assert_eq!(StatusCode::NotImplemented as i8, 10);
        assert_eq!(StatusCode::TimedOut as i8, 12);
        assert_eq!(StatusCode::NotFound as i8, 17);
        assert_eq!(StatusCode::AlreadyExists as i8, 20);
        assert_eq!(StatusCode::RpcError as i8, 30);
        assert_eq!(StatusCode::InvalidArgument as i8, 34);
        assert_eq!(StatusCode::PermissionDenied as i8, 37);
    }

    #[test]
    fn test_status_code_string_conversion() {
        assert_eq!(StatusCode::Ok.as_str(), "OK");
        assert_eq!(StatusCode::OutOfMemory.as_str(), "OutOfMemory");
        assert_eq!(StatusCode::KeyError.as_str(), "KeyError");

        assert_eq!(StatusCode::from_str("OK"), Some(StatusCode::Ok));
        assert_eq!(
            StatusCode::from_str("OutOfMemory"),
            Some(StatusCode::OutOfMemory)
        );
        assert_eq!(StatusCode::from_str("Invalid"), Some(StatusCode::Invalid));
        assert_eq!(StatusCode::from_str("unknown"), None);
    }

    #[test]
    fn test_ray_error_creation() {
        let err = RayError::key_error("Key not found");
        assert_eq!(err.code, StatusCode::KeyError);
        assert_eq!(err.message, "Key not found");
        assert!(err.is_key_error());
        assert!(!err.is_io_error());
    }

    #[test]
    fn test_ray_error_to_string() {
        let err = RayError::io_error("File not found");
        assert_eq!(err.to_string(), "IOError: File not found");
    }

    #[test]
    fn test_rpc_error() {
        let err = RayError::rpc_error("Connection failed", 14);
        assert_eq!(err.code, StatusCode::RpcError);
        assert_eq!(err.rpc_code, Some(14));
        assert!(err.is_rpc_error());
    }

    #[test]
    fn test_result_type() {
        fn may_fail(succeed: bool) -> Result<i32> {
            if succeed {
                Ok(42)
            } else {
                Err(RayError::invalid("Operation failed"))
            }
        }

        assert_eq!(may_fail(true).unwrap(), 42);
        assert!(may_fail(false).is_err());
    }
}
