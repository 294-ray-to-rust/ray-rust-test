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

//! Callback wrapper patterns for FFI.
//!
//! This module provides utilities for wrapping Rust callbacks to be
//! called from C++ code, and for wrapping C++ callbacks to be called
//! from Rust code.

use std::ffi::c_void;

/// A type-erased callback that can be passed across the FFI boundary.
///
/// This is used when C++ needs to pass a callback to Rust code that
/// will be invoked later.
pub struct FfiCallback {
    /// The callback function pointer.
    callback: unsafe extern "C" fn(*mut c_void, i32),
    /// User data to be passed to the callback.
    user_data: *mut c_void,
}

unsafe impl Send for FfiCallback {}
unsafe impl Sync for FfiCallback {}

impl FfiCallback {
    /// Create a new FFI callback.
    ///
    /// # Safety
    ///
    /// The caller must ensure that:
    /// - The callback function pointer is valid.
    /// - The user_data pointer remains valid until the callback is invoked.
    pub unsafe fn new(
        callback: unsafe extern "C" fn(*mut c_void, i32),
        user_data: *mut c_void,
    ) -> Self {
        Self {
            callback,
            user_data,
        }
    }

    /// Invoke the callback with a result code.
    ///
    /// # Safety
    ///
    /// The caller must ensure that:
    /// - The callback has not been invoked previously (callbacks are single-use).
    /// - The user_data pointer is still valid.
    pub unsafe fn invoke(self, result: i32) {
        (self.callback)(self.user_data, result);
    }
}

/// A wrapper for Rust closures that can be passed to C++ as a callback.
///
/// This is used when Rust needs to pass a callback to C++ code that
/// will be invoked later.
pub struct RustCallback<F> {
    closure: F,
}

impl<F: FnOnce(i32)> RustCallback<F> {
    /// Create a new Rust callback wrapper.
    pub fn new(closure: F) -> Self {
        Self { closure }
    }

    /// Get the raw callback function pointer and user data for FFI.
    ///
    /// The returned function pointer and user_data can be passed to C++.
    /// The C++ code should call the function pointer with the user_data
    /// as the first argument.
    pub fn into_raw(self) -> (unsafe extern "C" fn(*mut c_void, i32), *mut c_void) {
        let boxed = Box::new(self.closure);
        let user_data = Box::into_raw(boxed) as *mut c_void;

        (Self::invoke_callback::<F>, user_data)
    }

    /// The C-compatible callback function that invokes the Rust closure.
    unsafe extern "C" fn invoke_callback<T: FnOnce(i32)>(user_data: *mut c_void, result: i32) {
        let closure = Box::from_raw(user_data as *mut T);
        closure(result);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI32, Ordering};
    use std::sync::Arc;

    #[test]
    fn test_rust_callback() {
        let result = Arc::new(AtomicI32::new(0));
        let result_clone = result.clone();

        let callback = RustCallback::new(move |r| {
            result_clone.store(r, Ordering::SeqCst);
        });

        let (fn_ptr, user_data) = callback.into_raw();

        // Simulate C++ calling the callback
        unsafe {
            fn_ptr(user_data, 42);
        }

        assert_eq!(result.load(Ordering::SeqCst), 42);
    }
}
