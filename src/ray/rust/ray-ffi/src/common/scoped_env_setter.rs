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

//! ScopedEnvSetter FFI bridge.
//!
//! RAII wrapper that sets an environment variable and restores the original
//! value (or unsets it) when dropped.

use std::env;
use std::ffi::OsString;

/// RAII wrapper for environment variable manipulation.
/// Sets the environment variable on creation and restores/unsets on drop.
pub struct RustScopedEnvSetter {
    key: String,
    old_value: Option<OsString>,
}

impl RustScopedEnvSetter {
    /// Create a new scoped environment setter.
    /// Sets the environment variable immediately.
    pub fn new(key: &str, value: &str) -> Self {
        let old_value = env::var_os(key);
        env::set_var(key, value);
        Self {
            key: key.to_string(),
            old_value,
        }
    }
}

impl Drop for RustScopedEnvSetter {
    fn drop(&mut self) {
        match &self.old_value {
            Some(old) => env::set_var(&self.key, old),
            None => env::remove_var(&self.key),
        }
    }
}

#[cxx::bridge(namespace = "ffi")]
mod ffi {
    extern "Rust" {
        type RustScopedEnvSetter;

        fn scoped_env_setter_new(key: &str, value: &str) -> Box<RustScopedEnvSetter>;
        fn get_env_var(key: &str) -> String;
        fn env_var_exists(key: &str) -> bool;
    }
}

fn scoped_env_setter_new(key: &str, value: &str) -> Box<RustScopedEnvSetter> {
    Box::new(RustScopedEnvSetter::new(key, value))
}

fn get_env_var(key: &str) -> String {
    env::var(key).unwrap_or_default()
}

fn env_var_exists(key: &str) -> bool {
    env::var_os(key).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic() {
        let key = "TEST_SCOPED_ENV_VAR_12345";
        let value = "test_value";

        // Initially not set
        assert!(!env_var_exists(key));

        {
            let _setter = RustScopedEnvSetter::new(key, value);
            assert!(env_var_exists(key));
            assert_eq!(get_env_var(key), value);
        }

        // Restored to unset
        assert!(!env_var_exists(key));
    }

    #[test]
    fn test_restore_existing() {
        let key = "TEST_SCOPED_ENV_VAR_EXISTING";
        let original = "original_value";
        let new_value = "new_value";

        env::set_var(key, original);
        assert_eq!(get_env_var(key), original);

        {
            let _setter = RustScopedEnvSetter::new(key, new_value);
            assert_eq!(get_env_var(key), new_value);
        }

        // Restored to original
        assert_eq!(get_env_var(key), original);

        // Cleanup
        env::remove_var(key);
    }
}
