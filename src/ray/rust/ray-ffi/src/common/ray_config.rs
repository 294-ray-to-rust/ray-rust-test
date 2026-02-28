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

//! RayConfig utilities FFI bridge.
//!
//! This implements config value conversion utilities matching Ray's ray_config.h

/// Convert a comma-separated string to a vector of trimmed strings.
pub fn convert_to_string_vector(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(|s| s.trim().to_string())
        .collect()
}

/// Convert a string to an integer.
pub fn convert_to_int(value: &str) -> Result<i64, String> {
    value
        .trim()
        .parse::<i64>()
        .map_err(|e| format!("Cannot parse \"{}\" to int: {}", value, e))
}

/// Convert a string to a boolean.
pub fn convert_to_bool(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    lower == "true" || lower == "1"
}

/// Convert a string to a float.
pub fn convert_to_float(value: &str) -> Result<f64, String> {
    value
        .trim()
        .parse::<f64>()
        .map_err(|e| format!("Cannot parse \"{}\" to float: {}", value, e))
}

#[cxx::bridge(namespace = "ray::ffi")]
mod ffi {
    extern "Rust" {
        fn rust_convert_to_string_vector(value: &str) -> Vec<String>;
        fn rust_convert_to_int(value: &str) -> i64;
        fn rust_convert_to_bool(value: &str) -> bool;
        fn rust_convert_to_float(value: &str) -> f64;
    }
}

fn rust_convert_to_string_vector(value: &str) -> Vec<String> {
    convert_to_string_vector(value)
}

fn rust_convert_to_int(value: &str) -> i64 {
    convert_to_int(value).unwrap_or(0)
}

fn rust_convert_to_bool(value: &str) -> bool {
    convert_to_bool(value)
}

fn rust_convert_to_float(value: &str) -> f64 {
    convert_to_float(value).unwrap_or(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_to_string_vector() {
        let result = convert_to_string_vector("no_spaces, with spaces ");
        assert_eq!(result, vec!["no_spaces", "with spaces"]);
    }

    #[test]
    fn test_convert_to_bool() {
        assert!(convert_to_bool("true"));
        assert!(convert_to_bool("True"));
        assert!(convert_to_bool("TRUE"));
        assert!(convert_to_bool("1"));
        assert!(!convert_to_bool("false"));
        assert!(!convert_to_bool("0"));
    }

    #[test]
    fn test_convert_to_int() {
        assert_eq!(convert_to_int("42").unwrap(), 42);
        assert_eq!(convert_to_int("-10").unwrap(), -10);
        assert!(convert_to_int("not_a_number").is_err());
    }
}
