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

//! Filesystem utilities FFI bridge.
//!
//! This implements filesystem-related functions matching Ray's filesystem.h

use std::path::Path;

/// Get the filename portion of a path (equivalent to Python's os.path.basename).
pub fn get_file_name(path: &str) -> String {
    if path.is_empty() {
        return String::new();
    }

    // Handle special cases
    if path == "." || path == ".." {
        return path.to_string();
    }

    // Use std::path for parsing
    let p = Path::new(path);

    // Check if path ends with separator
    if path.ends_with('/') || path.ends_with('\\') {
        return String::new();
    }

    // Get the file name component
    p.file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_default()
}

/// Get the user's temporary directory.
pub fn get_user_temp_dir() -> String {
    std::env::temp_dir().to_string_lossy().to_string()
}

/// Check if a character is a directory separator.
pub fn is_dir_sep(ch: char) -> bool {
    ch == '/' || ch == '\\'
}

/// Join path components together.
pub fn join_paths(base: &str, component: &str) -> String {
    if component.is_empty() {
        return base.to_string();
    }

    // Handle component starting with separator - extract just the filename
    let component_to_add = if component.starts_with('/') || component.starts_with('\\') {
        Path::new(component)
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_default()
    } else {
        component.to_string()
    };

    if base.is_empty() {
        return component_to_add;
    }

    let p = Path::new(base);
    p.join(&component_to_add).to_string_lossy().to_string()
}

/// Join multiple path components.
pub fn join_paths_multi(parts: Vec<&str>) -> String {
    if parts.is_empty() {
        return String::new();
    }

    let mut result = parts[0].to_string();
    for part in parts.iter().skip(1) {
        result = join_paths(&result, part);
    }
    result
}

#[cxx::bridge(namespace = "ray::ffi")]
mod ffi {
    extern "Rust" {
        fn rust_get_file_name(path: &str) -> String;
        fn rust_get_user_temp_dir() -> String;
        fn rust_is_dir_sep(ch: u8) -> bool;
        fn rust_join_paths(base: &str, component: &str) -> String;
    }
}

fn rust_get_file_name(path: &str) -> String {
    get_file_name(path)
}

fn rust_get_user_temp_dir() -> String {
    get_user_temp_dir()
}

fn rust_is_dir_sep(ch: u8) -> bool {
    is_dir_sep(ch as char)
}

fn rust_join_paths(base: &str, component: &str) -> String {
    join_paths(base, component)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_file_name() {
        assert_eq!(get_file_name("."), ".");
        assert_eq!(get_file_name(".."), "..");
        assert_eq!(get_file_name("foo/bar"), "bar");
        assert_eq!(get_file_name("///bar"), "bar");
        assert_eq!(get_file_name("///bar/"), "");
    }

    #[test]
    fn test_is_dir_sep() {
        assert!(is_dir_sep('/'));
        assert!(is_dir_sep('\\'));
        assert!(!is_dir_sep('a'));
        assert!(!is_dir_sep('.'));
    }

    #[test]
    fn test_join_paths() {
        assert_eq!(join_paths("/tmp", "hello"), "/tmp/hello");
        assert_eq!(join_paths("/tmp", ""), "/tmp");
        assert_eq!(join_paths("", "hello"), "hello");
    }
}
