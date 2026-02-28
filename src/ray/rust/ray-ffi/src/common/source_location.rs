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

//! SourceLocation FFI bridge.
//!
//! This implements a source location type that captures file name and line number,
//! matching Ray's SourceLocation struct.

/// A struct representing source code location.
pub struct RustSourceLocation {
    filename: String,
    line_no: i32,
}

impl RustSourceLocation {
    pub fn new() -> Self {
        Self {
            filename: String::new(),
            line_no: 0,
        }
    }

    pub fn with_location(filename: String, line_no: i32) -> Self {
        Self { filename, line_no }
    }

    pub fn is_valid(&self) -> bool {
        !self.filename.is_empty() && self.line_no > 0
    }

    pub fn to_string(&self) -> String {
        if !self.is_valid() {
            String::new()
        } else {
            format!("{}:{}", self.filename, self.line_no)
        }
    }

    pub fn filename(&self) -> &str {
        &self.filename
    }

    pub fn line_no(&self) -> i32 {
        self.line_no
    }
}

impl Default for RustSourceLocation {
    fn default() -> Self {
        Self::new()
    }
}

#[cxx::bridge(namespace = "ray::ffi")]
mod ffi {
    extern "Rust" {
        type RustSourceLocation;

        fn source_location_new() -> Box<RustSourceLocation>;
        fn source_location_with_location(filename: &str, line_no: i32) -> Box<RustSourceLocation>;
        fn source_location_is_valid(loc: &RustSourceLocation) -> bool;
        fn source_location_to_string(loc: &RustSourceLocation) -> String;
        fn source_location_filename(loc: &RustSourceLocation) -> String;
        fn source_location_line_no(loc: &RustSourceLocation) -> i32;
    }
}

fn source_location_new() -> Box<RustSourceLocation> {
    Box::new(RustSourceLocation::new())
}

fn source_location_with_location(filename: &str, line_no: i32) -> Box<RustSourceLocation> {
    Box::new(RustSourceLocation::with_location(filename.to_string(), line_no))
}

fn source_location_is_valid(loc: &RustSourceLocation) -> bool {
    loc.is_valid()
}

fn source_location_to_string(loc: &RustSourceLocation) -> String {
    loc.to_string()
}

fn source_location_filename(loc: &RustSourceLocation) -> String {
    loc.filename().to_string()
}

fn source_location_line_no(loc: &RustSourceLocation) -> i32 {
    loc.line_no()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_location() {
        let loc = RustSourceLocation::new();
        assert!(!loc.is_valid());
        assert_eq!(loc.to_string(), "");
    }

    #[test]
    fn test_valid_location() {
        let loc = RustSourceLocation::with_location("test.rs".to_string(), 42);
        assert!(loc.is_valid());
        assert_eq!(loc.to_string(), "test.rs:42");
    }
}
