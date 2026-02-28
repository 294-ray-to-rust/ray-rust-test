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

//! FFI bindings for spilled object utilities.
//!
//! Implements URL parsing and byte conversion utilities used by SpilledObjectReader.

#[cxx::bridge(namespace = "ray::ffi")]
mod ffi {
    /// Result of parsing an object URL
    pub struct ParsedObjectUrl {
        pub success: bool,
        pub file_path: String,
        pub offset: u64,
        pub size: u64,
    }

    extern "Rust" {
        /// Parse an object URL in the format: {path}?offset={offset}&size={size}
        fn parse_object_url(url: &str) -> ParsedObjectUrl;

        /// Convert 8 bytes (little-endian) to uint64
        fn bytes_to_uint64(bytes: &[u8]) -> u64;

        /// Calculate the number of chunks needed for a given data size and chunk size
        fn calculate_num_chunks(data_size: u64, chunk_size: u64) -> u64;
    }
}

/// Parse an object URL in the format: {path}?offset={offset}&size={size}
fn parse_object_url(url: &str) -> ffi::ParsedObjectUrl {
    // Find the query string separator
    let query_pos = match url.rfind('?') {
        Some(pos) => pos,
        None => {
            return ffi::ParsedObjectUrl {
                success: false,
                file_path: String::new(),
                offset: 0,
                size: 0,
            };
        }
    };

    let file_path = &url[..query_pos];
    let query_string = &url[query_pos + 1..];

    // Parse query parameters
    let mut offset: Option<u64> = None;
    let mut size: Option<u64> = None;
    let mut param_count = 0;

    for param in query_string.split('&') {
        param_count += 1;
        let parts: Vec<&str> = param.splitn(2, '=').collect();
        if parts.len() != 2 {
            return ffi::ParsedObjectUrl {
                success: false,
                file_path: String::new(),
                offset: 0,
                size: 0,
            };
        }

        match parts[0] {
            "offset" => {
                offset = parts[1].parse().ok();
                if offset.is_none() {
                    return ffi::ParsedObjectUrl {
                        success: false,
                        file_path: String::new(),
                        offset: 0,
                        size: 0,
                    };
                }
            }
            "size" => {
                // Parse as i128 first to check for overflow
                match parts[1].parse::<i128>() {
                    Ok(val) => {
                        if val < 0 || val > i64::MAX as i128 {
                            return ffi::ParsedObjectUrl {
                                success: false,
                                file_path: String::new(),
                                offset: 0,
                                size: 0,
                            };
                        }
                        size = Some(val as u64);
                    }
                    Err(_) => {
                        return ffi::ParsedObjectUrl {
                            success: false,
                            file_path: String::new(),
                            offset: 0,
                            size: 0,
                        };
                    }
                }
            }
            _ => {
                // Unknown parameter - fail
                return ffi::ParsedObjectUrl {
                    success: false,
                    file_path: String::new(),
                    offset: 0,
                    size: 0,
                };
            }
        }
    }

    // Must have exactly 2 parameters
    if param_count != 2 {
        return ffi::ParsedObjectUrl {
            success: false,
            file_path: String::new(),
            offset: 0,
            size: 0,
        };
    }

    match (offset, size) {
        (Some(off), Some(sz)) => ffi::ParsedObjectUrl {
            success: true,
            file_path: file_path.to_string(),
            offset: off,
            size: sz,
        },
        _ => ffi::ParsedObjectUrl {
            success: false,
            file_path: String::new(),
            offset: 0,
            size: 0,
        },
    }
}

/// Convert 8 bytes (little-endian) to uint64
fn bytes_to_uint64(bytes: &[u8]) -> u64 {
    if bytes.len() != 8 {
        return 0;
    }
    u64::from_le_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ])
}

/// Calculate the number of chunks needed for a given data size and chunk size
fn calculate_num_chunks(data_size: u64, chunk_size: u64) -> u64 {
    if chunk_size == 0 || data_size == 0 {
        return 0;
    }
    (data_size + chunk_size - 1) / chunk_size
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_object_url() {
        // Success cases
        let result = parse_object_url("file://path/to/file?offset=123&size=456");
        assert!(result.success);
        assert_eq!(result.file_path, "file://path/to/file");
        assert_eq!(result.offset, 123);
        assert_eq!(result.size, 456);

        let result = parse_object_url("/tmp/file.txt?offset=123&size=456");
        assert!(result.success);
        assert_eq!(result.file_path, "/tmp/file.txt");
        assert_eq!(result.offset, 123);
        assert_eq!(result.size, 456);

        // Large numbers
        let result = parse_object_url("/tmp/123?offset=0&size=9223372036854775807");
        assert!(result.success);
        assert_eq!(result.size, 9223372036854775807);

        // Failure cases
        assert!(!parse_object_url("/tmp/123?offset=-1&size=1").success);
        assert!(!parse_object_url("/tmp/123?offset=0&size=9223372036854775808").success);
        assert!(!parse_object_url("file://path/to/file?offset=a&size=456").success);
        assert!(!parse_object_url("file://path/to/file?offset=123").success);
        assert!(!parse_object_url("file://path/to/file?offset=a&size=456&extra").success);
    }

    #[test]
    fn test_bytes_to_uint64() {
        assert_eq!(
            bytes_to_uint64(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]),
            0
        );
        assert_eq!(
            bytes_to_uint64(&[0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]),
            1
        );
        assert_eq!(
            bytes_to_uint64(&[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]),
            u64::MAX
        );
    }

    #[test]
    fn test_calculate_num_chunks() {
        assert_eq!(calculate_num_chunks(11, 1), 11);
        assert_eq!(calculate_num_chunks(1, 11), 1);
        assert_eq!(calculate_num_chunks(0, 11), 0);
        assert_eq!(calculate_num_chunks(9, 2), 5);
        assert_eq!(calculate_num_chunks(10, 2), 5);
        assert_eq!(calculate_num_chunks(11, 2), 6);
    }
}
