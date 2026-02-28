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

//! FFI bindings for cgroup utilities.
//!
//! Implements cgroup validation and parsing utilities.

use std::collections::HashSet;
use std::fs;
use std::path::Path;

/// Status codes for cgroup operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CgroupStatus {
    Ok = 0,
    Invalid = 1,
    NotFound = 2,
    PermissionDenied = 3,
    InvalidArgument = 4,
}

#[cxx::bridge(namespace = "ray::ffi")]
mod ffi {
    /// Result of a cgroup operation.
    pub struct CgroupResult {
        pub status: u8,
        pub message: String,
    }

    /// Result of getting available controllers.
    pub struct CgroupControllersResult {
        pub status: u8,
        pub controllers: Vec<String>,
    }

    extern "Rust" {
        /// Check if cgroupv2 is enabled by parsing the mount file.
        fn cgroup_check_v2_enabled(mount_file_path: &str, fallback_path: &str)
            -> CgroupResult;

        /// Check if a path is a valid cgroup directory.
        fn cgroup_check_path(path: &str) -> CgroupResult;

        /// Parse available controllers from a cgroup.controllers file.
        fn cgroup_get_available_controllers(path: &str) -> CgroupControllersResult;

        /// Check if a path looks like a cgroupv2 path.
        fn cgroup_is_v2_path(path: &str) -> bool;

        /// Parse mount file content to check for cgroupv2.
        fn cgroup_parse_mount_content(content: &str) -> CgroupResult;
    }
}

/// Check if cgroupv2 is enabled by parsing the mount file.
fn cgroup_check_v2_enabled(mount_file_path: &str, fallback_path: &str) -> ffi::CgroupResult {
    // Try primary mount file first
    let content = if let Ok(c) = fs::read_to_string(mount_file_path) {
        c
    } else if !fallback_path.is_empty() {
        // Try fallback
        match fs::read_to_string(fallback_path) {
            Ok(c) => c,
            Err(_) => {
                return ffi::CgroupResult {
                    status: CgroupStatus::Invalid as u8,
                    message: "Mount file not found".to_string(),
                };
            }
        }
    } else {
        return ffi::CgroupResult {
            status: CgroupStatus::Invalid as u8,
            message: "Mount file not found".to_string(),
        };
    };

    cgroup_parse_mount_content(&content)
}

/// Parse mount file content to check for cgroupv2.
fn cgroup_parse_mount_content(content: &str) -> ffi::CgroupResult {
    if content.is_empty() {
        return ffi::CgroupResult {
            status: CgroupStatus::Invalid as u8,
            message: "Empty mount file".to_string(),
        };
    }

    let mut has_cgroup_v1 = false;
    let mut has_cgroup_v2 = false;

    for line in content.lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();

        // Mount file format: fs_spec fs_file fs_vfstype fs_mntops fs_freq fs_passno
        // We need at least 3 parts for a valid mount entry
        if parts.len() < 3 {
            // Check if line is malformed (not empty but has insufficient parts)
            if !line.trim().is_empty() {
                return ffi::CgroupResult {
                    status: CgroupStatus::Invalid as u8,
                    message: "Malformed mount file".to_string(),
                };
            }
            continue;
        }

        let fs_type = parts[0];
        let mount_point = parts[1];

        // Check for cgroup v1 (type "cgroup", not "cgroup2")
        if fs_type == "cgroup" && !mount_point.contains("unified") {
            has_cgroup_v1 = true;
        }

        // Check for cgroup v2 (type "cgroup2")
        if fs_type == "cgroup2" {
            has_cgroup_v2 = true;
        }
    }

    // If cgroup v1 is mounted (even alongside v2), fail
    if has_cgroup_v1 {
        return ffi::CgroupResult {
            status: CgroupStatus::Invalid as u8,
            message: "cgroupv1 is mounted".to_string(),
        };
    }

    // Must have cgroup v2 mounted
    if has_cgroup_v2 {
        ffi::CgroupResult {
            status: CgroupStatus::Ok as u8,
            message: String::new(),
        }
    } else {
        ffi::CgroupResult {
            status: CgroupStatus::Invalid as u8,
            message: "cgroupv2 is not mounted".to_string(),
        }
    }
}

/// Check if a path is a valid cgroup directory.
fn cgroup_check_path(path: &str) -> ffi::CgroupResult {
    let p = Path::new(path);

    // Check if path exists
    if !p.exists() {
        return ffi::CgroupResult {
            status: CgroupStatus::NotFound as u8,
            message: "Path not found".to_string(),
        };
    }

    // Check if it's a cgroupv2 path (must be under /sys/fs/cgroup)
    if !cgroup_is_v2_path(path) {
        return ffi::CgroupResult {
            status: CgroupStatus::InvalidArgument as u8,
            message: "Not a cgroupv2 path".to_string(),
        };
    }

    ffi::CgroupResult {
        status: CgroupStatus::Ok as u8,
        message: String::new(),
    }
}

/// Check if a path looks like a cgroupv2 path.
fn cgroup_is_v2_path(path: &str) -> bool {
    path.starts_with("/sys/fs/cgroup")
}

/// Parse available controllers from a cgroup.controllers file.
fn cgroup_get_available_controllers(path: &str) -> ffi::CgroupControllersResult {
    // Must be a cgroupv2 path
    if !cgroup_is_v2_path(path) {
        return ffi::CgroupControllersResult {
            status: CgroupStatus::InvalidArgument as u8,
            controllers: Vec::new(),
        };
    }

    let controllers_file = Path::new(path).join("cgroup.controllers");
    match fs::read_to_string(&controllers_file) {
        Ok(content) => {
            let controllers: Vec<String> = content
                .split_whitespace()
                .map(|s| s.to_string())
                .collect();
            ffi::CgroupControllersResult {
                status: CgroupStatus::Ok as u8,
                controllers,
            }
        }
        Err(_) => ffi::CgroupControllersResult {
            status: CgroupStatus::NotFound as u8,
            controllers: Vec::new(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_empty_mount() {
        let result = cgroup_parse_mount_content("");
        assert_eq!(result.status, CgroupStatus::Invalid as u8);
    }

    #[test]
    fn test_parse_v2_only() {
        let result = cgroup_parse_mount_content("cgroup2 /sys/fs/cgroup cgroup2 rw 0 0\n");
        assert_eq!(result.status, CgroupStatus::Ok as u8);
    }

    #[test]
    fn test_parse_v1_only() {
        let result = cgroup_parse_mount_content("cgroup /sys/fs/cgroup rw 0 0\n");
        assert_eq!(result.status, CgroupStatus::Invalid as u8);
    }

    #[test]
    fn test_parse_v1_and_v2() {
        let result = cgroup_parse_mount_content(
            "cgroup /sys/fs/cgroup rw 0 0\ncgroup2 /sys/fs/cgroup/unified/ rw 0 0\n",
        );
        assert_eq!(result.status, CgroupStatus::Invalid as u8);
    }

    #[test]
    fn test_is_v2_path() {
        assert!(cgroup_is_v2_path("/sys/fs/cgroup"));
        assert!(cgroup_is_v2_path("/sys/fs/cgroup/ray"));
        assert!(!cgroup_is_v2_path("/tmp"));
        assert!(!cgroup_is_v2_path("/some/other/path"));
    }
}
