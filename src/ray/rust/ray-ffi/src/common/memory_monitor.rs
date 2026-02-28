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

//! Memory monitor utilities FFI bridge.
//!
//! This implements memory monitoring utilities matching Ray's memory_monitor_utils.h

use std::collections::HashMap;
use std::fs;
use std::path::Path;

/// Null value for memory operations.
pub const K_NULL: i64 = -2;

/// System memory snapshot.
#[derive(Debug, Clone)]
pub struct SystemMemory {
    pub total_bytes: i64,
    pub used_bytes: i64,
}

impl Default for SystemMemory {
    fn default() -> Self {
        Self {
            total_bytes: 0,
            used_bytes: 0,
        }
    }
}

/// Calculate the memory threshold.
/// Takes the greater of (total * usage_fraction) and (total - min_memory_free_bytes).
pub fn get_memory_threshold(
    total_memory_bytes: i64,
    usage_fraction: f64,
    min_memory_free_bytes: i64,
) -> i64 {
    let fraction_threshold = (total_memory_bytes as f64 * usage_fraction) as i64;

    if min_memory_free_bytes == K_NULL {
        return fraction_threshold;
    }

    let min_free_threshold = total_memory_bytes - min_memory_free_bytes;
    fraction_threshold.max(min_free_threshold)
}

/// Truncate a string to max_len characters, adding "..." if truncated.
pub fn truncate_string(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len])
    }
}

/// Parse a cgroup stat file to get value for a given key.
fn parse_cgroup_stat(content: &str, key: &str) -> Option<i64> {
    for line in content.lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() >= 2 && parts[0] == key {
            return parts[1].parse().ok();
        }
    }
    None
}

/// Get cgroup memory used bytes from stat and current files.
pub fn get_cgroup_memory_used_bytes(
    stat_file_path: &str,
    current_file_path: &str,
    inactive_file_key: &str,
    active_file_key: &str,
) -> i64 {
    // Read stat file
    let stat_content = match fs::read_to_string(stat_file_path) {
        Ok(c) => c,
        Err(_) => return K_NULL,
    };

    // Read current file
    let current_content = match fs::read_to_string(current_file_path) {
        Ok(c) => c,
        Err(_) => return K_NULL,
    };

    // Parse current memory
    let current_bytes: i64 = match current_content.trim().parse() {
        Ok(v) => v,
        Err(_) => return K_NULL,
    };

    // Parse inactive file
    let inactive_file = match parse_cgroup_stat(&stat_content, inactive_file_key) {
        Some(v) => v,
        None => return K_NULL,
    };

    // Parse active file
    let active_file = match parse_cgroup_stat(&stat_content, active_file_key) {
        Some(v) => v,
        None => return K_NULL,
    };

    current_bytes - inactive_file - active_file
}

/// Get PIDs from a proc-like directory.
pub fn get_pids_from_dir(dir_path: &str) -> Vec<i32> {
    let path = Path::new(dir_path);
    if !path.is_dir() {
        return Vec::new();
    }

    let mut pids = Vec::new();
    if let Ok(entries) = fs::read_dir(path) {
        for entry in entries.flatten() {
            if let Ok(file_name) = entry.file_name().into_string() {
                if let Ok(pid) = file_name.parse::<i32>() {
                    pids.push(pid);
                }
            }
        }
    }
    pids
}

/// Get command line for a PID.
pub fn get_command_line_for_pid(pid: i32, proc_dir: &str) -> String {
    let cmdline_path = format!("{}/{}/cmdline", proc_dir, pid);
    match fs::read_to_string(&cmdline_path) {
        Ok(content) => content.trim().to_string(),
        Err(_) => String::new(),
    }
}

/// Get top N memory usage from a map of pid -> bytes.
pub fn get_top_n_memory_usage(n: usize, usage: &HashMap<i32, i64>) -> Vec<(i32, i64)> {
    let mut list: Vec<(i32, i64)> = usage.iter().map(|(&k, &v)| (k, v)).collect();
    list.sort_by(|a, b| b.1.cmp(&a.1));  // Sort descending by memory
    list.truncate(n);
    list
}

#[cxx::bridge(namespace = "ray::ffi")]
mod ffi {
    extern "Rust" {
        fn rust_get_memory_threshold(
            total_memory_bytes: i64,
            usage_fraction: f64,
            min_memory_free_bytes: i64,
        ) -> i64;
        fn rust_truncate_string(s: &str, max_len: usize) -> String;
        fn rust_get_cgroup_memory_used_bytes(
            stat_file_path: &str,
            current_file_path: &str,
            inactive_file_key: &str,
            active_file_key: &str,
        ) -> i64;
        fn rust_get_pids_from_dir(dir_path: &str) -> Vec<i32>;
        fn rust_get_command_line_for_pid(pid: i32, proc_dir: &str) -> String;
        fn rust_memory_monitor_k_null() -> i64;
    }
}

fn rust_get_memory_threshold(
    total_memory_bytes: i64,
    usage_fraction: f64,
    min_memory_free_bytes: i64,
) -> i64 {
    get_memory_threshold(total_memory_bytes, usage_fraction, min_memory_free_bytes)
}

fn rust_truncate_string(s: &str, max_len: usize) -> String {
    truncate_string(s, max_len)
}

fn rust_get_cgroup_memory_used_bytes(
    stat_file_path: &str,
    current_file_path: &str,
    inactive_file_key: &str,
    active_file_key: &str,
) -> i64 {
    get_cgroup_memory_used_bytes(stat_file_path, current_file_path, inactive_file_key, active_file_key)
}

fn rust_get_pids_from_dir(dir_path: &str) -> Vec<i32> {
    get_pids_from_dir(dir_path)
}

fn rust_get_command_line_for_pid(pid: i32, proc_dir: &str) -> String {
    get_command_line_for_pid(pid, proc_dir)
}

fn rust_memory_monitor_k_null() -> i64 {
    K_NULL
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_memory_threshold() {
        assert_eq!(get_memory_threshold(100, 0.5, 0), 100);
        assert_eq!(get_memory_threshold(100, 0.5, 60), 50);
        assert_eq!(get_memory_threshold(100, 1.0, 10), 100);
        assert_eq!(get_memory_threshold(100, 0.1, 100), 10);
        assert_eq!(get_memory_threshold(100, 0.0, 10), 90);
        assert_eq!(get_memory_threshold(100, 0.0, K_NULL), 0);
        assert_eq!(get_memory_threshold(100, 0.5, K_NULL), 50);
    }

    #[test]
    fn test_truncate_string() {
        assert_eq!(truncate_string("im short", 20), "im short");
        assert_eq!(truncate_string("kkkkkkk", 5), "kkkkk...");
    }
}
