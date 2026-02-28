// Copyright 2024 The Ray Authors.
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

#pragma once

#include <string>
#include <vector>
#include <cstdint>

#include "rust/cxx.h"

// Include the generated CXX bridge header
#include "src/ray/ffi/memory_monitor_bridge_gen.h"

namespace ray {

/// Null value for memory operations.
inline int64_t RustMemoryMonitorKNull() {
  return ffi::rust_memory_monitor_k_null();
}

/// Memory monitor utilities backed by Rust implementation.
class RustMemoryMonitorUtils {
 public:
  /// Calculate the memory threshold.
  static int64_t GetMemoryThreshold(int64_t total_memory_bytes,
                                    double usage_fraction,
                                    int64_t min_memory_free_bytes) {
    return ffi::rust_get_memory_threshold(total_memory_bytes, usage_fraction,
                                          min_memory_free_bytes);
  }

  /// Truncate a string to max_len characters.
  static std::string TruncateString(const std::string& s, size_t max_len) {
    return static_cast<std::string>(ffi::rust_truncate_string(s, max_len));
  }

  /// Get cgroup memory used bytes.
  static int64_t GetCGroupMemoryUsedBytes(const std::string& stat_file_path,
                                          const std::string& current_file_path,
                                          const std::string& inactive_file_key,
                                          const std::string& active_file_key) {
    return ffi::rust_get_cgroup_memory_used_bytes(stat_file_path, current_file_path,
                                                  inactive_file_key, active_file_key);
  }

  /// Get PIDs from a proc directory.
  static std::vector<int32_t> GetPidsFromDir(const std::string& dir_path) {
    auto rust_pids = ffi::rust_get_pids_from_dir(dir_path);
    std::vector<int32_t> result;
    result.reserve(rust_pids.size());
    for (const auto& pid : rust_pids) {
      result.push_back(pid);
    }
    return result;
  }

  /// Get command line for a PID.
  static std::string GetCommandLineForPid(int32_t pid, const std::string& proc_dir) {
    return static_cast<std::string>(ffi::rust_get_command_line_for_pid(pid, proc_dir));
  }

  static constexpr const char* kCommandlinePath = "cmdline";
};

}  // namespace ray
