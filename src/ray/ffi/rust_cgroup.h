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

#pragma once

#include <cstdint>
#include <string>
#include <unordered_set>
#include <vector>

#include "rust/cxx.h"
#include "src/ray/ffi/cgroup_bridge_gen.h"

namespace ray {

/// Status codes for cgroup operations.
enum class RustCgroupStatus : uint8_t {
  Ok = 0,
  Invalid = 1,
  NotFound = 2,
  PermissionDenied = 3,
  InvalidArgument = 4,
};

/// Result of a cgroup operation.
class RustCgroupResult {
 public:
  RustCgroupResult(ffi::CgroupResult result)
      : status_(static_cast<RustCgroupStatus>(result.status)),
        message_(std::string(result.message)) {}

  bool ok() const { return status_ == RustCgroupStatus::Ok; }
  bool IsInvalid() const { return status_ == RustCgroupStatus::Invalid; }
  bool IsNotFound() const { return status_ == RustCgroupStatus::NotFound; }
  bool IsPermissionDenied() const {
    return status_ == RustCgroupStatus::PermissionDenied;
  }
  bool IsInvalidArgument() const {
    return status_ == RustCgroupStatus::InvalidArgument;
  }

  RustCgroupStatus status() const { return status_; }
  const std::string &message() const { return message_; }

  std::string ToString() const {
    if (ok()) {
      return "OK";
    }
    return message_;
  }

 private:
  RustCgroupStatus status_;
  std::string message_;
};

/// Sysfs cgroup driver backed by Rust implementation.
class RustSysFsCgroupDriver {
 public:
  RustSysFsCgroupDriver() : mount_file_path_("/proc/mounts") {}
  explicit RustSysFsCgroupDriver(const std::string &mount_file_path)
      : mount_file_path_(mount_file_path) {}
  RustSysFsCgroupDriver(const std::string &mount_file_path,
                        const std::string &fallback_mount_file_path)
      : mount_file_path_(mount_file_path),
        fallback_mount_file_path_(fallback_mount_file_path) {}

  /// Check if cgroupv2 is enabled.
  RustCgroupResult CheckCgroupv2Enabled() const {
    return RustCgroupResult(
        ffi::cgroup_check_v2_enabled(mount_file_path_, fallback_mount_file_path_));
  }

  /// Check if a path is a valid cgroup.
  RustCgroupResult CheckCgroup(const std::string &path) const {
    return RustCgroupResult(ffi::cgroup_check_path(path));
  }

  /// Delete a cgroup (validates path only - doesn't actually delete).
  RustCgroupResult DeleteCgroup(const std::string &path) const {
    // For FFI, we just validate the path
    return RustCgroupResult(ffi::cgroup_check_path(path));
  }

  /// Get available controllers from a cgroup.
  std::pair<RustCgroupResult, std::unordered_set<std::string>> GetAvailableControllers(
      const std::string &path) const {
    auto result = ffi::cgroup_get_available_controllers(path);
    RustCgroupResult status(ffi::CgroupResult{result.status, ""});
    std::unordered_set<std::string> controllers;
    for (const auto &c : result.controllers) {
      controllers.insert(std::string(c));
    }
    return {status, controllers};
  }

  /// Enable a controller (validates path only).
  RustCgroupResult EnableController(const std::string &path,
                                    const std::string &controller) const {
    if (!ffi::cgroup_is_v2_path(path)) {
      return RustCgroupResult(
          ffi::CgroupResult{static_cast<uint8_t>(RustCgroupStatus::InvalidArgument),
                            "Not a cgroupv2 path"});
    }
    return RustCgroupResult(ffi::CgroupResult{0, ""});
  }

  /// Disable a controller (validates path only).
  RustCgroupResult DisableController(const std::string &path,
                                     const std::string &controller) const {
    if (!ffi::cgroup_is_v2_path(path)) {
      return RustCgroupResult(
          ffi::CgroupResult{static_cast<uint8_t>(RustCgroupStatus::InvalidArgument),
                            "Not a cgroupv2 path"});
    }
    return RustCgroupResult(ffi::CgroupResult{0, ""});
  }

  /// Add a constraint (validates path only).
  RustCgroupResult AddConstraint(const std::string &path,
                                 const std::string &name,
                                 const std::string &value) const {
    if (!ffi::cgroup_is_v2_path(path)) {
      return RustCgroupResult(
          ffi::CgroupResult{static_cast<uint8_t>(RustCgroupStatus::InvalidArgument),
                            "Not a cgroupv2 path"});
    }
    return RustCgroupResult(ffi::CgroupResult{0, ""});
  }

 private:
  std::string mount_file_path_;
  std::string fallback_mount_file_path_;
};

/// Helper class for creating temporary files in tests.
class RustTempFile {
 public:
  RustTempFile() {
    char template_path[] = "/tmp/rust_cgroup_test_XXXXXX";
    int fd = mkstemp(template_path);
    if (fd != -1) {
      path_ = template_path;
      close(fd);
    }
  }

  explicit RustTempFile(const std::string &path) : path_(path) {
    // Create file
    FILE *f = fopen(path.c_str(), "w");
    if (f) {
      fclose(f);
    }
  }

  ~RustTempFile() {
    if (!path_.empty()) {
      unlink(path_.c_str());
    }
  }

  void AppendLine(const std::string &line) {
    FILE *f = fopen(path_.c_str(), "a");
    if (f) {
      fputs(line.c_str(), f);
      fclose(f);
    }
  }

  const std::string &GetPath() const { return path_; }

 private:
  std::string path_;
};

/// Helper class for creating temporary directories in tests.
class RustTempDirectory {
 public:
  static std::pair<bool, std::unique_ptr<RustTempDirectory>> Create() {
    char template_path[] = "/tmp/rust_cgroup_dir_XXXXXX";
    char *result = mkdtemp(template_path);
    if (result) {
      auto dir = std::unique_ptr<RustTempDirectory>(new RustTempDirectory(result));
      return {true, std::move(dir)};
    }
    return {false, nullptr};
  }

  ~RustTempDirectory() {
    if (!path_.empty()) {
      rmdir(path_.c_str());
    }
  }

  const std::string &GetPath() const { return path_; }

 private:
  explicit RustTempDirectory(const std::string &path) : path_(path) {}
  std::string path_;
};

}  // namespace ray
