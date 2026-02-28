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

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "rust/cxx.h"
#include "src/ray/ffi/cgroup_manager_bridge_gen.h"

namespace ray {

/// Status codes for cgroup manager operations.
enum class RustCgroupManagerStatus : uint8_t {
  Ok = 0,
  Invalid = 1,
  NotFound = 2,
  PermissionDenied = 3,
  InvalidArgument = 4,
};

/// Result of a cgroup manager operation.
class RustCgroupManagerResult {
 public:
  RustCgroupManagerResult(ffi::CgroupManagerResult result)
      : status_(static_cast<RustCgroupManagerStatus>(result.status)),
        message_(std::string(result.message)) {}

  RustCgroupManagerResult(RustCgroupManagerStatus status, const std::string &message = "")
      : status_(status), message_(message) {}

  bool ok() const { return status_ == RustCgroupManagerStatus::Ok; }
  bool IsInvalid() const { return status_ == RustCgroupManagerStatus::Invalid; }
  bool IsNotFound() const { return status_ == RustCgroupManagerStatus::NotFound; }
  bool IsPermissionDenied() const {
    return status_ == RustCgroupManagerStatus::PermissionDenied;
  }
  bool IsInvalidArgument() const {
    return status_ == RustCgroupManagerStatus::InvalidArgument;
  }

  RustCgroupManagerStatus status() const { return status_; }
  const std::string &message() const { return message_; }

  std::string ToString() const {
    if (ok()) {
      return "OK";
    }
    return message_;
  }

 private:
  RustCgroupManagerStatus status_;
  std::string message_;
};

/// Fake cgroup driver state backed by Rust implementation.
class RustFakeCgroupDriverState {
 public:
  RustFakeCgroupDriverState() : handle_(ffi::cgroup_manager_create_driver_state()) {}

  ~RustFakeCgroupDriverState() {
    if (handle_ != 0) {
      ffi::cgroup_manager_free_driver_state(handle_);
    }
  }

  // Non-copyable
  RustFakeCgroupDriverState(const RustFakeCgroupDriverState &) = delete;
  RustFakeCgroupDriverState &operator=(const RustFakeCgroupDriverState &) = delete;

  // Movable
  RustFakeCgroupDriverState(RustFakeCgroupDriverState &&other) noexcept
      : handle_(other.handle_) {
    other.handle_ = 0;
  }

  RustFakeCgroupDriverState &operator=(RustFakeCgroupDriverState &&other) noexcept {
    if (this != &other) {
      if (handle_ != 0) {
        ffi::cgroup_manager_free_driver_state(handle_);
      }
      handle_ = other.handle_;
      other.handle_ = 0;
    }
    return *this;
  }

  uint64_t handle() const { return handle_; }

  void AddCgroup(const std::string &path,
                 const std::vector<int32_t> &processes = {},
                 const std::vector<std::string> &available_controllers = {}) {
    bool has_cpu = std::find(available_controllers.begin(),
                             available_controllers.end(),
                             "cpu") != available_controllers.end();
    bool has_memory = std::find(available_controllers.begin(),
                                available_controllers.end(),
                                "memory") != available_controllers.end();

    if (!processes.empty()) {
      ffi::cgroup_manager_add_cgroup_with_process(
          handle_, path, processes[0], has_cpu, has_memory);
    } else {
      ffi::cgroup_manager_add_cgroup_simple(handle_, path, has_cpu, has_memory);
    }
  }

  size_t GetCgroupCount() const { return ffi::cgroup_manager_get_cgroup_count(handle_); }

  bool HasCgroup(const std::string &path) const {
    return ffi::cgroup_manager_has_cgroup(handle_, path);
  }

  void SetCheckEnabledStatus(RustCgroupManagerStatus status) {
    ffi::cgroup_manager_set_check_enabled_status(handle_, static_cast<uint8_t>(status));
  }

  void SetCheckCgroupStatus(RustCgroupManagerStatus status) {
    ffi::cgroup_manager_set_check_cgroup_status(handle_, static_cast<uint8_t>(status));
  }

  void SetAddProcessStatus(RustCgroupManagerStatus status) {
    ffi::cgroup_manager_set_add_process_status(handle_, static_cast<uint8_t>(status));
  }

  void SetCleanupMode(bool enabled) {
    ffi::cgroup_manager_set_cleanup_mode(handle_, enabled);
  }

  size_t GetEnabledControllersCount(const std::string &path) const {
    return ffi::cgroup_manager_get_enabled_controllers_count(handle_, path);
  }

  bool HasEnabledController(const std::string &path, const std::string &controller) const {
    return ffi::cgroup_manager_has_enabled_controller(handle_, path, controller);
  }

  size_t GetProcessesCount(const std::string &path) const {
    return ffi::cgroup_manager_get_processes_count(handle_, path);
  }

  std::string GetConstraint(const std::string &path, const std::string &name) const {
    return std::string(ffi::cgroup_manager_get_constraint(handle_, path, name));
  }

  size_t GetConstraintsDisabledCount() const {
    return ffi::cgroup_manager_get_constraints_disabled_count(handle_);
  }

  size_t GetControllersDisabledCount() const {
    return ffi::cgroup_manager_get_controllers_disabled_count(handle_);
  }

  size_t GetProcessesMovedCount() const {
    return ffi::cgroup_manager_get_processes_moved_count(handle_);
  }

  size_t GetDeletedCgroupsCount() const {
    return ffi::cgroup_manager_get_deleted_cgroups_count(handle_);
  }

  std::string GetDeletedCgroup(size_t index) const {
    return std::string(ffi::cgroup_manager_get_deleted_cgroup(handle_, index));
  }

  bool WasConstraintDisabled(const std::string &cgroup, const std::string &name) const {
    return ffi::cgroup_manager_was_constraint_disabled(handle_, cgroup, name);
  }

  std::string GetControllerDisabledCgroup(size_t index) const {
    return std::string(ffi::cgroup_manager_get_controller_disabled_cgroup(handle_, index));
  }

  int32_t GetLastConstraintOrder() const {
    return ffi::cgroup_manager_get_last_constraint_order(handle_);
  }

  int32_t GetFirstControllerOrder() const {
    return ffi::cgroup_manager_get_first_controller_order(handle_);
  }

  int32_t GetLastProcessMovedOrder() const {
    return ffi::cgroup_manager_get_last_process_moved_order(handle_);
  }

  int32_t GetFirstDeletedOrder() const {
    return ffi::cgroup_manager_get_first_deleted_order(handle_);
  }

  std::string GetProcessMovedTo(size_t index) const {
    return std::string(ffi::cgroup_manager_get_process_moved_to(handle_, index));
  }

  size_t CountProcessesMovedFrom(const std::string &from, const std::string &to) const {
    return ffi::cgroup_manager_count_processes_moved_from(handle_, from, to);
  }

 private:
  uint64_t handle_;
};

/// Cgroup manager backed by Rust implementation.
class RustCgroupManager {
 public:
  /// Create a cgroup manager.
  static std::pair<RustCgroupManagerResult, std::unique_ptr<RustCgroupManager>> Create(
      const std::string &base_cgroup,
      const std::string &node_id,
      int64_t system_reserved_cpu_weight,
      int64_t system_memory_bytes_min,
      int64_t system_memory_bytes_low,
      int64_t user_memory_high_bytes,
      int64_t user_memory_max_bytes,
      RustFakeCgroupDriverState &driver_state) {
    // First try to create to check if it would succeed
    auto result = ffi::cgroup_manager_create(driver_state.handle(),
                                             base_cgroup,
                                             node_id,
                                             system_reserved_cpu_weight,
                                             system_memory_bytes_min,
                                             system_memory_bytes_low,
                                             user_memory_high_bytes,
                                             user_memory_max_bytes);

    if (result.status != 0) {
      return {RustCgroupManagerResult(result), nullptr};
    }

    // Create and store the actual manager
    uint64_t handle = ffi::cgroup_manager_store(driver_state.handle(),
                                                base_cgroup,
                                                node_id,
                                                system_reserved_cpu_weight,
                                                system_memory_bytes_min,
                                                system_memory_bytes_low,
                                                user_memory_high_bytes,
                                                user_memory_max_bytes);

    if (handle == 0) {
      return {RustCgroupManagerResult(RustCgroupManagerStatus::Invalid,
                                      "Failed to store manager"),
              nullptr};
    }

    auto manager = std::unique_ptr<RustCgroupManager>(new RustCgroupManager(handle));
    return {RustCgroupManagerResult(RustCgroupManagerStatus::Ok), std::move(manager)};
  }

  ~RustCgroupManager() {
    if (handle_ != 0) {
      ffi::cgroup_manager_free(handle_);
    }
  }

  // Non-copyable
  RustCgroupManager(const RustCgroupManager &) = delete;
  RustCgroupManager &operator=(const RustCgroupManager &) = delete;

  // Movable
  RustCgroupManager(RustCgroupManager &&other) noexcept : handle_(other.handle_) {
    other.handle_ = 0;
  }

  RustCgroupManager &operator=(RustCgroupManager &&other) noexcept {
    if (this != &other) {
      if (handle_ != 0) {
        ffi::cgroup_manager_free(handle_);
      }
      handle_ = other.handle_;
      other.handle_ = 0;
    }
    return *this;
  }

  RustCgroupManagerResult AddProcessToSystemCgroup(const std::string &pid) {
    return RustCgroupManagerResult(
        ffi::cgroup_manager_add_process_to_system(handle_, pid));
  }

 private:
  explicit RustCgroupManager(uint64_t handle) : handle_(handle) {}

  uint64_t handle_;
};

}  // namespace ray
