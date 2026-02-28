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

#include <chrono>
#include <memory>
#include <optional>
#include <string>
#include <cstdint>
#include <functional>

#include "rust/cxx.h"

// Include the generated CXX bridge header
#include "src/ray/ffi/event_stats_bridge_gen.h"

namespace ray {

/// Event statistics.
struct RustEventStats {
  int64_t cum_count = 0;
  int64_t curr_count = 0;
  int64_t cum_execution_time = 0;
  int64_t cum_queue_time = 0;
  int64_t running_count = 0;
};

/// Opaque stats handle.
struct RustStatsHandle {
  uint64_t handle_id;
  std::weak_ptr<class RustEventTracker> tracker;

  RustStatsHandle(uint64_t id, std::weak_ptr<RustEventTracker> t)
      : handle_id(id), tracker(std::move(t)) {}
};

/// Event tracker backed by Rust implementation.
class RustEventTracker : public std::enable_shared_from_this<RustEventTracker> {
 public:
  RustEventTracker() : impl_(ffi::event_tracker_new()) {}

  /// Record the start of an event.
  std::shared_ptr<RustStatsHandle> RecordStart(const std::string& name) {
    uint64_t handle_id = ffi::event_tracker_record_start(*impl_, name);
    return std::make_shared<RustStatsHandle>(handle_id, weak_from_this());
  }

  /// Record the end of an event.
  void RecordEnd(std::shared_ptr<RustStatsHandle> handle) {
    if (handle) {
      ffi::event_tracker_record_end(*impl_, handle->handle_id);
    }
  }

  /// Record execution of a function.
  void RecordExecution(const std::function<void()>& fn,
                       std::shared_ptr<RustStatsHandle> handle) {
    if (!handle) {
      fn();
      return;
    }

    // Record queue time ending
    ffi::event_tracker_record_execution_start(*impl_, handle->handle_id);

    // Track execution start time
    auto start = std::chrono::high_resolution_clock::now();

    // Execute the function
    fn();

    // Calculate execution time
    auto end = std::chrono::high_resolution_clock::now();
    auto execution_time = std::chrono::duration_cast<std::chrono::nanoseconds>(
        end - start).count();

    // Record execution end
    ffi::event_tracker_record_execution_end(*impl_, handle->handle_id, execution_time);
  }

  /// Get statistics for an event.
  std::optional<RustEventStats> get_event_stats(const std::string& name) const {
    if (!ffi::event_tracker_has_stats(*impl_, name)) {
      return std::nullopt;
    }
    auto stats = ffi::event_tracker_get_stats(*impl_, name);
    return RustEventStats{
        stats.cum_count,
        stats.curr_count,
        stats.cum_execution_time,
        stats.cum_queue_time,
        stats.running_count
    };
  }

 private:
  rust::Box<ffi::RustEventTracker> impl_;
};

}  // namespace ray
