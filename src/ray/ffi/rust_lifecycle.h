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

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "rust/cxx.h"

// Include the generated CXX bridge header
#include "src/ray/ffi/lifecycle_bridge_gen.h"

namespace ray {

/// Lifecycle error codes.
enum class RustLifecycleErrorCode : uint8_t {
  None = 0,
  ObjectExists = 1,
  ObjectNotFound = 2,
  ObjectAlreadySealed = 3,
  OutOfMemory = 4,
  ObjectNotSealed = 7,
  InvalidRequest = 8,
  Unexpected = 10,
};

/// Result of a lifecycle operation.
class RustLifecycleResult {
 public:
  RustLifecycleResult(rust::Box<ffi::RustLifecycleResult> impl)
      : impl_(std::move(impl)) {}

  bool ok() const { return ffi::lifecycle_result_success(*impl_); }
  bool Success() const { return ok(); }

  RustLifecycleErrorCode error_code() const {
    return static_cast<RustLifecycleErrorCode>(ffi::lifecycle_result_error_code(*impl_));
  }

  std::string error_message() const {
    return std::string(ffi::lifecycle_result_error_message(*impl_));
  }
  std::string ErrorMessage() const { return error_message(); }

 private:
  rust::Box<ffi::RustLifecycleResult> impl_;
};

/// Detailed statistics snapshot from the stats collector.
class RustStatsCollectorSnapshot {
 public:
  RustStatsCollectorSnapshot(rust::Box<ffi::RustStatsCollectorSnapshot> impl)
      : impl_(std::move(impl)) {}

  int64_t num_bytes_created_total() const {
    return ffi::stats_snapshot_num_bytes_created_total(*impl_);
  }
  int64_t num_objects_spillable() const {
    return ffi::stats_snapshot_num_objects_spillable(*impl_);
  }
  int64_t num_bytes_spillable() const {
    return ffi::stats_snapshot_num_bytes_spillable(*impl_);
  }
  int64_t num_objects_unsealed() const {
    return ffi::stats_snapshot_num_objects_unsealed(*impl_);
  }
  int64_t num_bytes_unsealed() const {
    return ffi::stats_snapshot_num_bytes_unsealed(*impl_);
  }
  int64_t num_objects_in_use() const {
    return ffi::stats_snapshot_num_objects_in_use(*impl_);
  }
  int64_t num_bytes_in_use() const {
    return ffi::stats_snapshot_num_bytes_in_use(*impl_);
  }
  int64_t num_objects_evictable() const {
    return ffi::stats_snapshot_num_objects_evictable(*impl_);
  }
  int64_t num_bytes_evictable() const {
    return ffi::stats_snapshot_num_bytes_evictable(*impl_);
  }
  int64_t num_objects_created_by_worker() const {
    return ffi::stats_snapshot_num_objects_created_by_worker(*impl_);
  }
  int64_t num_bytes_created_by_worker() const {
    return ffi::stats_snapshot_num_bytes_created_by_worker(*impl_);
  }
  int64_t num_objects_restored() const {
    return ffi::stats_snapshot_num_objects_restored(*impl_);
  }
  int64_t num_bytes_restored() const {
    return ffi::stats_snapshot_num_bytes_restored(*impl_);
  }
  int64_t num_objects_received() const {
    return ffi::stats_snapshot_num_objects_received(*impl_);
  }
  int64_t num_bytes_received() const {
    return ffi::stats_snapshot_num_bytes_received(*impl_);
  }
  int64_t num_objects_errored() const {
    return ffi::stats_snapshot_num_objects_errored(*impl_);
  }
  int64_t num_bytes_errored() const {
    return ffi::stats_snapshot_num_bytes_errored(*impl_);
  }

  // Additional accessors with CamelCase names for test compatibility
  int64_t NumBytesCreatedTotal() const { return num_bytes_created_total(); }
  int64_t NumObjectsSpillable() const { return num_objects_spillable(); }
  int64_t NumBytesSpillable() const { return num_bytes_spillable(); }
  int64_t NumObjectsUnsealed() const { return num_objects_unsealed(); }
  int64_t NumBytesUnsealed() const { return num_bytes_unsealed(); }
  int64_t NumObjectsInUse() const { return num_objects_in_use(); }
  int64_t NumBytesInUse() const { return num_bytes_in_use(); }
  int64_t NumObjectsEvictable() const { return num_objects_evictable(); }
  int64_t NumBytesEvictable() const { return num_bytes_evictable(); }
  int64_t NumObjectsCreatedByWorker() const { return num_objects_created_by_worker(); }
  int64_t NumBytesCreatedByWorker() const { return num_bytes_created_by_worker(); }
  int64_t NumObjectsRestored() const { return num_objects_restored(); }
  int64_t NumBytesRestored() const { return num_bytes_restored(); }
  int64_t NumObjectsReceived() const { return num_objects_received(); }
  int64_t NumBytesReceived() const { return num_bytes_received(); }
  int64_t NumObjectsErrored() const { return num_objects_errored(); }
  int64_t NumBytesErrored() const { return num_bytes_errored(); }

  // "Current" bytes = in_use + spillable + evictable
  int64_t GetNumBytesCreatedCurrent() const {
    return ffi::stats_snapshot_get_num_bytes_created_current(*impl_);
  }

  // bytes_by_loc_seal counters
  int64_t BytesFallbackSealed() const {
    return ffi::stats_snapshot_bytes_fallback_sealed(*impl_);
  }
  int64_t BytesFallbackUnsealed() const {
    return ffi::stats_snapshot_bytes_fallback_unsealed(*impl_);
  }
  int64_t BytesPrimarySealed() const {
    return ffi::stats_snapshot_bytes_primary_sealed(*impl_);
  }
  int64_t BytesPrimaryUnsealed() const {
    return ffi::stats_snapshot_bytes_primary_unsealed(*impl_);
  }

 private:
  rust::Box<ffi::RustStatsCollectorSnapshot> impl_;
};

/// Object source enum for lifecycle manager.
enum class RustLifecycleObjectSource : uint8_t {
  CreatedByWorker = 0,
  RestoredFromStorage = 1,
  ReceivedFromRemoteRaylet = 2,
  ErrorStoredByRaylet = 3,
  CreatedByPlasmaFallbackAllocation = 4,
};

/// Object lifecycle manager backed by Rust implementation.
class RustObjectLifecycleManager {
 public:
  /// Create a new lifecycle manager with the given memory capacity.
  explicit RustObjectLifecycleManager(size_t capacity)
      : impl_(ffi::lifecycle_manager_new(capacity)) {}

  /// Create an object.
  RustLifecycleResult CreateObject(const std::string &object_id_binary,
                                   size_t data_size,
                                   size_t metadata_size,
                                   RustLifecycleObjectSource source,
                                   bool fallback = false) {
    rust::Slice<const uint8_t> id_slice(
        reinterpret_cast<const uint8_t *>(object_id_binary.data()),
        object_id_binary.size());
    return RustLifecycleResult(ffi::lifecycle_manager_create_object(
        *impl_, id_slice, data_size, metadata_size, static_cast<uint8_t>(source),
        fallback));
  }

  /// Seal an object (make it immutable).
  RustLifecycleResult SealObject(const std::string &object_id_binary) {
    rust::Slice<const uint8_t> id_slice(
        reinterpret_cast<const uint8_t *>(object_id_binary.data()),
        object_id_binary.size());
    return RustLifecycleResult(ffi::lifecycle_manager_seal_object(*impl_, id_slice));
  }

  /// Abort object creation (delete unsealed object).
  RustLifecycleResult AbortObject(const std::string &object_id_binary) {
    rust::Slice<const uint8_t> id_slice(
        reinterpret_cast<const uint8_t *>(object_id_binary.data()),
        object_id_binary.size());
    return RustLifecycleResult(ffi::lifecycle_manager_abort_object(*impl_, id_slice));
  }

  /// Delete an object.
  RustLifecycleResult DeleteObject(const std::string &object_id_binary) {
    rust::Slice<const uint8_t> id_slice(
        reinterpret_cast<const uint8_t *>(object_id_binary.data()),
        object_id_binary.size());
    return RustLifecycleResult(ffi::lifecycle_manager_delete_object(*impl_, id_slice));
  }

  /// Add a reference to an object.
  bool AddReference(const std::string &object_id_binary) {
    rust::Slice<const uint8_t> id_slice(
        reinterpret_cast<const uint8_t *>(object_id_binary.data()),
        object_id_binary.size());
    return ffi::lifecycle_manager_add_reference(*impl_, id_slice);
  }

  /// Remove a reference from an object.
  bool RemoveReference(const std::string &object_id_binary) {
    rust::Slice<const uint8_t> id_slice(
        reinterpret_cast<const uint8_t *>(object_id_binary.data()),
        object_id_binary.size());
    return ffi::lifecycle_manager_remove_reference(*impl_, id_slice);
  }

  /// Check if an object exists.
  bool Contains(const std::string &object_id_binary) const {
    rust::Slice<const uint8_t> id_slice(
        reinterpret_cast<const uint8_t *>(object_id_binary.data()),
        object_id_binary.size());
    return ffi::lifecycle_manager_contains(*impl_, id_slice);
  }

  /// Get the number of objects.
  size_t Size() const { return ffi::lifecycle_manager_len(*impl_); }

  /// Check if empty.
  bool IsEmpty() const { return ffi::lifecycle_manager_is_empty(*impl_); }

  /// Evict an object from the cache.
  bool EvictObject(const std::string &object_id_binary) {
    rust::Slice<const uint8_t> id_slice(
        reinterpret_cast<const uint8_t *>(object_id_binary.data()),
        object_id_binary.size());
    return ffi::lifecycle_manager_evict_object(*impl_, id_slice);
  }

  /// Get statistics snapshot.
  RustStatsCollectorSnapshot GetStats() const {
    return RustStatsCollectorSnapshot(ffi::lifecycle_manager_get_stats(*impl_));
  }

  // Overloads that accept vector<uint8_t> for convenience
  RustLifecycleResult CreateObject(const std::vector<uint8_t> &object_id_bytes,
                                   size_t data_size,
                                   size_t metadata_size,
                                   uint8_t source,
                                   bool fallback = false) {
    rust::Slice<const uint8_t> id_slice(object_id_bytes.data(), object_id_bytes.size());
    return RustLifecycleResult(ffi::lifecycle_manager_create_object(
        *impl_, id_slice, data_size, metadata_size, source, fallback));
  }

  RustLifecycleResult SealObject(const std::vector<uint8_t> &object_id_bytes) {
    rust::Slice<const uint8_t> id_slice(object_id_bytes.data(), object_id_bytes.size());
    return RustLifecycleResult(ffi::lifecycle_manager_seal_object(*impl_, id_slice));
  }

  RustLifecycleResult AbortObject(const std::vector<uint8_t> &object_id_bytes) {
    rust::Slice<const uint8_t> id_slice(object_id_bytes.data(), object_id_bytes.size());
    return RustLifecycleResult(ffi::lifecycle_manager_abort_object(*impl_, id_slice));
  }

  RustLifecycleResult DeleteObject(const std::vector<uint8_t> &object_id_bytes) {
    rust::Slice<const uint8_t> id_slice(object_id_bytes.data(), object_id_bytes.size());
    return RustLifecycleResult(ffi::lifecycle_manager_delete_object(*impl_, id_slice));
  }

  bool AddReference(const std::vector<uint8_t> &object_id_bytes) {
    rust::Slice<const uint8_t> id_slice(object_id_bytes.data(), object_id_bytes.size());
    return ffi::lifecycle_manager_add_reference(*impl_, id_slice);
  }

  bool RemoveReference(const std::vector<uint8_t> &object_id_bytes) {
    rust::Slice<const uint8_t> id_slice(object_id_bytes.data(), object_id_bytes.size());
    return ffi::lifecycle_manager_remove_reference(*impl_, id_slice);
  }

  bool Contains(const std::vector<uint8_t> &object_id_bytes) const {
    rust::Slice<const uint8_t> id_slice(object_id_bytes.data(), object_id_bytes.size());
    return ffi::lifecycle_manager_contains(*impl_, id_slice);
  }

  bool EvictObject(const std::vector<uint8_t> &object_id_bytes) {
    rust::Slice<const uint8_t> id_slice(object_id_bytes.data(), object_id_bytes.size());
    return ffi::lifecycle_manager_evict_object(*impl_, id_slice);
  }

 private:
  rust::Box<ffi::RustObjectLifecycleManager> impl_;
};

}  // namespace ray
