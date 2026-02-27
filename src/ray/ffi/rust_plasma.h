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
#include "src/ray/ffi/plasma_bridge_gen.h"

namespace ray {

/// Object state enum matching C++ implementation.
enum class RustObjectState : uint8_t {
  PlasmaCreated = 1,
  PlasmaSealed = 2,
};

/// Object source enum matching C++ implementation.
enum class RustObjectSourceEnum : uint8_t {
  CreatedByWorker = 0,
  RestoredFromStorage = 1,
  ReceivedFromRemoteRaylet = 2,
  ErrorStoredByRaylet = 3,
  CreatedByPlasmaFallbackAllocation = 4,
};

/// Plasma error codes.
enum class RustPlasmaErrorCode : uint8_t {
  None = 0,
  ObjectExists = 1,
  ObjectNotFound = 2,
  ObjectAlreadySealed = 3,
  OutOfMemory = 4,
  TransientOutOfMemory = 5,
  OutOfDisk = 6,
  ObjectNotSealed = 7,
  InvalidRequest = 8,
  IoError = 9,
  Unexpected = 10,
};

/// Result of a plasma operation.
class RustPlasmaResult {
 public:
  RustPlasmaResult(rust::Box<ffi::RustPlasmaResult> impl) : impl_(std::move(impl)) {}

  bool ok() const { return ffi::plasma_result_success(*impl_); }

  RustPlasmaErrorCode error_code() const {
    return static_cast<RustPlasmaErrorCode>(ffi::plasma_result_error_code(*impl_));
  }

  std::string error_message() const {
    return std::string(ffi::plasma_result_error_message(*impl_));
  }

 private:
  rust::Box<ffi::RustPlasmaResult> impl_;
};

/// Statistics snapshot from the object store.
class RustObjectStoreStats {
 public:
  RustObjectStoreStats(rust::Box<ffi::RustObjectStoreStats> impl)
      : impl_(std::move(impl)) {}

  size_t num_objects() const { return ffi::stats_num_objects(*impl_); }
  int64_t num_bytes_used() const { return ffi::stats_num_bytes_used(*impl_); }
  int64_t num_bytes_created_total() const {
    return ffi::stats_num_bytes_created_total(*impl_);
  }
  size_t num_objects_created_total() const {
    return ffi::stats_num_objects_created_total(*impl_);
  }
  int64_t num_bytes_sealed() const { return ffi::stats_num_bytes_sealed(*impl_); }
  size_t num_objects_sealed() const { return ffi::stats_num_objects_sealed(*impl_); }
  size_t num_fallback_allocations() const {
    return ffi::stats_num_fallback_allocations(*impl_);
  }
  int64_t num_bytes_fallback() const { return ffi::stats_num_bytes_fallback(*impl_); }

 private:
  rust::Box<ffi::RustObjectStoreStats> impl_;
};

/// Drop-in replacement for plasma ObjectStore backed by Rust implementation.
class RustObjectStore {
 public:
  /// Create a new object store with the given capacity.
  explicit RustObjectStore(size_t capacity)
      : impl_(ffi::object_store_new(capacity)) {}

  /// Create with full configuration.
  RustObjectStore(size_t capacity, bool enable_fallback, size_t min_fallback_size)
      : impl_(ffi::object_store_new_with_config(capacity, enable_fallback,
                                                 min_fallback_size)) {}

  /// Create an object in the store.
  RustPlasmaResult CreateObject(const std::string &object_id_binary,
                                 size_t data_size,
                                 size_t metadata_size,
                                 RustObjectSourceEnum source,
                                 const std::vector<uint8_t> &owner_address) {
    rust::Slice<const uint8_t> id_slice(
        reinterpret_cast<const uint8_t *>(object_id_binary.data()),
        object_id_binary.size());
    rust::Slice<const uint8_t> owner_slice(owner_address.data(), owner_address.size());
    return RustPlasmaResult(ffi::object_store_create_object(
        *impl_, id_slice, data_size, metadata_size, static_cast<uint8_t>(source),
        owner_slice));
  }

  /// Seal an object (make it immutable).
  RustPlasmaResult SealObject(const std::string &object_id_binary) {
    rust::Slice<const uint8_t> id_slice(
        reinterpret_cast<const uint8_t *>(object_id_binary.data()),
        object_id_binary.size());
    return RustPlasmaResult(ffi::object_store_seal_object(*impl_, id_slice));
  }

  /// Get an object reference (increment ref count).
  RustPlasmaResult GetObject(const std::string &object_id_binary) {
    rust::Slice<const uint8_t> id_slice(
        reinterpret_cast<const uint8_t *>(object_id_binary.data()),
        object_id_binary.size());
    return RustPlasmaResult(ffi::object_store_get_object(*impl_, id_slice));
  }

  /// Release an object reference.
  RustPlasmaResult ReleaseObject(const std::string &object_id_binary) {
    rust::Slice<const uint8_t> id_slice(
        reinterpret_cast<const uint8_t *>(object_id_binary.data()),
        object_id_binary.size());
    return RustPlasmaResult(ffi::object_store_release_object(*impl_, id_slice));
  }

  /// Delete an object from the store.
  RustPlasmaResult DeleteObject(const std::string &object_id_binary) {
    rust::Slice<const uint8_t> id_slice(
        reinterpret_cast<const uint8_t *>(object_id_binary.data()),
        object_id_binary.size());
    return RustPlasmaResult(ffi::object_store_delete_object(*impl_, id_slice));
  }

  /// Abort object creation (delete unsealed object).
  RustPlasmaResult AbortObject(const std::string &object_id_binary) {
    rust::Slice<const uint8_t> id_slice(
        reinterpret_cast<const uint8_t *>(object_id_binary.data()),
        object_id_binary.size());
    return RustPlasmaResult(ffi::object_store_abort_object(*impl_, id_slice));
  }

  /// Check if an object exists.
  bool Contains(const std::string &object_id_binary) const {
    rust::Slice<const uint8_t> id_slice(
        reinterpret_cast<const uint8_t *>(object_id_binary.data()),
        object_id_binary.size());
    return ffi::object_store_contains(*impl_, id_slice);
  }

  /// Check if an object is sealed.
  bool IsSealed(const std::string &object_id_binary) const {
    rust::Slice<const uint8_t> id_slice(
        reinterpret_cast<const uint8_t *>(object_id_binary.data()),
        object_id_binary.size());
    return ffi::object_store_is_sealed(*impl_, id_slice);
  }

  /// Get the number of objects.
  size_t Size() const { return ffi::object_store_len(*impl_); }

  /// Check if the store is empty.
  bool IsEmpty() const { return ffi::object_store_is_empty(*impl_); }

  /// Get the total capacity.
  size_t Capacity() const { return ffi::object_store_capacity(*impl_); }

  /// Get available capacity.
  size_t AvailableCapacity() const {
    return ffi::object_store_available_capacity(*impl_);
  }

  /// Evict objects to free up space.
  size_t Evict(size_t bytes_needed) {
    return ffi::object_store_evict(*impl_, bytes_needed);
  }

  /// Get statistics snapshot.
  RustObjectStoreStats GetStats() const {
    return RustObjectStoreStats(ffi::object_store_get_stats(*impl_));
  }

 private:
  rust::Box<ffi::RustObjectStore> impl_;
};

}  // namespace ray
