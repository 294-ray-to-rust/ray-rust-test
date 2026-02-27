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
#include <optional>
#include <string>

#include "rust/cxx.h"

// Include the generated CXX bridge header
#include "src/ray/ffi/allocator_bridge_gen.h"

namespace ray {

/// Result of an allocation operation.
class RustAllocation {
 public:
  RustAllocation(rust::Box<ffi::RustAllocationResult> impl) : impl_(std::move(impl)) {}

  bool ok() const { return ffi::allocation_result_success(*impl_); }

  uint64_t address() const { return ffi::allocation_result_address(*impl_); }

  size_t size() const { return ffi::allocation_result_size(*impl_); }

  int32_t fd() const { return ffi::allocation_result_fd(*impl_); }

  bool is_fallback_allocated() const { return ffi::allocation_result_is_fallback(*impl_); }

 private:
  rust::Box<ffi::RustAllocationResult> impl_;
};

/// Plasma allocator backed by Rust implementation.
/// Uses mmap for primary allocation and file-backed mmap for fallback.
class RustPlasmaAllocator {
 public:
  /// Create a new plasma allocator.
  ///
  /// @param plasma_directory Directory for primary mmap files.
  /// @param fallback_directory Directory for fallback mmap files.
  /// @param hugepage_enabled Whether to enable huge pages.
  /// @param footprint_limit Maximum memory footprint for primary allocations.
  RustPlasmaAllocator(const std::string &plasma_directory,
                      const std::string &fallback_directory,
                      bool hugepage_enabled,
                      size_t footprint_limit)
      : impl_(ffi::plasma_allocator_new(plasma_directory,
                                        fallback_directory,
                                        hugepage_enabled,
                                        footprint_limit)) {}

  /// Get the footprint limit.
  size_t GetFootprintLimit() const { return ffi::plasma_allocator_get_footprint_limit(*impl_); }

  /// Allocate from primary memory pool.
  /// @param size Number of bytes to allocate.
  /// @return Allocation result, check ok() before using.
  std::optional<RustAllocation> Allocate(size_t size) {
    auto result = ffi::plasma_allocator_allocate(*impl_, size);
    if (ffi::allocation_result_success(*result)) {
      return RustAllocation(std::move(result));
    }
    return std::nullopt;
  }

  /// Allocate from fallback storage (disk-backed).
  /// @param size Number of bytes to allocate.
  /// @return Allocation result, check ok() before using.
  std::optional<RustAllocation> FallbackAllocate(size_t size) {
    auto result = ffi::plasma_allocator_fallback_allocate(*impl_, size);
    if (ffi::allocation_result_success(*result)) {
      return RustAllocation(std::move(result));
    }
    return std::nullopt;
  }

  /// Free an allocation.
  /// @param alloc The allocation to free.
  void Free(RustAllocation alloc) {
    ffi::plasma_allocator_free(
        *impl_, alloc.address(), alloc.size(), alloc.is_fallback_allocated());
  }

  /// Get total bytes allocated (primary + fallback).
  size_t Allocated() const { return ffi::plasma_allocator_allocated(*impl_); }

  /// Get total fallback bytes allocated.
  size_t FallbackAllocated() const { return ffi::plasma_allocator_fallback_allocated(*impl_); }

 private:
  rust::Box<ffi::RustPlasmaAllocator> impl_;
};

}  // namespace ray
