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

#include <memory>
#include <string>
#include <cstdint>

#include "rust/cxx.h"

// Include the generated CXX bridge header
#include "src/ray/ffi/util_bridge_gen.h"

namespace ray {

/// ExponentialBackoff backed by Rust implementation.
class RustExponentialBackoff {
 public:
  /// Construct an exponential backoff counter.
  ///
  /// @param initial_value The start value for this counter
  /// @param multiplier The multiplier for this counter
  /// @param max_value The maximum value for this counter
  RustExponentialBackoff(uint64_t initial_value,
                         double multiplier,
                         uint64_t max_value = std::numeric_limits<uint64_t>::max())
      : impl_(ffi::exponential_backoff_new(initial_value, multiplier, max_value)) {}

  /// Computes the backoff delay using the exponential backoff algorithm.
  /// Formula: min(base * 2^attempt, max_backoff)
  ///
  /// @param attempt The attempt number (0-indexed)
  /// @param base_ms The base delay in milliseconds
  /// @param max_backoff_ms The maximum backoff value
  /// @return The delay in ms
  static uint64_t GetBackoffMs(uint64_t attempt,
                               uint64_t base_ms,
                               uint64_t max_backoff_ms = 60 * 1000) {
    return ffi::exponential_backoff_get_backoff_ms(attempt, base_ms, max_backoff_ms);
  }

  /// Get the next backoff value and advance the counter.
  uint64_t Next() { return ffi::exponential_backoff_next(*impl_); }

  /// Get the current backoff value without advancing.
  uint64_t Current() const { return ffi::exponential_backoff_current(*impl_); }

  /// Reset to the initial value.
  void Reset() { ffi::exponential_backoff_reset(*impl_); }

 private:
  rust::Box<ffi::RustExponentialBackoff> impl_;
};

/// StatusOr-like result for string parsing, backed by Rust.
template <typename T>
class RustParseResult {
 public:
  explicit RustParseResult(rust::Box<ffi::RustParseResult> impl)
      : impl_(std::move(impl)) {}

  bool ok() const { return ffi::parse_result_is_ok(*impl_); }

  T value() const { return static_cast<T>(ffi::parse_result_value(*impl_)); }

  bool IsInvalidArgument() const {
    return ffi::parse_result_code(*impl_) == ffi::StatusCode::InvalidArgument;
  }

  std::string ToString() const {
    if (ok()) {
      return "OK";
    }
    return "InvalidArgument";
  }

 private:
  rust::Box<ffi::RustParseResult> impl_;
};

/// Parse a string to an integer type.
template <typename T>
RustParseResult<T> StringToInt(const std::string &input);

// Specializations
template <>
inline RustParseResult<int64_t> StringToInt<int64_t>(const std::string &input) {
  return RustParseResult<int64_t>(ffi::string_to_i64(input));
}

template <>
inline RustParseResult<int32_t> StringToInt<int32_t>(const std::string &input) {
  return RustParseResult<int32_t>(ffi::string_to_i32(input));
}

// Note: int and int32_t are the same type on most platforms, so we don't need
// a separate specialization for int.

template <>
inline RustParseResult<int8_t> StringToInt<int8_t>(const std::string &input) {
  return RustParseResult<int8_t>(ffi::string_to_i8(input));
}

/// Size literal helper functions (matching C++ _MiB, _KB, _GB).
inline constexpr uint64_t MiB(uint64_t value) { return value * 1024 * 1024; }
inline constexpr uint64_t KB(uint64_t value) { return value * 1000; }
inline constexpr uint64_t GB(uint64_t value) { return value * 1000000000ULL; }

// These use the Rust implementation for verification
inline uint64_t RustMiB(uint64_t value) { return ffi::size_mib(value); }
inline uint64_t RustKB(uint64_t value) { return ffi::size_kb(value); }
inline uint64_t RustGB(uint64_t value) { return ffi::size_gb(value); }

}  // namespace ray
