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
#include <functional>

#include "rust/cxx.h"

// Include the generated CXX bridge header
#include "src/ray/ffi/status_or_bridge_gen.h"

namespace ray {

/// StatusOr<int> backed by Rust implementation.
///
/// This class provides a C++ interface that mirrors ray::StatusOr<int>,
/// but uses the Rust implementation under the hood.
class RustStatusOrInt {
 public:
  /// Create a success result with value.
  explicit RustStatusOrInt(int value) : impl_(ffi::result_i32_ok(value)) {}

  /// Create an error result.
  RustStatusOrInt(ffi::StatusCode code, const std::string &msg)
      : impl_(ffi::result_i32_error(code, msg)) {}

  /// Create from InvalidArgument error (convenience).
  static RustStatusOrInt InvalidArgument(const std::string &msg) {
    return RustStatusOrInt(ffi::StatusCode::InvalidArgument, msg);
  }

  /// Create from Invalid error (convenience).
  static RustStatusOrInt Invalid(const std::string &msg) {
    return RustStatusOrInt(ffi::StatusCode::Invalid, msg);
  }

  /// Copy constructor.
  RustStatusOrInt(const RustStatusOrInt &other)
      : impl_(ffi::result_i32_clone(*other.impl_)) {}

  /// Copy assignment.
  RustStatusOrInt &operator=(const RustStatusOrInt &other) {
    if (this != &other) {
      impl_ = ffi::result_i32_clone(*other.impl_);
    }
    return *this;
  }

  /// Move constructor.
  RustStatusOrInt(RustStatusOrInt &&other) noexcept = default;

  /// Move assignment.
  RustStatusOrInt &operator=(RustStatusOrInt &&other) noexcept = default;

  /// Returns whether the result is OK (contains a value).
  bool ok() const { return ffi::result_i32_is_ok(*impl_); }

  /// Explicit bool conversion.
  explicit operator bool() const { return ok(); }

  /// Get the value. Behavior is undefined if !ok().
  int value() const { return ffi::result_i32_value(*impl_); }

  /// Get the value or return default if error.
  int value_or(int default_val) const {
    return ffi::result_i32_value_or(*impl_, default_val);
  }

  /// Get the value or return default-constructed value if error.
  int value_or_default() const { return ffi::result_i32_value_or_default(*impl_); }

  /// Dereference operator.
  int operator*() const { return value(); }

  /// Get the status code.
  ffi::StatusCode code() const { return ffi::result_i32_code(*impl_); }

  /// Get the error message (empty if ok).
  std::string message() const {
    auto msg = ffi::result_i32_message(*impl_);
    return std::string(msg.data(), msg.size());
  }

  /// Apply functor if ok, otherwise return error.
  template <typename F>
  auto and_then(F &&f) const -> decltype(f(*this)) {
    if (ok()) {
      return f(*this);
    }
    return RustStatusOrInt(code(), message());
  }

  /// Apply functor if error, otherwise return value.
  template <typename F>
  auto or_else(F &&f) const -> decltype(f(*this)) {
    if (!ok()) {
      return f(*this);
    }
    return *this;
  }

  /// Equality comparison.
  bool operator==(const RustStatusOrInt &other) const {
    if (ok() && other.ok()) {
      return value() == other.value();
    }
    if (!ok() && !other.ok()) {
      return code() == other.code();
    }
    return false;
  }

  bool operator!=(const RustStatusOrInt &other) const { return !(*this == other); }

 private:
  // Constructor from raw impl
  explicit RustStatusOrInt(rust::Box<ffi::RustResultI32> impl)
      : impl_(std::move(impl)) {}

  rust::Box<ffi::RustResultI32> impl_;

  // Allow factory functions to construct from impl
  friend RustStatusOrInt GetValueOrError(bool return_error);
  friend RustStatusOrInt Divide(int a, int b);
  friend RustStatusOrInt ParseInt(const std::string &s);
};

/// StatusOr<int64_t> backed by Rust implementation.
class RustStatusOrInt64 {
 public:
  explicit RustStatusOrInt64(int64_t value) : impl_(ffi::result_i64_ok(value)) {}

  RustStatusOrInt64(ffi::StatusCode code, const std::string &msg)
      : impl_(ffi::result_i64_error(code, msg)) {}

  static RustStatusOrInt64 InvalidArgument(const std::string &msg) {
    return RustStatusOrInt64(ffi::StatusCode::InvalidArgument, msg);
  }

  RustStatusOrInt64(const RustStatusOrInt64 &other)
      : impl_(ffi::result_i64_clone(*other.impl_)) {}

  RustStatusOrInt64 &operator=(const RustStatusOrInt64 &other) {
    if (this != &other) {
      impl_ = ffi::result_i64_clone(*other.impl_);
    }
    return *this;
  }

  RustStatusOrInt64(RustStatusOrInt64 &&other) noexcept = default;
  RustStatusOrInt64 &operator=(RustStatusOrInt64 &&other) noexcept = default;

  bool ok() const { return ffi::result_i64_is_ok(*impl_); }
  explicit operator bool() const { return ok(); }
  int64_t value() const { return ffi::result_i64_value(*impl_); }
  int64_t value_or(int64_t default_val) const {
    return ffi::result_i64_value_or(*impl_, default_val);
  }
  int64_t value_or_default() const { return ffi::result_i64_value_or_default(*impl_); }
  int64_t operator*() const { return value(); }
  ffi::StatusCode code() const { return ffi::result_i64_code(*impl_); }
  std::string message() const {
    auto msg = ffi::result_i64_message(*impl_);
    return std::string(msg.data(), msg.size());
  }

  bool operator==(const RustStatusOrInt64 &other) const {
    if (ok() && other.ok()) return value() == other.value();
    if (!ok() && !other.ok()) return code() == other.code();
    return false;
  }
  bool operator!=(const RustStatusOrInt64 &other) const { return !(*this == other); }

 private:
  rust::Box<ffi::RustResultI64> impl_;
};

/// StatusOr<std::string> backed by Rust implementation.
class RustStatusOrString {
 public:
  explicit RustStatusOrString(const std::string &value)
      : impl_(ffi::result_string_ok(value)) {}

  RustStatusOrString(ffi::StatusCode code, const std::string &msg)
      : impl_(ffi::result_string_error(code, msg)) {}

  static RustStatusOrString InvalidArgument(const std::string &msg) {
    return RustStatusOrString(ffi::StatusCode::InvalidArgument, msg);
  }

  RustStatusOrString(const RustStatusOrString &other)
      : impl_(ffi::result_string_clone(*other.impl_)) {}

  RustStatusOrString &operator=(const RustStatusOrString &other) {
    if (this != &other) {
      impl_ = ffi::result_string_clone(*other.impl_);
    }
    return *this;
  }

  RustStatusOrString(RustStatusOrString &&other) noexcept = default;
  RustStatusOrString &operator=(RustStatusOrString &&other) noexcept = default;

  bool ok() const { return ffi::result_string_is_ok(*impl_); }
  explicit operator bool() const { return ok(); }
  std::string value() const {
    auto v = ffi::result_string_value(*impl_);
    return std::string(v.data(), v.size());
  }
  std::string value_or(const std::string &default_val) const {
    return std::string(ffi::result_string_value_or(*impl_, default_val));
  }
  std::string value_or_default() const {
    return std::string(ffi::result_string_value_or_default(*impl_));
  }
  std::string operator*() const { return value(); }
  ffi::StatusCode code() const { return ffi::result_string_code(*impl_); }
  std::string message() const {
    auto msg = ffi::result_string_message(*impl_);
    return std::string(msg.data(), msg.size());
  }

  bool operator==(const RustStatusOrString &other) const {
    if (ok() && other.ok()) return value() == other.value();
    if (!ok() && !other.ok()) return code() == other.code();
    return false;
  }
  bool operator!=(const RustStatusOrString &other) const { return !(*this == other); }

 private:
  rust::Box<ffi::RustResultString> impl_;
};

// Helper functions for testing

inline RustStatusOrInt GetValueOrError(bool return_error) {
  return RustStatusOrInt(ffi::get_value_or_error(return_error));
}

inline RustStatusOrInt Divide(int a, int b) {
  return RustStatusOrInt(ffi::divide(a, b));
}

inline RustStatusOrInt ParseInt(const std::string &s) {
  return RustStatusOrInt(ffi::parse_int(s));
}

}  // namespace ray
