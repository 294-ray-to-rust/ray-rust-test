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

#include <memory>
#include <string>

#include "rust/cxx.h"

// Include the generated CXX bridge header
#include "src/ray/ffi/grpc_util_bridge_gen.h"

namespace ray {

/// A LabelIn structure backed by Rust implementation.
/// Mimics protobuf's LabelIn message with a list of string values.
class RustLabelIn {
 public:
  RustLabelIn() : impl_(ffi::label_in_new()) {}

  /// Copy constructor.
  RustLabelIn(const RustLabelIn &other) : impl_(ffi::label_in_clone(*other.impl_)) {}

  /// Copy assignment.
  RustLabelIn &operator=(const RustLabelIn &other) {
    if (this != &other) {
      impl_ = ffi::label_in_clone(*other.impl_);
    }
    return *this;
  }

  /// Move constructor.
  RustLabelIn(RustLabelIn &&other) noexcept = default;

  /// Move assignment.
  RustLabelIn &operator=(RustLabelIn &&other) noexcept = default;

  /// Add a value to the list.
  void AddValue(const std::string &value) {
    ffi::label_in_add_value(*impl_, rust::String(value));
  }

  /// Equality comparison.
  bool operator==(const RustLabelIn &other) const {
    return ffi::label_in_equals(*impl_, *other.impl_);
  }

  bool operator!=(const RustLabelIn &other) const { return !(*this == other); }

  /// Get the underlying implementation (for map insertion).
  rust::Box<ffi::RustLabelIn> TakeImpl() { return std::move(impl_); }

  /// Clone the implementation for map insertion.
  rust::Box<ffi::RustLabelIn> CloneImpl() const {
    return ffi::label_in_clone(*impl_);
  }

 private:
  rust::Box<ffi::RustLabelIn> impl_;
};

/// A map from string to double backed by Rust implementation.
class RustDoubleMap {
 public:
  RustDoubleMap() : impl_(ffi::double_map_new()) {}

  /// Insert a key-value pair.
  void Insert(const std::string &key, double value) {
    ffi::double_map_insert(*impl_, rust::String(key), value);
  }

  /// Operator[] for convenient insertion.
  class Proxy {
   public:
    Proxy(RustDoubleMap &map, const std::string &key) : map_(map), key_(key) {}
    Proxy &operator=(double value) {
      map_.Insert(key_, value);
      return *this;
    }

   private:
    RustDoubleMap &map_;
    std::string key_;
  };

  Proxy operator[](const std::string &key) { return Proxy(*this, key); }

  /// Get size.
  size_t Size() const { return ffi::double_map_len(*impl_); }

  /// Check if empty.
  bool Empty() const { return ffi::double_map_is_empty(*impl_); }

  /// Get the underlying implementation for comparison.
  const ffi::RustDoubleMap &GetImpl() const { return *impl_; }

 private:
  rust::Box<ffi::RustDoubleMap> impl_;
};

/// Check if two double maps are equal.
inline bool MapEqual(const RustDoubleMap &lhs, const RustDoubleMap &rhs) {
  return ffi::double_map_equals(lhs.GetImpl(), rhs.GetImpl());
}

/// A map from string to LabelIn backed by Rust implementation.
class RustLabelInMap {
 public:
  RustLabelInMap() : impl_(ffi::label_in_map_new()) {}

  /// Insert a key-value pair.
  void Insert(const std::string &key, const RustLabelIn &value) {
    ffi::label_in_map_insert(*impl_, rust::String(key), value.CloneImpl());
  }

  /// Operator[] for convenient insertion.
  class Proxy {
   public:
    Proxy(RustLabelInMap &map, const std::string &key) : map_(map), key_(key) {}
    Proxy &operator=(const RustLabelIn &value) {
      map_.Insert(key_, value);
      return *this;
    }

   private:
    RustLabelInMap &map_;
    std::string key_;
  };

  Proxy operator[](const std::string &key) { return Proxy(*this, key); }

  /// Get size.
  size_t Size() const { return ffi::label_in_map_len(*impl_); }

  /// Check if empty.
  bool Empty() const { return ffi::label_in_map_is_empty(*impl_); }

  /// Get the underlying implementation for comparison.
  const ffi::RustLabelInMap &GetImpl() const { return *impl_; }

 private:
  rust::Box<ffi::RustLabelInMap> impl_;
};

/// Check if two LabelIn maps are equal.
inline bool MapEqual(const RustLabelInMap &lhs, const RustLabelInMap &rhs) {
  return ffi::label_in_map_equals(lhs.GetImpl(), rhs.GetImpl());
}

}  // namespace ray
