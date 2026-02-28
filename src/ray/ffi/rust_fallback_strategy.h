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

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "rust/cxx.h"

// Include the generated CXX bridge header
#include "src/ray/ffi/fallback_strategy_bridge_gen.h"

namespace ray {

/// A fallback option backed by Rust implementation.
class RustFallbackOption {
 public:
  /// Create from a map of key-value pairs.
  template <typename MapType>
  explicit RustFallbackOption(const MapType &label_selector)
      : impl_(ffi::fallback_option_empty()) {
    rust::Vec<rust::String> keys;
    rust::Vec<rust::String> values;
    for (const auto &[k, v] : label_selector) {
      keys.push_back(rust::String(std::string(k)));
      values.push_back(rust::String(std::string(v)));
    }
    impl_ = ffi::fallback_option_new(keys, values);
  }

  /// Create an empty option.
  RustFallbackOption() : impl_(ffi::fallback_option_empty()) {}

  /// Copy constructor.
  RustFallbackOption(const RustFallbackOption &other)
      : impl_(ffi::fallback_option_clone(*other.impl_)) {}

  /// Copy assignment.
  RustFallbackOption &operator=(const RustFallbackOption &other) {
    if (this != &other) {
      impl_ = ffi::fallback_option_clone(*other.impl_);
    }
    return *this;
  }

  /// Move constructor.
  RustFallbackOption(RustFallbackOption &&other) noexcept = default;

  /// Move assignment.
  RustFallbackOption &operator=(RustFallbackOption &&other) noexcept = default;

  /// Get label selector as a string map.
  std::map<std::string, std::string> GetSelectorMap() const {
    auto keys = ffi::fallback_option_get_selector_keys(*impl_);
    auto values = ffi::fallback_option_get_selector_values(*impl_);
    std::map<std::string, std::string> result;
    for (size_t i = 0; i < keys.size(); ++i) {
      result[std::string(keys[i])] = std::string(values[i]);
    }
    return result;
  }

  /// Equality comparison.
  bool operator==(const RustFallbackOption &other) const {
    return ffi::fallback_option_equals(*impl_, *other.impl_);
  }

  bool operator!=(const RustFallbackOption &other) const {
    return !(*this == other);
  }

  /// Hash value for use in hash containers.
  size_t Hash() const { return ffi::fallback_option_hash(*impl_); }

 private:
  rust::Box<ffi::RustFallbackOption> impl_;

  // Allow RustFallbackStrategy to access impl_
  friend class RustFallbackStrategy;
};

/// A fallback strategy backed by Rust implementation.
class RustFallbackStrategy {
 public:
  RustFallbackStrategy() : impl_(ffi::fallback_strategy_new()) {}

  /// Add an option from a label selector map.
  template <typename MapType>
  void AddOption(const MapType &label_selector) {
    rust::Vec<rust::String> keys;
    rust::Vec<rust::String> values;
    for (const auto &[k, v] : label_selector) {
      keys.push_back(rust::String(std::string(k)));
      values.push_back(rust::String(std::string(v)));
    }
    ffi::fallback_strategy_add_option(*impl_, keys, values);
  }

  /// Add an existing option.
  void AddOption(const RustFallbackOption &option) {
    auto keys = ffi::fallback_option_get_selector_keys(*option.impl_);
    auto values = ffi::fallback_option_get_selector_values(*option.impl_);
    ffi::fallback_strategy_add_option(*impl_, keys, values);
  }

  /// Get number of options.
  size_t Size() const { return ffi::fallback_strategy_len(*impl_); }

  /// Check if empty.
  bool Empty() const { return ffi::fallback_strategy_is_empty(*impl_); }

  /// Get option at index.
  RustFallbackOption GetOption(size_t index) const {
    auto data = ffi::fallback_strategy_get_option(*impl_, index);
    std::map<std::string, std::string> selector;
    for (size_t i = 0; i < data.keys.size(); ++i) {
      selector[std::string(data.keys[i])] = std::string(data.values[i]);
    }
    return RustFallbackOption(selector);
  }

  /// Equality comparison.
  bool operator==(const RustFallbackStrategy &other) const {
    return ffi::fallback_strategy_equals(*impl_, *other.impl_);
  }

  bool operator!=(const RustFallbackStrategy &other) const {
    return !(*this == other);
  }

  /// Serialize to a vector of option data.
  std::vector<std::map<std::string, std::string>> Serialize() const {
    auto data = ffi::fallback_strategy_serialize(*impl_);
    std::vector<std::map<std::string, std::string>> result;
    for (const auto &d : data) {
      std::map<std::string, std::string> option_map;
      for (size_t i = 0; i < d.keys.size(); ++i) {
        option_map[std::string(d.keys[i])] = std::string(d.values[i]);
      }
      result.push_back(std::move(option_map));
    }
    return result;
  }

  /// Parse from serialized data.
  static RustFallbackStrategy Parse(
      const std::vector<std::map<std::string, std::string>> &data) {
    rust::Vec<ffi::FallbackOptionData> rust_data;
    for (const auto &option_map : data) {
      ffi::FallbackOptionData d;
      for (const auto &[k, v] : option_map) {
        d.keys.push_back(rust::String(k));
        d.values.push_back(rust::String(v));
      }
      rust_data.push_back(std::move(d));
    }
    RustFallbackStrategy strategy;
    strategy.impl_ = ffi::fallback_strategy_parse(rust_data);
    return strategy;
  }

 private:
  rust::Box<ffi::RustFallbackStrategy> impl_;
};

}  // namespace ray

// Hash specialization for RustFallbackOption
namespace std {
template <>
struct hash<ray::RustFallbackOption> {
  size_t operator()(const ray::RustFallbackOption &opt) const { return opt.Hash(); }
};
}  // namespace std
