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
#include <set>
#include <string>
#include <vector>

#include "rust/cxx.h"

// Include the generated CXX bridge header
#include "src/ray/ffi/label_selector_bridge_gen.h"

namespace ray {

/// Label selector operator enum matching Rust implementation.
enum class RustLabelSelectorOperator {
  LABEL_OPERATOR_UNSPECIFIED = 0,
  LABEL_IN = 1,
  LABEL_NOT_IN = 2
};

/// A label constraint with key, operator, and values.
class RustLabelConstraint {
 public:
  RustLabelConstraint(std::string key, RustLabelSelectorOperator op,
                      std::set<std::string> values)
      : key_(std::move(key)), op_(op), values_(std::move(values)) {}

  const std::string &GetLabelKey() const { return key_; }
  RustLabelSelectorOperator GetOperator() const { return op_; }
  const std::set<std::string> &GetLabelValues() const { return values_; }

 private:
  std::string key_;
  RustLabelSelectorOperator op_;
  std::set<std::string> values_;
};

/// Label selector backed by Rust implementation.
class RustLabelSelector {
 public:
  RustLabelSelector() : impl_(ffi::label_selector_new()) {}

  /// Create from a map of key-value pairs.
  template <typename MapType>
  explicit RustLabelSelector(const MapType &label_selector)
      : impl_(ffi::label_selector_new()) {
    for (const auto &[k, v] : label_selector) {
      AddConstraint(std::string(k), std::string(v));
    }
  }

  /// Add a constraint by parsing a string value.
  void AddConstraint(const std::string &key, const std::string &value) {
    ffi::label_selector_add_constraint_str(*impl_, key, value);
  }

  /// Add a constraint with explicit operator and values.
  void AddConstraint(const RustLabelConstraint &constraint) {
    rust::Vec<rust::String> values;
    for (const auto &v : constraint.GetLabelValues()) {
      values.push_back(rust::String(v));
    }
    ffi::LabelOperator op;
    switch (constraint.GetOperator()) {
      case RustLabelSelectorOperator::LABEL_IN:
        op = ffi::LabelOperator::In;
        break;
      case RustLabelSelectorOperator::LABEL_NOT_IN:
        op = ffi::LabelOperator::NotIn;
        break;
      default:
        op = ffi::LabelOperator::Unspecified;
        break;
    }
    ffi::label_selector_add_constraint(*impl_, constraint.GetLabelKey(), op, values);
  }

  /// Get all constraints.
  std::vector<RustLabelConstraint> GetConstraints() const {
    std::vector<RustLabelConstraint> result;
    auto constraints = ffi::label_selector_get_all_constraints(*impl_);
    for (const auto &c : constraints) {
      RustLabelSelectorOperator op;
      switch (c.op) {
        case ffi::LabelOperator::In:
          op = RustLabelSelectorOperator::LABEL_IN;
          break;
        case ffi::LabelOperator::NotIn:
          op = RustLabelSelectorOperator::LABEL_NOT_IN;
          break;
        default:
          op = RustLabelSelectorOperator::LABEL_OPERATOR_UNSPECIFIED;
          break;
      }
      std::set<std::string> values;
      for (const auto &v : c.values) {
        values.insert(std::string(v));
      }
      result.emplace_back(std::string(c.key), op, std::move(values));
    }
    return result;
  }

  /// Convert back to a string map.
  std::map<std::string, std::string> ToStringMap() const {
    auto keys = ffi::label_selector_to_string_map_keys(*impl_);
    auto values = ffi::label_selector_to_string_map_values(*impl_);
    std::map<std::string, std::string> result;
    for (size_t i = 0; i < keys.size(); ++i) {
      result[std::string(keys[i])] = std::string(values[i]);
    }
    return result;
  }

  /// Get debug string representation.
  std::string DebugString() const {
    return std::string(ffi::label_selector_debug_string(*impl_));
  }

  /// Number of constraints.
  size_t Size() const { return ffi::label_selector_num_constraints(*impl_); }

  /// Equality comparison.
  bool operator==(const RustLabelSelector &other) const {
    return ffi::label_selector_equals(*impl_, *other.impl_);
  }

  bool operator!=(const RustLabelSelector &other) const { return !(*this == other); }

 private:
  rust::Box<ffi::RustLabelSelector> impl_;
};

}  // namespace ray
