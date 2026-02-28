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

#include <functional>
#include <string>
#include <vector>
#include <cstdint>

#include "rust/cxx.h"

// Include the generated CXX bridge header
#include "src/ray/ffi/counter_map_bridge_gen.h"

namespace ray {

/// CounterMap backed by Rust implementation.
/// Matches the interface of ray::CounterMap<std::string>.
class RustCounterMap {
 public:
  RustCounterMap() : impl_(ffi::counter_map_new()) {}

  void Increment(const std::string& key, int64_t value = 1) {
    ffi::counter_map_increment(*impl_, key, value);
  }

  void Decrement(const std::string& key, int64_t value = 1) {
    ffi::counter_map_decrement(*impl_, key, value);
  }

  int64_t Get(const std::string& key) const {
    return ffi::counter_map_get(*impl_, key);
  }

  int64_t Total() const {
    return ffi::counter_map_total(*impl_);
  }

  size_t Size() const {
    return ffi::counter_map_size(*impl_);
  }

  void Swap(const std::string& from, const std::string& to, int64_t value = 1) {
    ffi::counter_map_swap(*impl_, from, to, value);
  }

  size_t NumPendingCallbacks() const {
    return ffi::counter_map_num_pending_callbacks(*impl_);
  }

  void SetOnChangeCallback(std::function<void(const std::string&)> callback) {
    callback_ = std::move(callback);
  }

  void FlushOnChangeCallbacks() {
    if (!callback_) return;
    auto keys = ffi::counter_map_flush_callbacks(*impl_);
    for (const auto& key : keys) {
      callback_(static_cast<std::string>(key));
    }
  }

  template <typename Fn>
  void ForEachEntry(Fn fn) const {
    auto keys = ffi::counter_map_keys(*impl_);
    for (const auto& key : keys) {
      std::string k = static_cast<std::string>(key);
      fn(k, Get(k));
    }
  }

 private:
  rust::Box<ffi::RustCounterMap> impl_;
  std::function<void(const std::string&)> callback_;
};

}  // namespace ray
