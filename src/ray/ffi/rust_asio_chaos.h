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

#include <cstdint>
#include <string>

#include "rust/cxx.h"
#include "src/ray/ffi/asio_chaos_bridge_gen.h"

namespace ray {
namespace asio {
namespace testing {

/// Initialize delay configuration from a string.
/// Format: "method1=min:max,method2=min:max,*=min:max"
inline bool Init(const std::string &config) {
  return ffi::asio_chaos_init(config);
}

/// Get a random delay for a method in microseconds.
inline int64_t GetDelayUs(const std::string &method_name) {
  return ffi::asio_chaos_get_delay_us(method_name);
}

/// Clear the delay configuration.
inline void Clear() { ffi::asio_chaos_clear(); }

}  // namespace testing
}  // namespace asio
}  // namespace ray
