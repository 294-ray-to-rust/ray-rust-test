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

#include <string>
#include <vector>
#include <cstdint>

#include "rust/cxx.h"

// Include the generated CXX bridge header
#include "src/ray/ffi/ray_config_bridge_gen.h"

namespace ray {

/// Convert a comma-separated string to a vector of trimmed strings.
/// This is the Rust implementation of ConvertValue<std::vector<std::string>>.
template <typename T>
T RustConvertValue(const std::string& type_string, const std::string& value);

template <>
inline std::vector<std::string> RustConvertValue<std::vector<std::string>>(
    const std::string& type_string, const std::string& value) {
  auto rust_vec = ffi::rust_convert_to_string_vector(value);
  std::vector<std::string> result;
  result.reserve(rust_vec.size());
  for (const auto& s : rust_vec) {
    result.push_back(static_cast<std::string>(s));
  }
  return result;
}

template <>
inline int64_t RustConvertValue<int64_t>(const std::string& type_string,
                                          const std::string& value) {
  return ffi::rust_convert_to_int(value);
}

template <>
inline bool RustConvertValue<bool>(const std::string& type_string,
                                   const std::string& value) {
  return ffi::rust_convert_to_bool(value);
}

template <>
inline double RustConvertValue<double>(const std::string& type_string,
                                       const std::string& value) {
  return ffi::rust_convert_to_float(value);
}

template <>
inline std::string RustConvertValue<std::string>(const std::string& type_string,
                                                  const std::string& value) {
  return value;
}

}  // namespace ray
