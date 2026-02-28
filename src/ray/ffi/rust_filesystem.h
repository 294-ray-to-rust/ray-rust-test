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
#include <cstdint>

#include "rust/cxx.h"

// Include the generated CXX bridge header
#include "src/ray/ffi/filesystem_bridge_gen.h"

namespace ray {

/// Get the filename portion of a path.
/// Equivalent to Python's os.path.basename() for file system paths.
inline std::string RustGetFileName(const std::string& path) {
  return static_cast<std::string>(ffi::rust_get_file_name(path));
}

/// Get the user's temporary directory.
inline std::string RustGetUserTempDir() {
  return static_cast<std::string>(ffi::rust_get_user_temp_dir());
}

/// Check if a character is a directory separator.
inline bool RustIsDirSep(char ch) {
  return ffi::rust_is_dir_sep(static_cast<uint8_t>(ch));
}

/// Join two path components.
inline std::string RustJoinPaths(const std::string& base, const std::string& component) {
  return static_cast<std::string>(ffi::rust_join_paths(base, component));
}

/// Variadic join paths (multiple components)
template<typename... Paths>
std::string RustJoinPaths(const std::string& base, const std::string& first, Paths... rest) {
  return RustJoinPaths(RustJoinPaths(base, first), rest...);
}

}  // namespace ray
