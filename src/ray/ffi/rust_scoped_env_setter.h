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
#include "src/ray/ffi/scoped_env_setter_bridge_gen.h"

namespace ray {

/// RAII wrapper for environment variable manipulation backed by Rust.
/// Sets the environment variable on construction and restores on destruction.
class RustScopedEnvSetter {
 public:
  RustScopedEnvSetter(const std::string &key, const std::string &value)
      : impl_(::ffi::scoped_env_setter_new(key, value)) {}

  // Non-copyable, non-movable (RAII semantics)
  RustScopedEnvSetter(const RustScopedEnvSetter &) = delete;
  RustScopedEnvSetter &operator=(const RustScopedEnvSetter &) = delete;

 private:
  rust::Box<::ffi::RustScopedEnvSetter> impl_;
};

/// Check if an environment variable exists.
inline bool EnvVarExists(const std::string &key) {
  return ::ffi::env_var_exists(key);
}

/// Get the value of an environment variable (empty string if not set).
inline std::string GetEnvVar(const std::string &key) {
  return std::string(::ffi::get_env_var(key));
}

}  // namespace ray
