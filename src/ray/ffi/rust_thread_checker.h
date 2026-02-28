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

#include "rust/cxx.h"
#include "src/ray/ffi/thread_checker_bridge_gen.h"

namespace ray {

/// Thread checker backed by Rust implementation.
/// Validates that calls happen on the same thread that created the checker.
class RustThreadChecker {
 public:
  RustThreadChecker() : impl_(::ffi::thread_checker_new()) {}

  /// Check if the current call is on the same thread that created this checker.
  bool IsOnSameThread() const {
    return ::ffi::thread_checker_is_on_same_thread(*impl_);
  }

  // Non-copyable (thread affinity)
  RustThreadChecker(const RustThreadChecker &) = delete;
  RustThreadChecker &operator=(const RustThreadChecker &) = delete;

 private:
  rust::Box<::ffi::RustThreadChecker> impl_;
};

}  // namespace ray
