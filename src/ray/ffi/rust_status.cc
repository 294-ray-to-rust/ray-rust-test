// Copyright 2017 The Ray Authors.
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

#include "ray/ffi/rust_status.h"

// This file is intentionally mostly empty. The RustStatus class is
// header-only for simplicity, with all logic delegating to the Rust
// FFI functions. This .cc file exists to:
// 1. Ensure the header compiles correctly
// 2. Provide a place for any future non-inline implementations
// 3. Serve as the compilation unit for the cc_library target

namespace ray {

// Static assertion to verify StatusCode enum values match
static_assert(static_cast<int>(ffi::StatusCode::Ok) == 0, "StatusCode::Ok must be 0");
static_assert(static_cast<int>(ffi::StatusCode::OutOfMemory) == 1,
              "StatusCode::OutOfMemory must be 1");
static_assert(static_cast<int>(ffi::StatusCode::KeyError) == 2,
              "StatusCode::KeyError must be 2");

}  // namespace ray
