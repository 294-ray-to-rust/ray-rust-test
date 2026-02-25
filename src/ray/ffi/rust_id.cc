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

#include "ray/ffi/rust_id.h"

// This file is intentionally mostly empty. The Rust ID wrapper classes are
// header-only for simplicity, with all logic delegating to the Rust FFI
// functions. This .cc file exists to:
// 1. Ensure the header compiles correctly
// 2. Provide a place for any future non-inline implementations
// 3. Serve as the compilation unit for the cc_library target

namespace ray {

// Static assertions to verify ID sizes match
static_assert(kRustJobIdSize == 4, "JobID size must be 4 bytes");
static_assert(kRustActorIdSize == 16, "ActorID size must be 16 bytes");
static_assert(kRustTaskIdSize == 24, "TaskID size must be 24 bytes");
static_assert(kRustObjectIdSize == 28, "ObjectID size must be 28 bytes");

}  // namespace ray
