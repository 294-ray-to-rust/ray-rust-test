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

//! Build script for ray-ffi crate.
//!
//! This generates C++ bridge code from the cxx::bridge macros.

fn main() {
    cxx_build::bridges([
        "src/common/status.rs",
        "src/common/id.rs",
        "src/common/scheduling.rs",
        "src/common/plasma.rs",
        "src/common/lifecycle.rs",
    ])
    .flag_if_supported("-std=c++17")
    .compile("ray_ffi");

    println!("cargo:rerun-if-changed=src/lib.rs");
    println!("cargo:rerun-if-changed=src/common/mod.rs");
    println!("cargo:rerun-if-changed=src/common/status.rs");
    println!("cargo:rerun-if-changed=src/common/id.rs");
    println!("cargo:rerun-if-changed=src/common/scheduling.rs");
    println!("cargo:rerun-if-changed=src/common/plasma.rs");
    println!("cargo:rerun-if-changed=src/common/lifecycle.rs");
    println!("cargo:rerun-if-changed=src/callbacks.rs");
}
