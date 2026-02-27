# Copyright 2017 The Ray Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Rust dependency setup for Ray FFI infrastructure."""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def ray_rust_deps():
    """Set up Rust dependencies for Ray FFI infrastructure.

    This function should be called from WORKSPACE after ray_deps_setup().
    """

    # rules_rust for Rust toolchain and build rules
    # Using 0.56.0 which supports Cargo.lock v4 format
    if "rules_rust" not in native.existing_rules():
        http_archive(
            name = "rules_rust",
            sha256 = "f1306aac0b258b790df01ad9abc6abb0df0b65416c74b4ef27f4aab298780a64",
            urls = ["https://github.com/bazelbuild/rules_rust/releases/download/0.56.0/rules_rust-0.56.0.tar.gz"],
        )

def ray_rust_toolchains():
    """Register Rust toolchains.

    This function should be called from WORKSPACE after ray_rust_deps().
    """
    pass  # Toolchain registration happens via load() statements in WORKSPACE

def ray_rust_crate_repositories():
    """Set up crate repositories for Rust dependencies.

    This function should be called from WORKSPACE after ray_rust_toolchains().
    """
    pass  # Crate repository setup happens via load() statements in WORKSPACE
