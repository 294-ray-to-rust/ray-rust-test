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

"""Custom Rust rules for Ray following ray.bzl patterns."""

load("@rules_rust//rust:defs.bzl", "rust_library", "rust_test", "rust_shared_library")
load("@rules_cc//cc:defs.bzl", "cc_library", "cc_test")
load("//bazel:ray.bzl", "COPTS", "COPTS_TESTS")

def ray_rust_library(
        name,
        srcs,
        deps = [],
        proc_macro_deps = [],
        crate_features = [],
        edition = "2021",
        visibility = ["//visibility:public"],
        **kwargs):
    """Creates a Rust library with Ray-specific defaults.

    Args:
        name: The name of the library.
        srcs: Source files for the library.
        deps: Dependencies for the library.
        proc_macro_deps: Procedural macro dependencies.
        crate_features: Crate features to enable.
        edition: Rust edition (default: 2021).
        visibility: Target visibility.
        **kwargs: Additional arguments passed to rust_library.
    """
    rust_library(
        name = name,
        srcs = srcs,
        deps = deps,
        proc_macro_deps = proc_macro_deps,
        crate_features = crate_features,
        edition = edition,
        visibility = visibility,
        **kwargs
    )

def ray_rust_shared_library(
        name,
        srcs,
        deps = [],
        proc_macro_deps = [],
        crate_features = [],
        edition = "2021",
        visibility = ["//visibility:public"],
        **kwargs):
    """Creates a Rust shared library (.so) with Ray-specific defaults.

    This is used for FFI integration with C++.

    Args:
        name: The name of the library.
        srcs: Source files for the library.
        deps: Dependencies for the library.
        proc_macro_deps: Procedural macro dependencies.
        crate_features: Crate features to enable.
        edition: Rust edition (default: 2021).
        visibility: Target visibility.
        **kwargs: Additional arguments passed to rust_shared_library.
    """
    rust_shared_library(
        name = name,
        srcs = srcs,
        deps = deps,
        proc_macro_deps = proc_macro_deps,
        crate_features = crate_features,
        edition = edition,
        visibility = visibility,
        **kwargs
    )

def ray_rust_test(
        name,
        srcs = None,
        crate = None,
        deps = [],
        proc_macro_deps = [],
        crate_features = [],
        edition = "2021",
        **kwargs):
    """Creates a Rust test with Ray-specific defaults.

    Args:
        name: The name of the test.
        srcs: Source files for the test (mutually exclusive with crate).
        crate: The crate to test (mutually exclusive with srcs).
        deps: Dependencies for the test.
        proc_macro_deps: Procedural macro dependencies.
        crate_features: Crate features to enable.
        edition: Rust edition (default: 2021).
        **kwargs: Additional arguments passed to rust_test.
    """
    if crate:
        rust_test(
            name = name,
            crate = crate,
            deps = deps,
            proc_macro_deps = proc_macro_deps,
            crate_features = crate_features,
            **kwargs
        )
    else:
        rust_test(
            name = name,
            srcs = srcs,
            deps = deps,
            proc_macro_deps = proc_macro_deps,
            crate_features = crate_features,
            edition = edition,
            **kwargs
        )

def ray_cc_library_with_rust(
        name,
        srcs = [],
        hdrs = [],
        deps = [],
        rust_deps = [],
        copts = [],
        strip_include_prefix = "/src",
        visibility = ["//visibility:public"],
        **kwargs):
    """Creates a C++ library that depends on Rust FFI code.

    Args:
        name: The name of the library.
        srcs: C++ source files.
        hdrs: C++ header files.
        deps: C++ dependencies.
        rust_deps: Rust library dependencies (FFI bridges).
        copts: Additional C++ compiler options.
        strip_include_prefix: Include prefix to strip.
        visibility: Target visibility.
        **kwargs: Additional arguments passed to cc_library.
    """
    cc_library(
        name = name,
        srcs = srcs,
        hdrs = hdrs,
        deps = deps + rust_deps,
        copts = COPTS + copts,
        strip_include_prefix = strip_include_prefix,
        visibility = visibility,
        **kwargs
    )

def ray_cc_test_with_rust(
        name,
        srcs,
        deps = [],
        rust_impl_deps = None,
        copts = [],
        linkopts = [],
        **kwargs):
    """Creates C++ tests that can run against both C++ and Rust implementations.

    This rule creates two test targets:
    - {name}: Test using C++ implementation
    - {name}_rust: Test using Rust implementation

    Args:
        name: The name of the test.
        srcs: Test source files.
        deps: Dependencies for the C++ implementation.
        rust_impl_deps: Dict mapping C++ deps to their Rust replacements.
                       If None, only creates the C++ test.
        copts: Additional C++ compiler options.
        linkopts: Additional linker options.
        **kwargs: Additional arguments passed to cc_test.
    """
    # Original C++ test
    cc_test(
        name = name,
        srcs = srcs,
        deps = deps,
        copts = COPTS_TESTS + copts,
        linkopts = linkopts + ["-pie"],
        **kwargs
    )

    # Rust-backed test (if rust_impl_deps is provided)
    if rust_impl_deps:
        rust_deps = []
        for dep in deps:
            if dep in rust_impl_deps:
                rust_deps.append(rust_impl_deps[dep])
            else:
                rust_deps.append(dep)

        cc_test(
            name = name + "_rust",
            srcs = srcs,
            deps = rust_deps,
            copts = COPTS_TESTS + copts,
            linkopts = linkopts + ["-pie"],
            **kwargs
        )
