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

"""CXX bridge generation rules for Ray Rust/C++ FFI."""

load("@rules_cc//cc:defs.bzl", "cc_library")

def _ray_cxx_bridge_impl(ctx):
    """Implementation of ray_cxx_bridge rule.

    Generates C++ header and source files from a Rust cxx::bridge module.
    Uses the system cxxbridge tool (installed via cargo install cxxbridge-cmd).
    """
    src = ctx.file.src
    name = ctx.attr.name

    # Output files
    header_out = ctx.actions.declare_file(name + ".h")
    source_out = ctx.actions.declare_file(name + ".cc")

    # Run cxxbridge to generate header
    # Use absolute path since Bazel sandbox doesn't inherit PATH
    cxxbridge_path = "/root/.cargo/bin/cxxbridge"

    ctx.actions.run_shell(
        outputs = [header_out],
        inputs = [src],
        command = "{cxxbridge} {src} --header > {out}".format(
            cxxbridge = cxxbridge_path,
            src = src.path,
            out = header_out.path,
        ),
        mnemonic = "CxxBridgeHeader",
        progress_message = "Generating CXX bridge header for %s" % src.short_path,
    )

    # Run cxxbridge to generate source
    ctx.actions.run_shell(
        outputs = [source_out],
        inputs = [src],
        command = "{cxxbridge} {src} > {out}".format(
            cxxbridge = cxxbridge_path,
            src = src.path,
            out = source_out.path,
        ),
        mnemonic = "CxxBridgeSource",
        progress_message = "Generating CXX bridge source for %s" % src.short_path,
    )

    return [
        DefaultInfo(files = depset([header_out, source_out])),
        CcInfo(
            compilation_context = cc_common.create_compilation_context(
                headers = depset([header_out]),
                includes = depset([header_out.dirname]),
            ),
        ),
    ]

_ray_cxx_bridge_gen = rule(
    implementation = _ray_cxx_bridge_impl,
    attrs = {
        "src": attr.label(
            allow_single_file = [".rs"],
            mandatory = True,
            doc = "The Rust source file containing the cxx::bridge module.",
        ),
    },
    doc = "Generates C++ headers and sources from a Rust cxx::bridge module.",
)

def ray_cxx_bridge(
        name,
        src,
        deps = [],
        visibility = ["//visibility:public"]):
    """Generates C++ code from a Rust cxx::bridge module and creates a cc_library.

    This rule:
    1. Runs cxxbridge to generate .h and .cc files from the Rust source
    2. Creates a cc_library target with the generated code

    Requires cxxbridge-cmd to be installed (cargo install cxxbridge-cmd).

    Args:
        name: The name of the generated cc_library.
        src: The Rust source file containing the cxx::bridge module.
        deps: Additional dependencies for the cc_library.
        visibility: Target visibility.

    Example:
        ray_cxx_bridge(
            name = "status_bridge",
            src = "//src/ray/rust/ray-ffi:src/common/status.rs",
        )
    """
    gen_name = name + "_gen"

    _ray_cxx_bridge_gen(
        name = gen_name,
        src = src,
    )

    cc_library(
        name = name,
        srcs = [":" + gen_name],
        hdrs = [":" + gen_name],
        deps = deps + [
            "@crate_index//:cxx",
        ],
        visibility = visibility,
    )

def ray_cxx_bridge_library(
        name,
        bridge_srcs,
        rust_lib,
        deps = [],
        visibility = ["//visibility:public"]):
    """Creates a complete CXX bridge library from multiple Rust bridge sources.

    This is a convenience rule that combines multiple cxx::bridge modules
    into a single C++ library that links against the Rust FFI library.

    Args:
        name: The name of the combined library.
        bridge_srcs: List of Rust source files containing cxx::bridge modules.
        rust_lib: The Rust shared library to link against.
        deps: Additional C++ dependencies.
        visibility: Target visibility.

    Example:
        ray_cxx_bridge_library(
            name = "ray_ffi_bridge",
            bridge_srcs = [
                "//src/ray/rust/ray-ffi:src/common/status.rs",
                "//src/ray/rust/ray-ffi:src/common/id.rs",
            ],
            rust_lib = "//src/ray/rust/ray-ffi:ray_ffi_shared",
        )
    """
    bridge_targets = []

    for i, src in enumerate(bridge_srcs):
        bridge_name = name + "_bridge_" + str(i)
        ray_cxx_bridge(
            name = bridge_name,
            src = src,
        )
        bridge_targets.append(":" + bridge_name)

    cc_library(
        name = name,
        deps = bridge_targets + [rust_lib] + deps,
        visibility = visibility,
    )
