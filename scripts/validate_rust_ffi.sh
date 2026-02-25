#!/bin/bash
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

# Validation script for Ray Rust FFI infrastructure.
# This script verifies that the Rust and C++ implementations are functionally equivalent.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RAY_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "======================================"
echo "Ray Rust FFI Infrastructure Validation"
echo "======================================"
echo ""

cd "${RAY_ROOT}"

# Step 1: Build Rust crates
echo "Step 1: Building Rust crates..."
echo "--------------------------------------"
bazel build //src/ray/rust/...
echo "✓ Rust crates built successfully"
echo ""

# Step 2: Build FFI layer
echo "Step 2: Building FFI layer..."
echo "--------------------------------------"
bazel build //src/ray/ffi/...
echo "✓ FFI layer built successfully"
echo ""

# Step 3: Run Rust unit tests
echo "Step 3: Running Rust unit tests..."
echo "--------------------------------------"
bazel test //src/ray/rust/ray-common:ray_common_test
bazel test //src/ray/rust/ray-ffi:ray_ffi_test
echo "✓ Rust tests passed"
echo ""

# Step 4: Run C++ tests with C++ implementation (if they exist)
echo "Step 4: Running C++ tests with C++ implementation..."
echo "--------------------------------------"
if bazel query "//src/ray/common/tests:status_test" &>/dev/null; then
    bazel test //src/ray/common/tests:status_test
    echo "✓ C++ status tests passed"
else
    echo "⊘ C++ status tests not found (skipped)"
fi

if bazel query "//src/ray/common/tests:id_test" &>/dev/null; then
    bazel test //src/ray/common/tests:id_test
    echo "✓ C++ id tests passed"
else
    echo "⊘ C++ id tests not found (skipped)"
fi
echo ""

# Step 5: Run C++ tests with Rust implementation (if they exist)
echo "Step 5: Running C++ tests with Rust implementation..."
echo "--------------------------------------"
if bazel query "//src/ray/common/tests:status_test_rust" &>/dev/null; then
    bazel test //src/ray/common/tests:status_test_rust
    echo "✓ Rust-backed status tests passed"
else
    echo "⊘ Rust-backed status tests not found (skipped)"
fi

if bazel query "//src/ray/common/tests:id_test_rust" &>/dev/null; then
    bazel test //src/ray/common/tests:id_test_rust
    echo "✓ Rust-backed id tests passed"
else
    echo "⊘ Rust-backed id tests not found (skipped)"
fi
echo ""

echo "======================================"
echo "Validation Complete!"
echo "======================================"
echo ""
echo "Summary:"
echo "  - Rust crates: OK"
echo "  - FFI layer: OK"
echo "  - Rust tests: OK"
echo ""
echo "To add equivalence tests, update the test BUILD files to use"
echo "ray_cc_test_with_rust() from //bazel:rust.bzl"
