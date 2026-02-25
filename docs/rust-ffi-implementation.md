# Ray Rust FFI Infrastructure Implementation

This document describes the implementation of the Rust FFI infrastructure for Ray, enabling C++ tests to run against Rust implementations for functional equivalence validation during incremental migration.

## Overview

**Goal:** Any Ray component implemented in Rust can be swapped in place of its C++ counterpart, with existing C++ tests validating correctness.

**Status:** Complete and working

## Architecture

```
ray-rust-test/
├── WORKSPACE                           # rules_rust, crate_universe setup
├── .bazelrc                            # Rust FFI config (--config=rust-ffi)
├── .bazelversion                       # Bazel 7.6.0
├── Dockerfile.rust-ffi                 # Docker build environment
│
├── bazel/
│   ├── ray_deps_rust.bzl              # Rust dependency setup
│   ├── rust.bzl                       # Custom Rust rules
│   └── cxx_bridge.bzl                 # CXX bridge generation rule
│
├── src/ray/rust/                      # Rust crates workspace
│   ├── Cargo.toml                     # Workspace root
│   ├── Cargo.lock                     # Generated lockfile
│   ├── BUILD.bazel                    # Workspace aliases
│   │
│   ├── ray-common/                    # Pure Rust implementations
│   │   ├── Cargo.toml
│   │   ├── BUILD.bazel
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── status.rs              # StatusCode enum (37 variants)
│   │       ├── id.rs                  # JobId, ActorId, TaskId, ObjectId
│   │       └── config.rs              # RayConfig placeholder
│   │
│   ├── ray-ffi/                       # CXX bridge layer
│   │   ├── Cargo.toml
│   │   ├── BUILD.bazel
│   │   ├── build.rs
│   │   └── src/
│   │       ├── lib.rs
│   │       └── common/
│   │           ├── mod.rs
│   │           ├── status.rs          # Status CXX bridge
│   │           └── id.rs              # ID types CXX bridge
│   │
│   └── ray-test-utils/                # Shared test utilities
│       ├── Cargo.toml
│       ├── BUILD.bazel
│       └── src/lib.rs
│
└── src/ray/ffi/                       # C++ wrapper layer
    ├── BUILD.bazel
    ├── rust_status.h                  # C++ RustStatus class
    ├── rust_status.cc
    ├── rust_id.h                      # C++ ID wrapper classes
    ├── rust_id.cc
    └── tests/
        ├── BUILD.bazel
        └── rust_ffi_test.cc           # C++ tests (16 test cases)
```

## Implementation Details

### Phase 1: Bazel + rules_rust Setup

**Files created/modified:**

1. **`bazel/ray_deps_rust.bzl`** - Rust dependency loading
```python
def ray_rust_deps():
    if "rules_rust" not in native.existing_rules():
        http_archive(
            name = "rules_rust",
            sha256 = "c30dfdf1e86fd50650a76ea645b3a45f2f00667b06187a685e9554e167ca97ee",
            urls = ["https://github.com/bazelbuild/rules_rust/releases/download/0.40.0/rules_rust-v0.40.0.tar.gz"],
        )
```

2. **`WORKSPACE`** - Added rules_rust and crate_universe
```python
# Rust FFI Infrastructure
load("//bazel:ray_deps_rust.bzl", "ray_rust_deps")
ray_rust_deps()

load("@rules_rust//rust:repositories.bzl", "rules_rust_dependencies", "rust_register_toolchains")
rules_rust_dependencies()
rust_register_toolchains(
    edition = "2021",
    versions = ["1.82.0"],
)

# CXX crate for C++/Rust FFI
load("@rules_rust//crate_universe:defs.bzl", "crates_repository")
crates_repository(
    name = "crate_index",
    cargo_lockfile = "//src/ray/rust:Cargo.lock",
    manifests = [
        "//src/ray/rust:Cargo.toml",
        "//src/ray/rust/ray-common:Cargo.toml",
        "//src/ray/rust/ray-ffi:Cargo.toml",
        "//src/ray/rust/ray-test-utils:Cargo.toml",
    ],
)
```

3. **`bazel/rust.bzl`** - Custom Rust rules
   - `ray_rust_library` - Rust library with Ray defaults
   - `ray_rust_shared_library` - Shared library for FFI
   - `ray_rust_test` - Rust tests
   - `ray_cc_library_with_rust` - C++ library depending on Rust
   - `ray_cc_test_with_rust` - C++ test with Rust implementation swap

4. **`bazel/cxx_bridge.bzl`** - CXX bridge code generation
   - `ray_cxx_bridge` - Generates C++ from cxx::bridge macros
   - Uses system `cxxbridge` tool (installed via cargo)

5. **`.bazelversion`** - Updated to 7.6.0

6. **`.bazelrc`** - Added Rust FFI config
```
# Rust FFI configuration (for C++/Rust interop)
# Uses static linking to avoid allocator library issues
build:rust-ffi --dynamic_mode=off
test:rust-ffi --dynamic_mode=off
```

### Phase 2: ray-common Crate

**`src/ray/rust/ray-common/src/status.rs`**
- `StatusCode` enum with 37 variants matching C++
- `RayError` struct with code, message, rpc_code
- `pub type Result<T> = std::result::Result<T, RayError>`

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i8)]
pub enum StatusCode {
    Ok = 0,
    OutOfMemory = 1,
    KeyError = 2,
    // ... 34 more variants
    PermissionDenied = 37,
}
```

**`src/ray/rust/ray-common/src/id.rs`**
- `JobId` (4 bytes), `ActorId` (16 bytes), `TaskId` (24 bytes), `ObjectId` (28 bytes)
- `UniqueId` (28 bytes) - generic ID type
- MurmurHash64A implementation for hashing
- `from_binary`, `to_hex`, `is_nil`, `Hash`, `Eq` implementations

### Phase 3: ray-ffi Crate (CXX Bridge)

**`src/ray/rust/ray-ffi/src/common/status.rs`**
```rust
#[cxx::bridge(namespace = "ray::ffi")]
mod ffi {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    #[repr(i8)]
    enum StatusCode {
        Ok = 0,
        OutOfMemory = 1,
        // ... all variants
    }

    extern "Rust" {
        type RustStatus;
        fn status_ok() -> Box<RustStatus>;
        fn status_error(code: StatusCode, msg: &str) -> Box<RustStatus>;
        fn status_is_ok(status: &RustStatus) -> bool;
        fn status_code(status: &RustStatus) -> StatusCode;
        fn status_message(status: &RustStatus) -> &str;
        // ...
    }
}
```

**`src/ray/rust/ray-ffi/src/common/id.rs`**
```rust
#[cxx::bridge(namespace = "ray::ffi")]
mod ffi {
    extern "Rust" {
        type RustJobId;
        type RustActorId;
        type RustTaskId;
        type RustObjectId;

        fn job_id_from_int(id: u32) -> Box<RustJobId>;
        fn job_id_to_hex(id: &RustJobId) -> String;
        fn job_id_is_nil(id: &RustJobId) -> bool;
        // ... comprehensive FFI functions for all ID types
    }
}
```

### Phase 4: C++ Wrapper Layer

**`src/ray/ffi/rust_status.h`**
```cpp
#include "src/ray/ffi/status_bridge_gen.h"  // Generated by cxxbridge

namespace ray {

class RustStatus {
 public:
  static RustStatus OK();
  static RustStatus KeyError(const std::string& msg);
  // ... factory methods for all status types

  bool ok() const;
  bool IsKeyError() const;
  ffi::StatusCode code() const;
  std::string message() const;

 private:
  rust::Box<ffi::RustStatus> impl_;
};

}  // namespace ray
```

**`src/ray/ffi/rust_id.h`**
```cpp
#include "src/ray/ffi/id_bridge_gen.h"  // Generated by cxxbridge

namespace ray {

class RustJobId {
 public:
  static RustJobId FromInt(uint32_t value);
  static RustJobId FromBinary(const std::string& binary);
  std::string Hex() const;
  bool IsNil() const;
  size_t Hash() const;
  bool operator==(const RustJobId& other) const;
 private:
  rust::Box<ffi::RustJobId> impl_;
};

// Similar classes for RustActorId, RustTaskId, RustObjectId

}  // namespace ray

// std::hash specializations
namespace std {
template <> struct hash<ray::RustJobId> { /*...*/ };
template <> struct hash<ray::RustActorId> { /*...*/ };
// ...
}
```

### Phase 5: Docker Build Environment

**`Dockerfile.rust-ffi`**
```dockerfile
FROM ubuntu:22.04

# Install dependencies
RUN apt-get update && apt-get install -y \
    curl git build-essential python3 python3-pip \
    openjdk-11-jdk zip unzip

# Install Bazelisk
RUN curl -fsSL -o /usr/local/bin/bazel \
    https://github.com/bazelbuild/bazelisk/releases/download/v1.19.0/bazelisk-linux-amd64 \
    && chmod +x /usr/local/bin/bazel

# Install Rust 1.82.0
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
    sh -s -- -y --default-toolchain 1.82.0

# Generate Cargo.lock
RUN cd /ray/src/ray/rust && rm -f Cargo.lock && \
    cargo check --workspace --all-targets 2>/dev/null || true

# Install cxxbridge-cmd for C++ code generation
RUN cargo install cxxbridge-cmd@1.0.194

CMD ["bazel", "build", "--noenable_bzlmod", "//src/ray/rust/..."]
```

## Build and Test Instructions

### Using Docker (Recommended)

```bash
# Build the Docker image
docker build -f Dockerfile.rust-ffi -t ray-rust-ffi .

# Build Rust crates
docker run --rm ray-rust-ffi bazel build --noenable_bzlmod //src/ray/rust/...

# Run Rust tests
docker run --rm ray-rust-ffi bazel test --noenable_bzlmod //src/ray/rust/...

# Build and test C++ FFI (requires static linking mode)
docker run --rm ray-rust-ffi bazel test --noenable_bzlmod --config=rust-ffi \
    //src/ray/rust/... //src/ray/ffi/tests:rust_ffi_test
```

### Test Results

```
//src/ray/rust/ray-common:ray_common_test      PASSED (Rust unit tests)
//src/ray/rust/ray-ffi:ray_ffi_test            PASSED (Rust FFI tests)
//src/ray/ffi/tests:rust_ffi_test              PASSED (16 C++ tests)
```

The C++ test suite validates:
- RustStatus creation and error handling
- All StatusCode variants
- RustJobId, RustActorId, RustTaskId, RustObjectId
- ID serialization (binary, hex)
- ID hashing and equality
- Actor creation task IDs
- Object indices

## Key Technical Decisions

### 1. Static Linking for C++/Rust Interop

Using `--dynamic_mode=off` (configured as `--config=rust-ffi`) to avoid allocator library issues. The rules_rust allocator library (`allocator_library.so`) has undefined symbol errors when dynamically linked.

### 2. System cxxbridge Tool

Instead of trying to get `cxxbridge-cmd` from crates_repository (which has complex binary packaging requirements), we install it directly via `cargo install` in the Docker container. The `cxx_bridge.bzl` rule uses the absolute path `/root/.cargo/bin/cxxbridge`.

### 3. Rust 1.82.0 Toolchain

Required for compatibility with newer crate versions (e.g., `indexmap 2.13.0` requires Rust 1.82+).

### 4. Docker Build Environment

NixOS has limitations with dynamically linked executables, so we use Docker with Ubuntu 22.04 for the build environment.

### 5. ID Byte Sizes

Matching C++ Ray implementation:
- JobId: 4 bytes
- ActorId: 16 bytes (4 job + 12 unique)
- TaskId: 24 bytes (16 actor + 8 unique)
- ObjectId: 28 bytes (24 task + 4 index)

## Dependencies

### Rust Crates (Cargo.toml)
```toml
[workspace.dependencies]
cxx = "1.0"
cxx-build = "1.0"
thiserror = "1.0"
hex = "0.4"
rand = "0.8"
serde = { version = "1.0", features = ["derive"] }
rstest = "0.18"  # Testing
```

### Bazel External
- `@rules_rust` v0.40.0
- `@crate_index` - Generated from Cargo.lock

## Files Created/Modified

| File | Description |
|------|-------------|
| `WORKSPACE` | Added rules_rust, crate_universe |
| `.bazelversion` | Set to 7.6.0 |
| `.bazelrc` | Added rust-ffi config |
| `bazel/ray_deps_rust.bzl` | Rust dependency setup |
| `bazel/rust.bzl` | Custom Rust rules |
| `bazel/cxx_bridge.bzl` | CXX bridge generation |
| `Dockerfile.rust-ffi` | Docker build environment |
| `src/ray/rust/Cargo.toml` | Workspace manifest |
| `src/ray/rust/BUILD.bazel` | Workspace aliases |
| `src/ray/rust/ray-common/*` | Pure Rust implementations |
| `src/ray/rust/ray-ffi/*` | CXX bridge layer |
| `src/ray/rust/ray-test-utils/*` | Test utilities |
| `src/ray/ffi/BUILD.bazel` | C++ wrapper targets |
| `src/ray/ffi/rust_status.h/cc` | C++ Status wrapper |
| `src/ray/ffi/rust_id.h/cc` | C++ ID wrappers |
| `src/ray/ffi/tests/*` | C++ test suite |

## Future Work

1. **Additional Type Bridges**: Extend to RayConfig, scheduling types, etc.
2. **Async Callback Support**: Bridge async Rust code to C++ callbacks
3. **Performance Benchmarks**: Compare Rust vs C++ implementation performance
4. **CI Integration**: Add to Ray's CI pipeline
5. **macOS/Windows Support**: Currently Linux-only via Docker

## Troubleshooting

### Allocator Library Errors
```
undefined symbol: __rdl_alloc
```
**Solution:** Use `--config=rust-ffi` which sets `--dynamic_mode=off`

### cxxbridge Not Found
```
/bin/bash: line 1: cxxbridge: command not found
```
**Solution:** Ensure `cargo install cxxbridge-cmd` was run in Docker build

### Cargo.lock Version Mismatch
```
error: the lock file needs to be updated
```
**Solution:** Regenerate lockfile inside Docker with matching Rust version

## Session Statistics

- **Total Cost:** $18.40
- **API Duration:** 38m 53s
- **Wall Duration:** 2h 2m 21s
- **Code Changes:** 5338 lines added, 274 lines removed
