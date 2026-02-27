# Ray Rust FFI Implementation - Session 3

## Summary

This session focused on implementing the `PlasmaAllocator` with mmap support, matching Ray's C++ implementation pattern for memory-mapped allocations.

## What Was Implemented

### PlasmaAllocator with mmap-rs

Implemented a proper `PlasmaAllocator` in Rust using the `mmap-rs` crate:

- **Primary allocation**: Anonymous mmap for shared memory (fast, in-memory)
- **Fallback allocation**: File-backed mmap via tempfile for disk spilling when primary memory is exhausted
- **Capacity tracking**: Only primary allocations count against the footprint limit (matching Ray's C++ behavior)
- **64-byte alignment**: Matching Ray's allocation alignment for hash computation optimization

### Key Implementation Details

**Files Modified/Created:**
- `src/ray/rust/ray-object-manager/src/plasma/allocator.rs` - Added `PlasmaAllocator` struct with mmap support
- `src/ray/rust/ray-object-manager/src/plasma/mod.rs` - Export `PlasmaAllocator`
- `src/ray/rust/ray-object-manager/src/plasma/lifecycle.rs` - Fixed test for eager deletion behavior
- `src/ray/rust/ray-object-manager/BUILD.bazel` - Added mmap-rs and tempfile deps
- `src/ray/rust/Cargo.toml` - Added mmap-rs, tempfile dependencies
- `src/ray/rust/ray-ffi/src/common/plasma.rs` - Fixed `binary()` -> `to_binary()` method call
- `Dockerfile.test` - Updated to use Rust nightly for edition2024 support

### Bug Fixes

1. **Capacity check fix**: The original implementation counted both primary and fallback allocations against the limit. Fixed to only count primary allocations.

2. **Lifecycle eager deletion**: Fixed test to expect objects to be auto-deleted when the last reference is removed (eager deletion behavior).

3. **Dependency pinning**: Pinned `tempfile = "=3.14.0"` to avoid `getrandom 0.4` which requires Rust edition 2024 (not supported by Bazel's Rust toolchain).

## Test Results

### Rust Tests
- **51 unit tests** - all pass
- **8 integration tests** - all pass

### C++ FFI Tests (via Bazel)
All 9 tests pass:
- `rust_eviction_policy_test`
- `rust_id_test`
- `rust_lifecycle_test`
- `rust_object_store_test`
- `rust_resource_instance_set_test`
- `rust_resource_request_test`
- `rust_resource_set_test`
- `rust_scheduling_ids_test`
- `rust_status_test`

## Architecture

The PlasmaAllocator matches Ray's C++ pattern:

```
┌─────────────────────────────────────────────────────────┐
│                    PlasmaAllocator                       │
├─────────────────────────────────────────────────────────┤
│  Primary Pool (Anonymous mmap)                          │
│  - Fast shared memory                                   │
│  - Limited by footprint_limit                           │
│  - allocate() method                                    │
├─────────────────────────────────────────────────────────┤
│  Fallback Pool (File-backed mmap)                       │
│  - Disk-based storage                                   │
│  - Unlimited (disk space)                               │
│  - fallback_allocate() method                           │
├─────────────────────────────────────────────────────────┤
│  Tracking:                                              │
│  - total_allocated: primary + fallback bytes            │
│  - fallback_allocated: only fallback bytes              │
│  - Capacity check uses (total - fallback) vs limit      │
└─────────────────────────────────────────────────────────┘
```

## Remaining Tests (Future Work)

The following more complex tests still need implementation:
- `fallback_allocator_test` - Now has Rust allocator, needs C++ FFI wrapper
- `mutable_object_test` - Requires semaphores and threading
- `create_request_queue_test` - Requires async callbacks
- `get_request_queue_test` - Requires boost::asio
- `pull_manager_test`, `push_manager_test` - Require RPC mocking
- `label_selector_test`, `fallback_strategy_test` - Require protobuf

## Cost Information

```
Total cost:            $14.70
Total duration (API):  21m 2s
Total duration (wall): 2h 34m 20s
Total code changes:    2328 lines added, 76 lines removed
Usage by model:
        claude-haiku:  72.9k input, 2.5k output, 0 cache read, 10.5k cache write ($0.0983)
     claude-opus-4-5:  1.1k input, 72.6k output, 12.3m cache read, 1.1m cache write ($14.60)
```

## Dependencies Added

```toml
[workspace.dependencies]
mmap-rs = "0.6"
tempfile = "=3.14.0"  # Pinned to avoid edition2024 deps
```
