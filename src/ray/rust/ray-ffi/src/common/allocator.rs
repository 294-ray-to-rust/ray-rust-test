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

//! FFI bindings for PlasmaAllocator.

use ray_object_manager::plasma::PlasmaAllocator;

/// Wrapper for PlasmaAllocator allocation result.
pub struct RustAllocationResult {
    address: u64,
    size: usize,
    fd: i32,
    is_fallback: bool,
    success: bool,
}

/// FFI wrapper for PlasmaAllocator.
pub struct RustPlasmaAllocator {
    inner: PlasmaAllocator,
}

#[cxx::bridge(namespace = "ffi")]
mod ffi {
    extern "Rust" {
        type RustPlasmaAllocator;
        type RustAllocationResult;

        // PlasmaAllocator constructors
        fn plasma_allocator_new(
            plasma_directory: &str,
            fallback_directory: &str,
            hugepage_enabled: bool,
            footprint_limit: usize,
        ) -> Box<RustPlasmaAllocator>;

        // PlasmaAllocator methods
        fn plasma_allocator_get_footprint_limit(alloc: &RustPlasmaAllocator) -> usize;
        fn plasma_allocator_allocate(
            alloc: &mut RustPlasmaAllocator,
            size: usize,
        ) -> Box<RustAllocationResult>;
        fn plasma_allocator_fallback_allocate(
            alloc: &mut RustPlasmaAllocator,
            size: usize,
        ) -> Box<RustAllocationResult>;
        fn plasma_allocator_free(
            alloc: &mut RustPlasmaAllocator,
            address: u64,
            size: usize,
            is_fallback: bool,
        );
        fn plasma_allocator_allocated(alloc: &RustPlasmaAllocator) -> usize;
        fn plasma_allocator_fallback_allocated(alloc: &RustPlasmaAllocator) -> usize;

        // AllocationResult methods
        fn allocation_result_success(result: &RustAllocationResult) -> bool;
        fn allocation_result_address(result: &RustAllocationResult) -> u64;
        fn allocation_result_size(result: &RustAllocationResult) -> usize;
        fn allocation_result_fd(result: &RustAllocationResult) -> i32;
        fn allocation_result_is_fallback(result: &RustAllocationResult) -> bool;
    }
}

// PlasmaAllocator FFI implementations

fn plasma_allocator_new(
    plasma_directory: &str,
    fallback_directory: &str,
    hugepage_enabled: bool,
    footprint_limit: usize,
) -> Box<RustPlasmaAllocator> {
    Box::new(RustPlasmaAllocator {
        inner: PlasmaAllocator::new(
            plasma_directory,
            fallback_directory,
            hugepage_enabled,
            footprint_limit,
        ),
    })
}

fn plasma_allocator_get_footprint_limit(alloc: &RustPlasmaAllocator) -> usize {
    alloc.inner.get_footprint_limit()
}

fn plasma_allocator_allocate(
    alloc: &mut RustPlasmaAllocator,
    size: usize,
) -> Box<RustAllocationResult> {
    match alloc.inner.allocate(size) {
        Some(allocation) => Box::new(RustAllocationResult {
            address: allocation.address() as u64,
            size: allocation.size(),
            fd: allocation.fd(),
            is_fallback: allocation.is_fallback_allocated(),
            success: true,
        }),
        None => Box::new(RustAllocationResult {
            address: 0,
            size: 0,
            fd: -1,
            is_fallback: false,
            success: false,
        }),
    }
}

fn plasma_allocator_fallback_allocate(
    alloc: &mut RustPlasmaAllocator,
    size: usize,
) -> Box<RustAllocationResult> {
    match alloc.inner.fallback_allocate(size) {
        Some(allocation) => Box::new(RustAllocationResult {
            address: allocation.address() as u64,
            size: allocation.size(),
            fd: allocation.fd(),
            is_fallback: allocation.is_fallback_allocated(),
            success: true,
        }),
        None => Box::new(RustAllocationResult {
            address: 0,
            size: 0,
            fd: -1,
            is_fallback: false,
            success: false,
        }),
    }
}

fn plasma_allocator_free(
    alloc: &mut RustPlasmaAllocator,
    address: u64,
    size: usize,
    is_fallback: bool,
) {
    use ray_object_manager::plasma::Allocation;

    // Reconstruct the allocation to free it
    let allocation = Allocation::new(
        address as *mut u8,
        size,
        -1,    // fd (not needed for free)
        0,     // offset
        0,     // device_num
        size,  // mmap_size
        is_fallback,
    );
    alloc.inner.free(allocation);
}

fn plasma_allocator_allocated(alloc: &RustPlasmaAllocator) -> usize {
    alloc.inner.allocated()
}

fn plasma_allocator_fallback_allocated(alloc: &RustPlasmaAllocator) -> usize {
    alloc.inner.fallback_allocated()
}

// AllocationResult FFI implementations

fn allocation_result_success(result: &RustAllocationResult) -> bool {
    result.success
}

fn allocation_result_address(result: &RustAllocationResult) -> u64 {
    result.address
}

fn allocation_result_size(result: &RustAllocationResult) -> usize {
    result.size
}

fn allocation_result_fd(result: &RustAllocationResult) -> i32 {
    result.fd
}

fn allocation_result_is_fallback(result: &RustAllocationResult) -> bool {
    result.is_fallback
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plasma_allocator_ffi() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dir_path = temp_dir.path().to_str().unwrap();

        let mut alloc = plasma_allocator_new(dir_path, dir_path, false, 1024 * 1024);

        assert_eq!(plasma_allocator_get_footprint_limit(&alloc), 1024 * 1024);
        assert_eq!(plasma_allocator_allocated(&alloc), 0);

        // Test primary allocation
        let result = plasma_allocator_allocate(&mut alloc, 1000);
        assert!(allocation_result_success(&result));
        assert!(!allocation_result_is_fallback(&result));

        let addr = allocation_result_address(&result);
        let size = allocation_result_size(&result);
        assert_eq!(plasma_allocator_allocated(&alloc), 1000);

        // Free
        plasma_allocator_free(&mut alloc, addr, size, false);
        assert_eq!(plasma_allocator_allocated(&alloc), 0);
    }

    #[test]
    fn test_plasma_allocator_fallback_ffi() {
        let temp_dir = tempfile::tempdir().unwrap();
        let dir_path = temp_dir.path().to_str().unwrap();

        let mut alloc = plasma_allocator_new(dir_path, dir_path, false, 1024 * 1024);

        // Test fallback allocation
        let result = plasma_allocator_fallback_allocate(&mut alloc, 1000);
        assert!(allocation_result_success(&result));
        assert!(allocation_result_is_fallback(&result));

        let addr = allocation_result_address(&result);
        let size = allocation_result_size(&result);
        assert_eq!(plasma_allocator_allocated(&alloc), 1000);
        assert_eq!(plasma_allocator_fallback_allocated(&alloc), 1000);

        // Free
        plasma_allocator_free(&mut alloc, addr, size, true);
        assert_eq!(plasma_allocator_allocated(&alloc), 0);
        assert_eq!(plasma_allocator_fallback_allocated(&alloc), 0);
    }
}
