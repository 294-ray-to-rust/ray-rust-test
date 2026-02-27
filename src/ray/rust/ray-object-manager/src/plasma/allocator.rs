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

//! Memory allocator for the plasma store.

use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, AtomicUsize, Ordering};

use super::common::{Allocation, PlasmaError, PlasmaResult};

// Re-export mmap_rs for use in PlasmaAllocator
use mmap_rs;

/// Trait for memory allocators.
pub trait Allocator: Send + Sync {
    /// Allocate memory of the given size.
    fn allocate(&self, size: usize) -> PlasmaResult<Allocation>;

    /// Free a previously allocated block.
    fn free(&self, allocation: &Allocation) -> PlasmaResult<()>;

    /// Get available memory.
    fn available(&self) -> usize;

    /// Get total capacity.
    fn capacity(&self) -> usize;
}

/// Statistics for an allocator.
#[derive(Debug, Default)]
pub struct AllocatorStats {
    /// Total bytes allocated.
    pub bytes_allocated: AtomicUsize,
    /// Number of allocations.
    pub num_allocations: AtomicUsize,
    /// Number of frees.
    pub num_frees: AtomicUsize,
    /// Peak bytes allocated.
    pub peak_bytes: AtomicUsize,
}

impl AllocatorStats {
    /// Create new stats.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record an allocation.
    pub fn record_alloc(&self, size: usize) {
        let current = self.bytes_allocated.fetch_add(size, Ordering::SeqCst) + size;
        self.num_allocations.fetch_add(1, Ordering::SeqCst);

        // Update peak if needed
        let mut peak = self.peak_bytes.load(Ordering::SeqCst);
        while current > peak {
            match self.peak_bytes.compare_exchange_weak(
                peak,
                current,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(p) => peak = p,
            }
        }
    }

    /// Record a free.
    pub fn record_free(&self, size: usize) {
        self.bytes_allocated.fetch_sub(size, Ordering::SeqCst);
        self.num_frees.fetch_add(1, Ordering::SeqCst);
    }
}

/// A simple heap-based allocator for testing.
pub struct HeapAllocator {
    /// Maximum capacity.
    capacity: usize,
    /// Allocator statistics.
    stats: AllocatorStats,
    /// Track allocations for cleanup.
    allocations: Mutex<HashMap<usize, usize>>, // address -> size
    /// Next fake fd.
    next_fd: AtomicI32,
}

impl HeapAllocator {
    /// Create a new heap allocator.
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            stats: AllocatorStats::new(),
            allocations: Mutex::new(HashMap::new()),
            next_fd: AtomicI32::new(100),
        }
    }

    /// Get allocator statistics.
    pub fn stats(&self) -> &AllocatorStats {
        &self.stats
    }
}

impl Allocator for HeapAllocator {
    fn allocate(&self, size: usize) -> PlasmaResult<Allocation> {
        if size == 0 {
            return Err(PlasmaError::InvalidRequest(
                "Cannot allocate zero bytes".to_string(),
            ));
        }

        // Check capacity
        let current = self.stats.bytes_allocated.load(Ordering::SeqCst);
        if current + size > self.capacity {
            return Err(PlasmaError::OutOfMemory);
        }

        // Allocate memory
        let layout = std::alloc::Layout::from_size_align(size, 8)
            .map_err(|e| PlasmaError::InvalidRequest(e.to_string()))?;

        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        if ptr.is_null() {
            return Err(PlasmaError::OutOfMemory);
        }

        // Track the allocation
        self.allocations.lock().insert(ptr as usize, size);
        self.stats.record_alloc(size);

        let fd = self.next_fd.fetch_add(1, Ordering::SeqCst);

        Ok(Allocation::new(
            ptr,
            size,
            fd,
            0,    // offset
            0,    // device_num (CPU)
            size, // mmap_size
            false,
        ))
    }

    fn free(&self, allocation: &Allocation) -> PlasmaResult<()> {
        let addr = allocation.address() as usize;
        let size = self
            .allocations
            .lock()
            .remove(&addr)
            .ok_or_else(|| PlasmaError::InvalidRequest("Unknown allocation".to_string()))?;

        // Free the memory
        unsafe {
            let layout = std::alloc::Layout::from_size_align_unchecked(size, 8);
            std::alloc::dealloc(allocation.address(), layout);
        }

        self.stats.record_free(size);
        Ok(())
    }

    fn available(&self) -> usize {
        self.capacity
            .saturating_sub(self.stats.bytes_allocated.load(Ordering::SeqCst))
    }

    fn capacity(&self) -> usize {
        self.capacity
    }
}

impl Drop for HeapAllocator {
    fn drop(&mut self) {
        // Free any remaining allocations
        let allocations: Vec<_> = self.allocations.lock().drain().collect();
        for (addr, size) in allocations {
            unsafe {
                let layout = std::alloc::Layout::from_size_align_unchecked(size, 8);
                std::alloc::dealloc(addr as *mut u8, layout);
            }
        }
    }
}

/// A null allocator that rejects all allocations (for testing).
pub struct NullAllocator;

impl Allocator for NullAllocator {
    fn allocate(&self, _size: usize) -> PlasmaResult<Allocation> {
        Err(PlasmaError::OutOfMemory)
    }

    fn free(&self, _allocation: &Allocation) -> PlasmaResult<()> {
        Err(PlasmaError::InvalidRequest("Null allocator".to_string()))
    }

    fn available(&self) -> usize {
        0
    }

    fn capacity(&self) -> usize {
        0
    }
}

/// Holds an mmap allocation along with its metadata.
/// The mmap field is kept to ensure the memory mapping stays alive.
#[allow(dead_code)]
struct MmapAllocation {
    /// The mmap handle (keeps the mapping alive).
    mmap: mmap_rs::MmapMut,
    /// Size of the allocation.
    size: usize,
    /// Whether this is a fallback allocation.
    is_fallback: bool,
}

/// A PlasmaAllocator that uses mmap for memory allocation.
///
/// Primary allocation uses anonymous mmap (shared memory).
/// Fallback allocation uses file-backed mmap for spilling to disk.
pub struct PlasmaAllocator {
    /// Fallback directory for file-backed allocations.
    fallback_directory: std::path::PathBuf,
    /// Total bytes allocated (primary + fallback).
    total_allocated: AtomicUsize,
    /// Total fallback bytes allocated.
    fallback_allocated: AtomicUsize,
    /// Footprint limit (primary capacity).
    footprint_limit: usize,
    /// Track mmap allocations by address.
    allocations: Mutex<HashMap<usize, MmapAllocation>>,
    /// Next fake fd for tracking.
    next_fd: AtomicI32,
    /// Alignment for allocations.
    alignment: usize,
}

impl PlasmaAllocator {
    /// Alignment for allocations (64 bytes, matching Ray's C++ implementation).
    const DEFAULT_ALIGNMENT: usize = 64;

    /// Reserved bytes for internal bookkeeping.
    const RESERVED_BYTES: usize = 256 * std::mem::size_of::<usize>();

    /// Create a new PlasmaAllocator.
    ///
    /// # Arguments
    /// * `_plasma_directory` - Path to plasma directory (for primary mmap on non-Linux)
    /// * `fallback_directory` - Path to fallback directory for file-backed mmap
    /// * `_hugepage_enabled` - Whether hugepages are enabled
    /// * `footprint_limit` - Maximum primary memory allocation
    pub fn new(
        _plasma_directory: &str,
        fallback_directory: &str,
        _hugepage_enabled: bool,
        footprint_limit: usize,
    ) -> Self {
        Self {
            fallback_directory: std::path::PathBuf::from(fallback_directory),
            total_allocated: AtomicUsize::new(0),
            fallback_allocated: AtomicUsize::new(0),
            footprint_limit,
            allocations: Mutex::new(HashMap::new()),
            next_fd: AtomicI32::new(100),
            alignment: Self::DEFAULT_ALIGNMENT,
        }
    }

    /// Get the footprint limit (primary capacity).
    pub fn get_footprint_limit(&self) -> usize {
        self.footprint_limit
    }

    /// Allocate from primary memory pool (anonymous mmap).
    pub fn allocate(&self, size: usize) -> Option<Allocation> {
        if size == 0 {
            return None;
        }

        // Check capacity (accounting for reserved bytes)
        // Only primary allocations count against the limit, not fallback allocations
        let total = self.total_allocated.load(Ordering::SeqCst);
        let fallback = self.fallback_allocated.load(Ordering::SeqCst);
        let primary_allocated = total.saturating_sub(fallback);
        let usable_limit = self.footprint_limit.saturating_sub(Self::RESERVED_BYTES);
        if primary_allocated + size > usable_limit {
            return None;
        }

        // Round up size to alignment
        let aligned_size = (size + self.alignment - 1) & !(self.alignment - 1);

        // Create anonymous mmap
        let mmap_result = mmap_rs::MmapOptions::new(aligned_size)
            .map_err(|e| PlasmaError::InvalidRequest(e.to_string()))
            .and_then(|opts| {
                opts.map_mut().map_err(|e| PlasmaError::IoError(e.to_string()))
            });

        match mmap_result {
            Ok(mmap) => {
                let ptr = mmap.as_ptr() as *mut u8;
                let fd = self.next_fd.fetch_add(1, Ordering::SeqCst);

                // Store the mmap to keep it alive
                let alloc_entry = MmapAllocation {
                    mmap,
                    size: aligned_size,
                    is_fallback: false,
                };
                self.allocations.lock().insert(ptr as usize, alloc_entry);
                self.total_allocated.fetch_add(size, Ordering::SeqCst);

                Some(Allocation::new(
                    ptr,
                    size,
                    fd,
                    0,            // offset
                    0,            // device_num (CPU)
                    aligned_size, // mmap_size
                    false,        // not fallback
                ))
            }
            Err(_) => None,
        }
    }

    /// Allocate from fallback storage (file-backed mmap).
    pub fn fallback_allocate(&self, size: usize) -> Option<Allocation> {
        if size == 0 {
            return None;
        }

        // Round up size to alignment
        let aligned_size = (size + self.alignment - 1) & !(self.alignment - 1);

        // Create a temporary file for the fallback allocation
        let file = tempfile::tempfile_in(&self.fallback_directory).ok()?;

        // Set the file size
        file.set_len(aligned_size as u64).ok()?;

        // Create file-backed mmap
        let mmap_result = unsafe {
            mmap_rs::MmapOptions::new(aligned_size)
                .ok()?
                .with_file(&file, 0)
                .map_mut()
        };

        match mmap_result {
            Ok(mmap) => {
                let ptr = mmap.as_ptr() as *mut u8;
                let fd = self.next_fd.fetch_add(1, Ordering::SeqCst);

                // Store the mmap to keep it alive
                let alloc_entry = MmapAllocation {
                    mmap,
                    size: aligned_size,
                    is_fallback: true,
                };
                self.allocations.lock().insert(ptr as usize, alloc_entry);
                self.total_allocated.fetch_add(size, Ordering::SeqCst);
                self.fallback_allocated.fetch_add(size, Ordering::SeqCst);

                Some(Allocation::new(
                    ptr,
                    size,
                    fd,
                    0,            // offset
                    0,            // device_num (CPU)
                    aligned_size, // mmap_size
                    true,         // is fallback
                ))
            }
            Err(_) => None,
        }
    }

    /// Free an allocation.
    pub fn free(&self, allocation: Allocation) {
        let addr = allocation.address() as usize;
        let size = allocation.size();

        if let Some(mmap_alloc) = self.allocations.lock().remove(&addr) {
            self.total_allocated.fetch_sub(size, Ordering::SeqCst);
            if mmap_alloc.is_fallback {
                self.fallback_allocated.fetch_sub(size, Ordering::SeqCst);
            }
            // mmap_alloc.mmap is dropped here, unmapping the memory
        }
    }

    /// Get total bytes allocated (primary + fallback).
    pub fn allocated(&self) -> usize {
        self.total_allocated.load(Ordering::SeqCst)
    }

    /// Get total fallback bytes allocated.
    pub fn fallback_allocated(&self) -> usize {
        self.fallback_allocated.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_heap_allocator_creation() {
        let allocator = HeapAllocator::new(1024 * 1024);
        assert_eq!(allocator.capacity(), 1024 * 1024);
        assert_eq!(allocator.available(), 1024 * 1024);
    }

    #[test]
    fn test_heap_allocator_allocate() {
        let allocator = HeapAllocator::new(1024 * 1024);

        let alloc = allocator.allocate(100).unwrap();
        assert_eq!(alloc.size(), 100);
        assert!(!alloc.address().is_null());
        assert_eq!(alloc.device_num(), 0);
        assert!(!alloc.is_fallback_allocated());

        assert_eq!(allocator.available(), 1024 * 1024 - 100);
        assert_eq!(allocator.stats().num_allocations.load(Ordering::SeqCst), 1);

        allocator.free(&alloc).unwrap();
        assert_eq!(allocator.available(), 1024 * 1024);
    }

    #[test]
    fn test_heap_allocator_out_of_memory() {
        let allocator = HeapAllocator::new(100);

        let result = allocator.allocate(200);
        assert!(matches!(result, Err(PlasmaError::OutOfMemory)));
    }

    #[test]
    fn test_heap_allocator_zero_size() {
        let allocator = HeapAllocator::new(1024);

        let result = allocator.allocate(0);
        assert!(matches!(result, Err(PlasmaError::InvalidRequest(_))));
    }

    #[test]
    fn test_heap_allocator_multiple_allocations() {
        let allocator = HeapAllocator::new(1024 * 1024);

        let alloc1 = allocator.allocate(100).unwrap();
        let alloc2 = allocator.allocate(200).unwrap();
        let alloc3 = allocator.allocate(300).unwrap();

        assert_eq!(allocator.stats().num_allocations.load(Ordering::SeqCst), 3);
        assert_eq!(
            allocator.stats().bytes_allocated.load(Ordering::SeqCst),
            600
        );

        allocator.free(&alloc2).unwrap();
        assert_eq!(
            allocator.stats().bytes_allocated.load(Ordering::SeqCst),
            400
        );

        allocator.free(&alloc1).unwrap();
        allocator.free(&alloc3).unwrap();
        assert_eq!(
            allocator.stats().bytes_allocated.load(Ordering::SeqCst),
            0
        );
    }

    #[test]
    fn test_heap_allocator_peak_tracking() {
        let allocator = HeapAllocator::new(1024 * 1024);

        let alloc1 = allocator.allocate(100).unwrap();
        let alloc2 = allocator.allocate(200).unwrap();
        assert_eq!(allocator.stats().peak_bytes.load(Ordering::SeqCst), 300);

        allocator.free(&alloc1).unwrap();
        // Peak should still be 300
        assert_eq!(allocator.stats().peak_bytes.load(Ordering::SeqCst), 300);

        let alloc3 = allocator.allocate(400).unwrap();
        // New peak
        assert_eq!(allocator.stats().peak_bytes.load(Ordering::SeqCst), 600);

        allocator.free(&alloc2).unwrap();
        allocator.free(&alloc3).unwrap();
    }

    #[test]
    fn test_null_allocator() {
        let allocator = NullAllocator;

        assert_eq!(allocator.capacity(), 0);
        assert_eq!(allocator.available(), 0);

        let result = allocator.allocate(100);
        assert!(matches!(result, Err(PlasmaError::OutOfMemory)));
    }

    #[test]
    fn test_heap_allocator_fd_assignment() {
        let allocator = HeapAllocator::new(1024 * 1024);

        let alloc1 = allocator.allocate(100).unwrap();
        let alloc2 = allocator.allocate(100).unwrap();

        // FDs should be unique
        assert_ne!(alloc1.fd(), alloc2.fd());

        allocator.free(&alloc1).unwrap();
        allocator.free(&alloc2).unwrap();
    }

    // PlasmaAllocator tests (matching fallback_allocator_test.cc)

    #[test]
    fn test_plasma_allocator_creation() {
        let temp_dir = tempfile::tempdir().unwrap();
        let allocator = PlasmaAllocator::new(
            temp_dir.path().to_str().unwrap(),
            temp_dir.path().to_str().unwrap(),
            false,
            1024 * 1024,
        );

        assert_eq!(allocator.get_footprint_limit(), 1024 * 1024);
        assert_eq!(allocator.allocated(), 0);
        assert_eq!(allocator.fallback_allocated(), 0);
    }

    #[test]
    fn test_plasma_allocator_primary_allocation() {
        let temp_dir = tempfile::tempdir().unwrap();
        let allocator = PlasmaAllocator::new(
            temp_dir.path().to_str().unwrap(),
            temp_dir.path().to_str().unwrap(),
            false,
            1024 * 1024,
        );

        let alloc = allocator.allocate(1000).unwrap();
        assert_eq!(alloc.size(), 1000);
        assert!(!alloc.is_fallback_allocated());
        assert_eq!(allocator.allocated(), 1000);
        assert_eq!(allocator.fallback_allocated(), 0);

        allocator.free(alloc);
        assert_eq!(allocator.allocated(), 0);
    }

    #[test]
    fn test_plasma_allocator_fallback_allocation() {
        let temp_dir = tempfile::tempdir().unwrap();
        let allocator = PlasmaAllocator::new(
            temp_dir.path().to_str().unwrap(),
            temp_dir.path().to_str().unwrap(),
            false,
            1024 * 1024,
        );

        let alloc = allocator.fallback_allocate(1000).unwrap();
        assert_eq!(alloc.size(), 1000);
        assert!(alloc.is_fallback_allocated());
        assert_eq!(allocator.allocated(), 1000);
        assert_eq!(allocator.fallback_allocated(), 1000);

        allocator.free(alloc);
        assert_eq!(allocator.allocated(), 0);
        assert_eq!(allocator.fallback_allocated(), 0);
    }

    #[test]
    fn test_plasma_allocator_fallback_pass_through() {
        // This test matches FallbackPassThroughTest from fallback_allocator_test.cc
        let temp_dir = tempfile::tempdir().unwrap();
        const KB: usize = 1024;
        const MB: usize = 1024 * KB;

        // Limit is 256 * sizeof(size_t) + 2MB, but we use simpler 2MB for test
        let limit = 2 * MB;
        let object_size = 900 * KB;

        let allocator = PlasmaAllocator::new(
            temp_dir.path().to_str().unwrap(),
            temp_dir.path().to_str().unwrap(),
            false,
            limit,
        );

        assert_eq!(allocator.get_footprint_limit(), limit);

        // Allocate from primary
        let alloc1 = allocator.allocate(object_size).unwrap();
        assert!(!alloc1.is_fallback_allocated());

        let alloc2 = allocator.allocate(object_size).unwrap();
        assert!(!alloc2.is_fallback_allocated());

        assert_eq!(allocator.allocated(), 2 * object_size);

        // Free and reallocate
        allocator.free(alloc1);
        let alloc3 = allocator.allocate(object_size).unwrap();
        assert_eq!(allocator.fallback_allocated(), 0);
        assert_eq!(allocator.allocated(), 2 * object_size);

        allocator.free(alloc2);
        allocator.free(alloc3);
        assert_eq!(allocator.allocated(), 0);

        // Allocate until primary is full
        let primary_alloc1 = allocator.allocate(MB).unwrap();
        assert!(!primary_alloc1.is_fallback_allocated());

        // This should fail - over capacity
        let over_alloc = allocator.allocate(MB);
        assert!(over_alloc.is_none());
        assert_eq!(allocator.fallback_allocated(), 0);

        // Fallback allocation should succeed
        let fallback_alloc1 = allocator.fallback_allocate(MB).unwrap();
        assert!(fallback_alloc1.is_fallback_allocated());
        assert_eq!(allocator.allocated(), 2 * MB);
        assert_eq!(allocator.fallback_allocated(), MB);

        let fallback_alloc2 = allocator.fallback_allocate(MB).unwrap();
        assert!(fallback_alloc2.is_fallback_allocated());
        assert_eq!(allocator.allocated(), 3 * MB);
        assert_eq!(allocator.fallback_allocated(), 2 * MB);

        // Free fallback allocation
        allocator.free(fallback_alloc2);
        assert_eq!(allocator.allocated(), 2 * MB);
        assert_eq!(allocator.fallback_allocated(), MB);

        // Free primary allocation
        allocator.free(primary_alloc1);
        assert_eq!(allocator.allocated(), MB);
        assert_eq!(allocator.fallback_allocated(), MB);

        // Now we can allocate from primary again
        let new_primary = allocator.allocate(MB).unwrap();
        assert!(!new_primary.is_fallback_allocated());
        assert_eq!(allocator.allocated(), 2 * MB);
        assert_eq!(allocator.fallback_allocated(), MB);

        // Cleanup
        allocator.free(new_primary);
        allocator.free(fallback_alloc1);
        assert_eq!(allocator.allocated(), 0);
        assert_eq!(allocator.fallback_allocated(), 0);
    }

    #[test]
    fn test_plasma_allocator_write_read() {
        // Test that we can actually write to and read from the allocated memory
        let temp_dir = tempfile::tempdir().unwrap();
        let allocator = PlasmaAllocator::new(
            temp_dir.path().to_str().unwrap(),
            temp_dir.path().to_str().unwrap(),
            false,
            1024 * 1024,
        );

        let alloc = allocator.allocate(1000).unwrap();

        // Write some data
        unsafe {
            let slice = std::slice::from_raw_parts_mut(alloc.address(), alloc.size());
            for (i, byte) in slice.iter_mut().enumerate() {
                *byte = (i % 256) as u8;
            }
        }

        // Read it back
        unsafe {
            let slice = std::slice::from_raw_parts(alloc.address(), alloc.size());
            for (i, byte) in slice.iter().enumerate() {
                assert_eq!(*byte, (i % 256) as u8);
            }
        }

        allocator.free(alloc);
    }

    #[test]
    fn test_plasma_allocator_fallback_write_read() {
        // Test that we can write to and read from fallback allocated memory
        let temp_dir = tempfile::tempdir().unwrap();
        let allocator = PlasmaAllocator::new(
            temp_dir.path().to_str().unwrap(),
            temp_dir.path().to_str().unwrap(),
            false,
            1024 * 1024,
        );

        let alloc = allocator.fallback_allocate(1000).unwrap();

        // Write some data
        unsafe {
            let slice = std::slice::from_raw_parts_mut(alloc.address(), alloc.size());
            for (i, byte) in slice.iter_mut().enumerate() {
                *byte = (i % 256) as u8;
            }
        }

        // Read it back
        unsafe {
            let slice = std::slice::from_raw_parts(alloc.address(), alloc.size());
            for (i, byte) in slice.iter().enumerate() {
                assert_eq!(*byte, (i % 256) as u8);
            }
        }

        allocator.free(alloc);
    }
}
