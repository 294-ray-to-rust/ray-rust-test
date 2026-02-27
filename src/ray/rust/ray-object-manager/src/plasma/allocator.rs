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
}
