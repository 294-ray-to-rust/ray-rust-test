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

//! Common types for the Plasma object store.

use ray_common::ObjectId;
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use thiserror::Error;

/// Object state in the plasma store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ObjectState {
    /// Object is being created (writable).
    PlasmaCreated = 1,
    /// Object has been sealed (immutable, readable).
    PlasmaSealed = 2,
}

/// Source of how an object was created.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(u8)]
pub enum ObjectSource {
    /// Default/unknown source.
    #[default]
    CreatedByWorker = 0,
    /// Restored from spilled storage.
    RestoredFromStorage = 1,
    /// Received from another node.
    ReceivedFromRemoteRaylet = 2,
    /// Error during creation.
    ErrorStoredByRaylet = 3,
    /// Created via Plasma fallback.
    CreatedByPlasmaFallbackAllocation = 4,
}

/// Error types for Plasma operations.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum PlasmaError {
    /// Object already exists in the store.
    #[error("Object already exists: {0}")]
    ObjectExists(ObjectId),

    /// Object not found in the store.
    #[error("Object not found: {0}")]
    ObjectNotFound(ObjectId),

    /// Object is already sealed.
    #[error("Object already sealed: {0}")]
    ObjectAlreadySealed(ObjectId),

    /// Out of memory.
    #[error("Out of memory")]
    OutOfMemory,

    /// Transient out of memory (may succeed later).
    #[error("Transient out of memory")]
    TransientOutOfMemory,

    /// Out of disk space.
    #[error("Out of disk space")]
    OutOfDisk,

    /// Object is not sealed yet.
    #[error("Object not sealed: {0}")]
    ObjectNotSealed(ObjectId),

    /// Invalid request.
    #[error("Invalid request: {0}")]
    InvalidRequest(String),

    /// I/O error.
    #[error("I/O error: {0}")]
    IoError(String),

    /// Unexpected error.
    #[error("Unexpected error: {0}")]
    Unexpected(String),
}

impl From<std::io::Error> for PlasmaError {
    fn from(err: std::io::Error) -> Self {
        PlasmaError::IoError(err.to_string())
    }
}

/// Result type for Plasma operations.
pub type PlasmaResult<T> = Result<T, PlasmaError>;

/// Represents a memory allocation in the plasma store.
#[derive(Debug)]
pub struct Allocation {
    /// Pointer to the allocated memory.
    address: *mut u8,
    /// Size of the allocation in bytes.
    size: usize,
    /// File descriptor for memory-mapped file (-1 if none).
    fd: i32,
    /// Offset within the memory-mapped region.
    offset: usize,
    /// Device number (0 for CPU).
    device_num: i32,
    /// Total size of the memory-mapped region.
    mmap_size: usize,
    /// Whether this is a fallback allocation (filesystem-backed).
    fallback_allocated: bool,
}

impl Allocation {
    /// Create a new allocation.
    pub fn new(
        address: *mut u8,
        size: usize,
        fd: i32,
        offset: usize,
        device_num: i32,
        mmap_size: usize,
        fallback_allocated: bool,
    ) -> Self {
        Self {
            address,
            size,
            fd,
            offset,
            device_num,
            mmap_size,
            fallback_allocated,
        }
    }

    /// Get the address of the allocation.
    pub fn address(&self) -> *mut u8 {
        self.address
    }

    /// Get the size of the allocation.
    pub fn size(&self) -> usize {
        self.size
    }

    /// Get the file descriptor.
    pub fn fd(&self) -> i32 {
        self.fd
    }

    /// Get the offset within the memory-mapped region.
    pub fn offset(&self) -> usize {
        self.offset
    }

    /// Get the device number.
    pub fn device_num(&self) -> i32 {
        self.device_num
    }

    /// Get the total mmap size.
    pub fn mmap_size(&self) -> usize {
        self.mmap_size
    }

    /// Check if this is a fallback allocation.
    pub fn is_fallback_allocated(&self) -> bool {
        self.fallback_allocated
    }

    /// Get a slice to the allocated data.
    ///
    /// # Safety
    /// The caller must ensure the allocation is valid and properly initialized.
    pub unsafe fn as_slice(&self) -> &[u8] {
        std::slice::from_raw_parts(self.address, self.size)
    }

    /// Get a mutable slice to the allocated data.
    ///
    /// # Safety
    /// The caller must ensure the allocation is valid and properly initialized.
    pub unsafe fn as_mut_slice(&mut self) -> &mut [u8] {
        std::slice::from_raw_parts_mut(self.address, self.size)
    }
}

// Note: Allocation intentionally does not implement Clone - it represents unique ownership

// Safety: Allocation can be sent between threads as long as only one thread accesses it
unsafe impl Send for Allocation {}

// Safety: Allocation can be shared between threads - the memory it points to is treated
// as immutable once sealed, and mutable access requires proper synchronization
unsafe impl Sync for Allocation {}

/// Information about an object.
#[derive(Debug, Clone)]
pub struct ObjectInfo {
    /// The object ID.
    pub object_id: ObjectId,
    /// Size of the data section.
    pub data_size: usize,
    /// Size of the metadata section.
    pub metadata_size: usize,
    /// Owner's address (serialized).
    pub owner_address: Vec<u8>,
}

impl ObjectInfo {
    /// Create a new ObjectInfo.
    pub fn new(
        object_id: ObjectId,
        data_size: usize,
        metadata_size: usize,
        owner_address: Vec<u8>,
    ) -> Self {
        Self {
            object_id,
            data_size,
            metadata_size,
            owner_address,
        }
    }

    /// Total size of the object (data + metadata).
    pub fn total_size(&self) -> usize {
        self.data_size + self.metadata_size
    }
}

/// A local object in the plasma store.
pub struct LocalObject {
    /// The underlying memory allocation.
    allocation: Allocation,
    /// Object metadata.
    object_info: ObjectInfo,
    /// Reference count.
    ref_count: AtomicI32,
    /// Current state of the object.
    state: ObjectState,
    /// Source of the object.
    source: ObjectSource,
    /// Time when the object was created.
    create_time: Instant,
    /// Duration to construct the object (seal time - create time).
    construct_duration: Option<Duration>,
}

impl LocalObject {
    /// Create a new local object.
    pub fn new(
        allocation: Allocation,
        object_info: ObjectInfo,
        source: ObjectSource,
    ) -> Self {
        Self {
            allocation,
            object_info,
            ref_count: AtomicI32::new(0),
            state: ObjectState::PlasmaCreated,
            source,
            create_time: Instant::now(),
            construct_duration: None,
        }
    }

    /// Get the object ID.
    pub fn object_id(&self) -> &ObjectId {
        &self.object_info.object_id
    }

    /// Get the object info.
    pub fn object_info(&self) -> &ObjectInfo {
        &self.object_info
    }

    /// Get the data size.
    pub fn data_size(&self) -> usize {
        self.object_info.data_size
    }

    /// Get the metadata size.
    pub fn metadata_size(&self) -> usize {
        self.object_info.metadata_size
    }

    /// Get the total allocated size.
    pub fn allocated_size(&self) -> usize {
        self.allocation.size()
    }

    /// Get the current state.
    pub fn state(&self) -> ObjectState {
        self.state
    }

    /// Check if the object is sealed.
    pub fn is_sealed(&self) -> bool {
        self.state == ObjectState::PlasmaSealed
    }

    /// Get the object source.
    pub fn source(&self) -> ObjectSource {
        self.source
    }

    /// Get the current reference count.
    pub fn ref_count(&self) -> i32 {
        self.ref_count.load(Ordering::SeqCst)
    }

    /// Increment the reference count.
    pub fn add_ref(&self) -> i32 {
        self.ref_count.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Decrement the reference count.
    pub fn remove_ref(&self) -> i32 {
        self.ref_count.fetch_sub(1, Ordering::SeqCst) - 1
    }

    /// Seal the object.
    pub fn seal(&mut self) -> PlasmaResult<()> {
        if self.state == ObjectState::PlasmaSealed {
            return Err(PlasmaError::ObjectAlreadySealed(
                self.object_info.object_id.clone(),
            ));
        }
        self.state = ObjectState::PlasmaSealed;
        self.construct_duration = Some(self.create_time.elapsed());
        Ok(())
    }

    /// Get the allocation.
    pub fn allocation(&self) -> &Allocation {
        &self.allocation
    }

    /// Get the device number.
    pub fn device_num(&self) -> i32 {
        self.allocation.device_num()
    }

    /// Check if this is a fallback allocation.
    pub fn is_fallback_allocated(&self) -> bool {
        self.allocation.is_fallback_allocated()
    }

    /// Get a pointer to the data section.
    ///
    /// # Safety
    /// Caller must ensure proper synchronization.
    pub unsafe fn data_ptr(&self) -> *mut u8 {
        self.allocation.address()
    }

    /// Get a pointer to the metadata section.
    ///
    /// # Safety
    /// Caller must ensure proper synchronization.
    pub unsafe fn metadata_ptr(&self) -> *mut u8 {
        self.allocation.address().add(self.object_info.data_size)
    }
}

/// Header for mutable plasma objects (experimental).
#[repr(C)]
pub struct PlasmaObjectHeader {
    /// Object version number.
    pub version: AtomicU64,
    /// Whether the object is sealed.
    pub is_sealed: AtomicU64,
    /// Whether there was an error.
    pub has_error: AtomicU64,
    /// Number of active readers.
    pub num_readers: AtomicU64,
    /// Number of read acquires remaining.
    pub num_read_acquires_remaining: AtomicU64,
    /// Number of read releases remaining.
    pub num_read_releases_remaining: AtomicU64,
    /// Data size.
    pub data_size: AtomicU64,
    /// Metadata size.
    pub metadata_size: AtomicU64,
}

impl PlasmaObjectHeader {
    /// Create a new header with default values.
    pub fn new() -> Self {
        Self {
            version: AtomicU64::new(0),
            is_sealed: AtomicU64::new(0),
            has_error: AtomicU64::new(0),
            num_readers: AtomicU64::new(0),
            num_read_acquires_remaining: AtomicU64::new(0),
            num_read_releases_remaining: AtomicU64::new(0),
            data_size: AtomicU64::new(0),
            metadata_size: AtomicU64::new(0),
        }
    }

    /// Check if the object is sealed.
    pub fn is_sealed(&self) -> bool {
        self.is_sealed.load(Ordering::SeqCst) != 0
    }

    /// Get the current version.
    pub fn version(&self) -> u64 {
        self.version.load(Ordering::SeqCst)
    }
}

impl Default for PlasmaObjectHeader {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_state() {
        assert_eq!(ObjectState::PlasmaCreated as u8, 1);
        assert_eq!(ObjectState::PlasmaSealed as u8, 2);
    }

    #[test]
    fn test_object_source() {
        assert_eq!(ObjectSource::CreatedByWorker as u8, 0);
        assert_eq!(ObjectSource::RestoredFromStorage as u8, 1);
    }

    #[test]
    fn test_plasma_error() {
        let obj_id = ObjectId::from_random();
        let err = PlasmaError::ObjectExists(obj_id.clone());
        assert!(err.to_string().contains("already exists"));

        let err = PlasmaError::OutOfMemory;
        assert_eq!(err.to_string(), "Out of memory");
    }

    #[test]
    fn test_object_info() {
        let obj_id = ObjectId::from_random();
        let info = ObjectInfo::new(obj_id.clone(), 1024, 64, vec![1, 2, 3]);

        assert_eq!(info.data_size, 1024);
        assert_eq!(info.metadata_size, 64);
        assert_eq!(info.total_size(), 1088);
        assert_eq!(info.owner_address, vec![1, 2, 3]);
    }

    #[test]
    fn test_plasma_object_header() {
        let header = PlasmaObjectHeader::new();
        assert!(!header.is_sealed());
        assert_eq!(header.version(), 0);

        header.is_sealed.store(1, Ordering::SeqCst);
        assert!(header.is_sealed());

        header.version.store(42, Ordering::SeqCst);
        assert_eq!(header.version(), 42);
    }
}
