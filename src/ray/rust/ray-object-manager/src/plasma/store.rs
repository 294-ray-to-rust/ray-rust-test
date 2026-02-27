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

//! Plasma object store implementation.

use dashmap::DashMap;
use parking_lot::RwLock;
use ray_common::ObjectId;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;

use super::common::{
    Allocation, LocalObject, ObjectInfo, ObjectSource, ObjectState, PlasmaError, PlasmaResult,
};

/// Statistics for the object store.
#[derive(Debug, Default)]
pub struct ObjectStoreStats {
    /// Number of objects currently in the store.
    pub num_objects: AtomicUsize,
    /// Number of bytes used by objects.
    pub num_bytes_used: AtomicI64,
    /// Number of bytes created total.
    pub num_bytes_created_total: AtomicI64,
    /// Number of objects created total.
    pub num_objects_created_total: AtomicUsize,
    /// Number of bytes sealed.
    pub num_bytes_sealed: AtomicI64,
    /// Number of objects sealed.
    pub num_objects_sealed: AtomicUsize,
    /// Number of fallback allocations.
    pub num_fallback_allocations: AtomicUsize,
    /// Number of bytes in fallback allocations.
    pub num_bytes_fallback: AtomicI64,
}

impl ObjectStoreStats {
    /// Create new stats.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the number of objects.
    pub fn num_objects(&self) -> usize {
        self.num_objects.load(Ordering::SeqCst)
    }

    /// Get bytes used.
    pub fn bytes_used(&self) -> i64 {
        self.num_bytes_used.load(Ordering::SeqCst)
    }

    /// Record object creation.
    pub fn record_create(&self, size: usize, fallback: bool) {
        self.num_objects.fetch_add(1, Ordering::SeqCst);
        self.num_bytes_used.fetch_add(size as i64, Ordering::SeqCst);
        self.num_bytes_created_total
            .fetch_add(size as i64, Ordering::SeqCst);
        self.num_objects_created_total.fetch_add(1, Ordering::SeqCst);
        if fallback {
            self.num_fallback_allocations.fetch_add(1, Ordering::SeqCst);
            self.num_bytes_fallback
                .fetch_add(size as i64, Ordering::SeqCst);
        }
    }

    /// Record object seal.
    pub fn record_seal(&self, size: usize) {
        self.num_objects_sealed.fetch_add(1, Ordering::SeqCst);
        self.num_bytes_sealed.fetch_add(size as i64, Ordering::SeqCst);
    }

    /// Record object deletion.
    pub fn record_delete(&self, size: usize, fallback: bool) {
        self.num_objects.fetch_sub(1, Ordering::SeqCst);
        self.num_bytes_used.fetch_sub(size as i64, Ordering::SeqCst);
        if fallback {
            self.num_fallback_allocations.fetch_sub(1, Ordering::SeqCst);
            self.num_bytes_fallback
                .fetch_sub(size as i64, Ordering::SeqCst);
        }
    }
}

/// An entry in the object store.
struct ObjectEntry {
    /// The local object.
    object: LocalObject,
}

/// Configuration for the object store.
#[derive(Debug, Clone)]
pub struct ObjectStoreConfig {
    /// Maximum memory capacity in bytes.
    pub capacity: usize,
    /// Directory for fallback allocations.
    pub fallback_directory: Option<String>,
    /// Whether to enable fallback allocations.
    pub enable_fallback: bool,
    /// Minimum object size for fallback.
    pub min_fallback_size: usize,
}

impl Default for ObjectStoreConfig {
    fn default() -> Self {
        Self {
            capacity: 1024 * 1024 * 1024, // 1GB
            fallback_directory: None,
            enable_fallback: false,
            min_fallback_size: 1024 * 1024, // 1MB
        }
    }
}

/// The plasma object store.
pub struct ObjectStore {
    /// Object storage (concurrent hash map).
    objects: DashMap<ObjectId, ObjectEntry>,
    /// Objects in eviction order (LRU).
    eviction_queue: RwLock<VecDeque<ObjectId>>,
    /// Store statistics.
    stats: ObjectStoreStats,
    /// Store configuration.
    config: ObjectStoreConfig,
}

impl ObjectStore {
    /// Create a new object store.
    pub fn new(config: ObjectStoreConfig) -> Self {
        Self {
            objects: DashMap::new(),
            eviction_queue: RwLock::new(VecDeque::new()),
            stats: ObjectStoreStats::new(),
            config,
        }
    }

    /// Create with default configuration.
    pub fn with_capacity(capacity: usize) -> Self {
        Self::new(ObjectStoreConfig {
            capacity,
            ..Default::default()
        })
    }

    /// Get the store configuration.
    pub fn config(&self) -> &ObjectStoreConfig {
        &self.config
    }

    /// Get the store statistics.
    pub fn stats(&self) -> &ObjectStoreStats {
        &self.stats
    }

    /// Check if an object exists.
    pub fn contains(&self, object_id: &ObjectId) -> bool {
        self.objects.contains_key(object_id)
    }

    /// Get the number of objects.
    pub fn len(&self) -> usize {
        self.objects.len()
    }

    /// Check if the store is empty.
    pub fn is_empty(&self) -> bool {
        self.objects.is_empty()
    }

    /// Get available capacity.
    pub fn available_capacity(&self) -> usize {
        let used = self.stats.bytes_used() as usize;
        self.config.capacity.saturating_sub(used)
    }

    /// Create a new object in the store.
    ///
    /// This allocates memory and returns a mutable reference to write data.
    pub fn create_object(
        &self,
        object_id: ObjectId,
        data_size: usize,
        metadata_size: usize,
        source: ObjectSource,
        owner_address: Vec<u8>,
    ) -> PlasmaResult<()> {
        // Check if object already exists
        if self.objects.contains_key(&object_id) {
            return Err(PlasmaError::ObjectExists(object_id));
        }

        let total_size = data_size + metadata_size;

        // Check capacity
        if total_size > self.available_capacity() {
            return Err(PlasmaError::OutOfMemory);
        }

        // Allocate memory (in a real implementation, this would use mmap)
        let layout = std::alloc::Layout::from_size_align(total_size.max(1), 8)
            .map_err(|e| PlasmaError::InvalidRequest(e.to_string()))?;

        let address = unsafe { std::alloc::alloc_zeroed(layout) };
        if address.is_null() {
            return Err(PlasmaError::OutOfMemory);
        }

        let allocation = Allocation::new(
            address,
            total_size,
            -1, // No fd for heap allocation
            0,
            0, // CPU
            total_size,
            false,
        );

        let object_info = ObjectInfo::new(object_id.clone(), data_size, metadata_size, owner_address);
        let local_object = LocalObject::new(allocation, object_info, source);

        // Insert into store
        self.objects.insert(
            object_id,
            ObjectEntry {
                object: local_object,
            },
        );

        self.stats.record_create(total_size, false);

        Ok(())
    }

    /// Seal an object (make it immutable).
    pub fn seal_object(&self, object_id: &ObjectId) -> PlasmaResult<()> {
        let mut entry = self
            .objects
            .get_mut(object_id)
            .ok_or_else(|| PlasmaError::ObjectNotFound(object_id.clone()))?;

        let size = entry.object.allocated_size();
        entry.object.seal()?;

        // Add to eviction queue
        self.eviction_queue.write().push_back(object_id.clone());

        self.stats.record_seal(size);

        Ok(())
    }

    /// Get an object reference (increment ref count).
    pub fn get_object(&self, object_id: &ObjectId) -> PlasmaResult<ObjectRef> {
        let entry = self
            .objects
            .get(object_id)
            .ok_or_else(|| PlasmaError::ObjectNotFound(object_id.clone()))?;

        if !entry.object.is_sealed() {
            return Err(PlasmaError::ObjectNotSealed(object_id.clone()));
        }

        entry.object.add_ref();

        Ok(ObjectRef {
            object_id: object_id.clone(),
            data_size: entry.object.data_size(),
            metadata_size: entry.object.metadata_size(),
        })
    }

    /// Release an object reference.
    pub fn release_object(&self, object_id: &ObjectId) -> PlasmaResult<()> {
        let entry = self
            .objects
            .get(object_id)
            .ok_or_else(|| PlasmaError::ObjectNotFound(object_id.clone()))?;

        entry.object.remove_ref();
        Ok(())
    }

    /// Delete an object from the store.
    pub fn delete_object(&self, object_id: &ObjectId) -> PlasmaResult<()> {
        let (_, entry) = self
            .objects
            .remove(object_id)
            .ok_or_else(|| PlasmaError::ObjectNotFound(object_id.clone()))?;

        // Check if still referenced
        if entry.object.ref_count() > 0 {
            // Put it back - can't delete referenced object
            self.objects.insert(object_id.clone(), entry);
            return Err(PlasmaError::InvalidRequest(
                "Cannot delete object with active references".to_string(),
            ));
        }

        let size = entry.object.allocated_size();
        let fallback = entry.object.is_fallback_allocated();

        // Free the memory
        let allocation = entry.object.allocation();
        if allocation.size() > 0 {
            unsafe {
                let layout = std::alloc::Layout::from_size_align_unchecked(allocation.size(), 8);
                std::alloc::dealloc(allocation.address(), layout);
            }
        }

        // Remove from eviction queue
        self.eviction_queue
            .write()
            .retain(|id| id != object_id);

        self.stats.record_delete(size, fallback);

        Ok(())
    }

    /// Abort object creation (delete unsealed object).
    pub fn abort_object(&self, object_id: &ObjectId) -> PlasmaResult<()> {
        let entry = self
            .objects
            .get(object_id)
            .ok_or_else(|| PlasmaError::ObjectNotFound(object_id.clone()))?;

        if entry.object.is_sealed() {
            return Err(PlasmaError::ObjectAlreadySealed(object_id.clone()));
        }

        drop(entry);
        self.delete_object(object_id)
    }

    /// Get object state.
    pub fn get_object_state(&self, object_id: &ObjectId) -> Option<ObjectState> {
        self.objects.get(object_id).map(|e| e.object.state())
    }

    /// Check if object is sealed.
    pub fn is_sealed(&self, object_id: &ObjectId) -> bool {
        self.objects
            .get(object_id)
            .map(|e| e.object.is_sealed())
            .unwrap_or(false)
    }

    /// Get all object IDs.
    pub fn object_ids(&self) -> Vec<ObjectId> {
        self.objects.iter().map(|e| e.key().clone()).collect()
    }

    /// Get objects ready for eviction (sealed, no references).
    pub fn evictable_objects(&self) -> Vec<ObjectId> {
        self.eviction_queue
            .read()
            .iter()
            .filter(|id| {
                self.objects
                    .get(*id)
                    .map(|e| e.object.is_sealed() && e.object.ref_count() == 0)
                    .unwrap_or(false)
            })
            .cloned()
            .collect()
    }

    /// Evict objects to free up space.
    pub fn evict(&self, bytes_needed: usize) -> PlasmaResult<usize> {
        let mut bytes_freed = 0;
        let evictable = self.evictable_objects();

        for object_id in evictable {
            if bytes_freed >= bytes_needed {
                break;
            }

            if let Some(entry) = self.objects.get(&object_id) {
                let size = entry.object.allocated_size();
                drop(entry);

                if self.delete_object(&object_id).is_ok() {
                    bytes_freed += size;
                }
            }
        }

        Ok(bytes_freed)
    }
}

/// A reference to an object in the store.
#[derive(Debug, Clone)]
pub struct ObjectRef {
    /// The object ID.
    pub object_id: ObjectId,
    /// Size of the data section.
    pub data_size: usize,
    /// Size of the metadata section.
    pub metadata_size: usize,
}

impl ObjectRef {
    /// Total size of the object.
    pub fn total_size(&self) -> usize {
        self.data_size + self.metadata_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_store_config_default() {
        let config = ObjectStoreConfig::default();
        assert_eq!(config.capacity, 1024 * 1024 * 1024);
        assert!(!config.enable_fallback);
    }

    #[test]
    fn test_object_store_creation() {
        let store = ObjectStore::with_capacity(1024 * 1024);
        assert!(store.is_empty());
        assert_eq!(store.len(), 0);
        assert_eq!(store.available_capacity(), 1024 * 1024);
    }

    #[test]
    fn test_create_object() {
        let store = ObjectStore::with_capacity(1024 * 1024);
        let object_id = ObjectId::from_random();

        let result = store.create_object(
            object_id.clone(),
            100,
            20,
            ObjectSource::CreatedByWorker,
            vec![],
        );
        assert!(result.is_ok());
        assert!(store.contains(&object_id));
        assert_eq!(store.len(), 1);
        assert_eq!(store.stats().num_objects(), 1);
    }

    #[test]
    fn test_create_duplicate_object() {
        let store = ObjectStore::with_capacity(1024 * 1024);
        let object_id = ObjectId::from_random();

        store
            .create_object(
                object_id.clone(),
                100,
                0,
                ObjectSource::CreatedByWorker,
                vec![],
            )
            .unwrap();

        let result = store.create_object(
            object_id.clone(),
            100,
            0,
            ObjectSource::CreatedByWorker,
            vec![],
        );

        assert!(matches!(result, Err(PlasmaError::ObjectExists(_))));
    }

    #[test]
    fn test_seal_object() {
        let store = ObjectStore::with_capacity(1024 * 1024);
        let object_id = ObjectId::from_random();

        store
            .create_object(
                object_id.clone(),
                100,
                0,
                ObjectSource::CreatedByWorker,
                vec![],
            )
            .unwrap();

        assert!(!store.is_sealed(&object_id));
        store.seal_object(&object_id).unwrap();
        assert!(store.is_sealed(&object_id));
    }

    #[test]
    fn test_get_object() {
        let store = ObjectStore::with_capacity(1024 * 1024);
        let object_id = ObjectId::from_random();

        store
            .create_object(
                object_id.clone(),
                100,
                20,
                ObjectSource::CreatedByWorker,
                vec![],
            )
            .unwrap();

        // Can't get unsealed object
        let result = store.get_object(&object_id);
        assert!(matches!(result, Err(PlasmaError::ObjectNotSealed(_))));

        store.seal_object(&object_id).unwrap();

        let obj_ref = store.get_object(&object_id).unwrap();
        assert_eq!(obj_ref.data_size, 100);
        assert_eq!(obj_ref.metadata_size, 20);
        assert_eq!(obj_ref.total_size(), 120);
    }

    #[test]
    fn test_release_object() {
        let store = ObjectStore::with_capacity(1024 * 1024);
        let object_id = ObjectId::from_random();

        store
            .create_object(
                object_id.clone(),
                100,
                0,
                ObjectSource::CreatedByWorker,
                vec![],
            )
            .unwrap();
        store.seal_object(&object_id).unwrap();

        // Get increases ref count
        let _obj_ref = store.get_object(&object_id).unwrap();

        // Release decreases ref count
        store.release_object(&object_id).unwrap();
    }

    #[test]
    fn test_delete_object() {
        let store = ObjectStore::with_capacity(1024 * 1024);
        let object_id = ObjectId::from_random();

        store
            .create_object(
                object_id.clone(),
                100,
                0,
                ObjectSource::CreatedByWorker,
                vec![],
            )
            .unwrap();
        store.seal_object(&object_id).unwrap();

        assert!(store.contains(&object_id));
        store.delete_object(&object_id).unwrap();
        assert!(!store.contains(&object_id));
    }

    #[test]
    fn test_cannot_delete_referenced_object() {
        let store = ObjectStore::with_capacity(1024 * 1024);
        let object_id = ObjectId::from_random();

        store
            .create_object(
                object_id.clone(),
                100,
                0,
                ObjectSource::CreatedByWorker,
                vec![],
            )
            .unwrap();
        store.seal_object(&object_id).unwrap();

        // Get object (increments ref count)
        let _obj_ref = store.get_object(&object_id).unwrap();

        // Try to delete - should fail
        let result = store.delete_object(&object_id);
        assert!(matches!(result, Err(PlasmaError::InvalidRequest(_))));

        // Release and then delete
        store.release_object(&object_id).unwrap();
        store.delete_object(&object_id).unwrap();
        assert!(!store.contains(&object_id));
    }

    #[test]
    fn test_abort_object() {
        let store = ObjectStore::with_capacity(1024 * 1024);
        let object_id = ObjectId::from_random();

        store
            .create_object(
                object_id.clone(),
                100,
                0,
                ObjectSource::CreatedByWorker,
                vec![],
            )
            .unwrap();

        // Abort unsealed object
        store.abort_object(&object_id).unwrap();
        assert!(!store.contains(&object_id));
    }

    #[test]
    fn test_cannot_abort_sealed_object() {
        let store = ObjectStore::with_capacity(1024 * 1024);
        let object_id = ObjectId::from_random();

        store
            .create_object(
                object_id.clone(),
                100,
                0,
                ObjectSource::CreatedByWorker,
                vec![],
            )
            .unwrap();
        store.seal_object(&object_id).unwrap();

        // Can't abort sealed object
        let result = store.abort_object(&object_id);
        assert!(matches!(result, Err(PlasmaError::ObjectAlreadySealed(_))));
    }

    #[test]
    fn test_out_of_memory() {
        let store = ObjectStore::with_capacity(100);
        let object_id = ObjectId::from_random();

        let result = store.create_object(
            object_id,
            200,
            0,
            ObjectSource::CreatedByWorker,
            vec![],
        );

        assert!(matches!(result, Err(PlasmaError::OutOfMemory)));
    }

    #[test]
    fn test_evictable_objects() {
        let store = ObjectStore::with_capacity(1024 * 1024);
        let object_id1 = ObjectId::from_random();
        let object_id2 = ObjectId::from_random();

        // Create and seal two objects
        store
            .create_object(
                object_id1.clone(),
                100,
                0,
                ObjectSource::CreatedByWorker,
                vec![],
            )
            .unwrap();
        store.seal_object(&object_id1).unwrap();

        store
            .create_object(
                object_id2.clone(),
                100,
                0,
                ObjectSource::CreatedByWorker,
                vec![],
            )
            .unwrap();
        store.seal_object(&object_id2).unwrap();

        // Both should be evictable
        let evictable = store.evictable_objects();
        assert_eq!(evictable.len(), 2);

        // Hold reference to one
        let _ref1 = store.get_object(&object_id1).unwrap();

        // Only second should be evictable
        let evictable = store.evictable_objects();
        assert_eq!(evictable.len(), 1);
        assert_eq!(evictable[0], object_id2);
    }

    #[test]
    fn test_evict() {
        let store = ObjectStore::with_capacity(1024 * 1024);
        let object_id1 = ObjectId::from_random();
        let object_id2 = ObjectId::from_random();

        store
            .create_object(
                object_id1.clone(),
                100,
                0,
                ObjectSource::CreatedByWorker,
                vec![],
            )
            .unwrap();
        store.seal_object(&object_id1).unwrap();

        store
            .create_object(
                object_id2.clone(),
                200,
                0,
                ObjectSource::CreatedByWorker,
                vec![],
            )
            .unwrap();
        store.seal_object(&object_id2).unwrap();

        assert_eq!(store.len(), 2);

        // Evict 150 bytes - should evict first object
        let freed = store.evict(150).unwrap();
        assert!(freed >= 100);

        // At least one object should be evicted
        assert!(store.len() < 2);
    }

    #[test]
    fn test_object_ids() {
        let store = ObjectStore::with_capacity(1024 * 1024);
        let object_id1 = ObjectId::from_random();
        let object_id2 = ObjectId::from_random();

        store
            .create_object(
                object_id1.clone(),
                100,
                0,
                ObjectSource::CreatedByWorker,
                vec![],
            )
            .unwrap();
        store
            .create_object(
                object_id2.clone(),
                100,
                0,
                ObjectSource::CreatedByWorker,
                vec![],
            )
            .unwrap();

        let ids = store.object_ids();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&object_id1));
        assert!(ids.contains(&object_id2));
    }

    #[test]
    fn test_stats_tracking() {
        let store = ObjectStore::with_capacity(1024 * 1024);
        let object_id = ObjectId::from_random();

        assert_eq!(store.stats().num_objects(), 0);
        assert_eq!(store.stats().bytes_used(), 0);

        store
            .create_object(
                object_id.clone(),
                100,
                0,
                ObjectSource::CreatedByWorker,
                vec![],
            )
            .unwrap();

        assert_eq!(store.stats().num_objects(), 1);
        assert_eq!(store.stats().bytes_used(), 100);

        store.seal_object(&object_id).unwrap();

        assert_eq!(
            store.stats().num_objects_sealed.load(Ordering::SeqCst),
            1
        );

        store.delete_object(&object_id).unwrap();

        assert_eq!(store.stats().num_objects(), 0);
        assert_eq!(store.stats().bytes_used(), 0);
    }
}
