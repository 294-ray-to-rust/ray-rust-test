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

//! Object lifecycle manager for plasma store.

use dashmap::DashMap;
use parking_lot::RwLock;
use ray_common::ObjectId;
use std::collections::HashSet;
use std::sync::Arc;

use super::allocator::Allocator;
use super::common::{
    Allocation, LocalObject, ObjectInfo, ObjectSource, ObjectState, PlasmaError, PlasmaResult,
};
use super::eviction::EvictionPolicy;
use super::stats::ObjectStatsCollector;

/// Result of a create operation.
pub enum CreateResult {
    /// Object was created successfully.
    Ok,
    /// Object already exists.
    ObjectExists,
    /// Out of memory.
    OutOfMemory,
}

/// The object lifecycle manager handles object creation, sealing, and deletion.
pub struct ObjectLifecycleManager<A: Allocator, E: EvictionPolicy> {
    /// The allocator for object memory.
    allocator: Arc<A>,
    /// The eviction policy.
    eviction_policy: Arc<RwLock<E>>,
    /// The stats collector.
    stats_collector: ObjectStatsCollector,
    /// Objects in the store.
    objects: DashMap<ObjectId, LocalObject>,
    /// Objects marked for eager deletion.
    eager_deletion_objects: RwLock<HashSet<ObjectId>>,
    /// Callback for when an object is deleted.
    on_delete_callback: Option<Box<dyn Fn(&ObjectId) + Send + Sync>>,
}

impl<A: Allocator, E: EvictionPolicy> ObjectLifecycleManager<A, E> {
    /// Create a new lifecycle manager.
    pub fn new(allocator: Arc<A>, eviction_policy: Arc<RwLock<E>>) -> Self {
        Self {
            allocator,
            eviction_policy,
            stats_collector: ObjectStatsCollector::new(),
            objects: DashMap::new(),
            eager_deletion_objects: RwLock::new(HashSet::new()),
            on_delete_callback: None,
        }
    }

    /// Set the callback for when an object is deleted.
    pub fn set_on_delete_callback<F>(&mut self, callback: F)
    where
        F: Fn(&ObjectId) + Send + Sync + 'static,
    {
        self.on_delete_callback = Some(Box::new(callback));
    }

    /// Create a new object.
    pub fn create_object(
        &self,
        object_info: ObjectInfo,
        source: ObjectSource,
        fallback_allocator: bool,
    ) -> PlasmaResult<()> {
        let object_id = object_info.object_id.clone();

        // Check if object already exists
        if self.objects.contains_key(&object_id) {
            return Err(PlasmaError::ObjectExists(object_id));
        }

        let size = object_info.total_size();

        // Try to allocate memory
        let allocation = if fallback_allocator {
            // Try with eviction first
            self.try_allocate_with_eviction(size)?
        } else {
            self.allocator.allocate(size)?
        };

        // Create the local object
        let local_object = LocalObject::new(allocation, object_info, source);

        // Record stats
        self.stats_collector.on_object_created(&local_object);

        // Notify eviction policy
        self.eviction_policy.write().object_created(&object_id);

        // Insert into the store
        self.objects.insert(object_id, local_object);

        Ok(())
    }

    /// Try to allocate with eviction if needed.
    fn try_allocate_with_eviction(&self, size: usize) -> PlasmaResult<Allocation> {
        // First try direct allocation
        if let Ok(allocation) = self.allocator.allocate(size) {
            return Ok(allocation);
        }

        // Try eviction up to 10 times
        for _ in 0..10 {
            // Get objects to evict
            let mut objects_to_evict = Vec::new();
            let bytes_required = size as i64 - self.allocator.available() as i64;
            if bytes_required > 0 {
                self.eviction_policy
                    .read()
                    .choose_objects_to_evict(bytes_required, &mut objects_to_evict);
            }

            // Evict the objects
            for id in objects_to_evict {
                let _ = self.delete_object_internal(&id, false);
            }

            // Try allocation again
            if let Ok(allocation) = self.allocator.allocate(size) {
                return Ok(allocation);
            }
        }

        Err(PlasmaError::OutOfMemory)
    }

    /// Get an object by ID.
    pub fn get_object(&self, object_id: &ObjectId) -> Option<ObjectState> {
        self.objects.get(object_id).map(|obj| obj.state())
    }

    /// Check if an object exists.
    pub fn contains(&self, object_id: &ObjectId) -> bool {
        self.objects.contains_key(object_id)
    }

    /// Seal an object.
    pub fn seal_object(&self, object_id: &ObjectId) -> PlasmaResult<()> {
        let mut entry = self
            .objects
            .get_mut(object_id)
            .ok_or_else(|| PlasmaError::ObjectNotFound(object_id.clone()))?;

        entry.seal()?;
        self.stats_collector.on_object_sealed(&entry);

        Ok(())
    }

    /// Abort object creation.
    pub fn abort_object(&self, object_id: &ObjectId) -> PlasmaResult<()> {
        // Check if object exists and is not sealed
        let entry = self
            .objects
            .get(object_id)
            .ok_or_else(|| PlasmaError::ObjectNotFound(object_id.clone()))?;

        if entry.is_sealed() {
            return Err(PlasmaError::ObjectAlreadySealed(object_id.clone()));
        }

        drop(entry);

        // Delete without notification
        self.delete_object_internal(object_id, false)
    }

    /// Delete an object.
    pub fn delete_object(&self, object_id: &ObjectId) -> PlasmaResult<()> {
        let entry = self
            .objects
            .get(object_id)
            .ok_or_else(|| PlasmaError::ObjectNotFound(object_id.clone()))?;

        // Can't delete unsealed objects through this method
        if !entry.is_sealed() {
            self.eager_deletion_objects
                .write()
                .insert(object_id.clone());
            return Err(PlasmaError::ObjectNotSealed(object_id.clone()));
        }

        // Can't delete objects in use
        if entry.ref_count() > 0 {
            self.eager_deletion_objects
                .write()
                .insert(object_id.clone());
            return Err(PlasmaError::InvalidRequest(
                "Object in use".to_string(),
            ));
        }

        drop(entry);

        self.delete_object_internal(object_id, true)
    }

    /// Internal delete implementation.
    fn delete_object_internal(&self, object_id: &ObjectId, notify: bool) -> PlasmaResult<()> {
        let (_, object) = self
            .objects
            .remove(object_id)
            .ok_or_else(|| PlasmaError::ObjectNotFound(object_id.clone()))?;

        // Record stats
        self.stats_collector.on_object_deleting(&object);

        // Notify eviction policy
        self.eviction_policy.write().remove_object(object_id);

        // Free memory
        let _ = self.allocator.free(object.allocation());

        // Remove from eager deletion set
        self.eager_deletion_objects.write().remove(object_id);

        // Call delete callback
        if notify {
            if let Some(ref callback) = self.on_delete_callback {
                callback(object_id);
            }
        }

        Ok(())
    }

    /// Add a reference to an object.
    pub fn add_reference(&self, object_id: &ObjectId) -> bool {
        if let Some(entry) = self.objects.get(object_id) {
            entry.add_ref();
            self.stats_collector.on_reference_added(&entry);
            self.eviction_policy.write().begin_object_access(object_id);
            true
        } else {
            false
        }
    }

    /// Remove a reference from an object.
    pub fn remove_reference(&self, object_id: &ObjectId) -> bool {
        if let Some(entry) = self.objects.get(object_id) {
            if entry.ref_count() == 0 {
                return false;
            }

            entry.remove_ref();
            self.stats_collector.on_reference_removed(&entry);

            let should_delete = entry.ref_count() == 0
                && self.eager_deletion_objects.read().contains(object_id);

            // End object access if ref count is now 0
            if entry.ref_count() == 0 {
                self.eviction_policy.write().end_object_access(object_id);
            }

            drop(entry);

            // Delete if it was marked for eager deletion
            if should_delete {
                let _ = self.delete_object_internal(object_id, true);
            }

            true
        } else {
            false
        }
    }

    /// Evict objects to free up space.
    pub fn evict_objects(&self, object_ids: &[ObjectId]) {
        for id in object_ids {
            if let Some(entry) = self.objects.get(id) {
                // Only evict sealed objects with no references
                if entry.is_sealed() && entry.ref_count() == 0 {
                    drop(entry);
                    let _ = self.delete_object_internal(id, true);
                }
            }
        }
    }

    /// Get the stats collector.
    pub fn stats_collector(&self) -> &ObjectStatsCollector {
        &self.stats_collector
    }

    /// Get the number of objects.
    pub fn len(&self) -> usize {
        self.objects.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.objects.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plasma::allocator::HeapAllocator;
    use crate::plasma::eviction::LRUCache;

    fn make_manager() -> ObjectLifecycleManager<HeapAllocator, LRUCache> {
        let allocator = Arc::new(HeapAllocator::new(1024 * 1024));
        let eviction = Arc::new(RwLock::new(LRUCache::new(
            "test".to_string(),
            1024 * 1024,
        )));
        ObjectLifecycleManager::new(allocator, eviction)
    }

    #[test]
    fn test_create_object() {
        let manager = make_manager();
        let object_id = ObjectId::from_random();
        let info = ObjectInfo::new(object_id.clone(), 100, 0, vec![]);

        let result = manager.create_object(info, ObjectSource::CreatedByWorker, false);
        assert!(result.is_ok());
        assert!(manager.contains(&object_id));
    }

    #[test]
    fn test_create_duplicate() {
        let manager = make_manager();
        let object_id = ObjectId::from_random();
        let info = ObjectInfo::new(object_id.clone(), 100, 0, vec![]);

        manager
            .create_object(info.clone(), ObjectSource::CreatedByWorker, false)
            .unwrap();
        let result = manager.create_object(info, ObjectSource::CreatedByWorker, false);

        assert!(matches!(result, Err(PlasmaError::ObjectExists(_))));
    }

    #[test]
    fn test_seal_object() {
        let manager = make_manager();
        let object_id = ObjectId::from_random();
        let info = ObjectInfo::new(object_id.clone(), 100, 0, vec![]);

        manager
            .create_object(info, ObjectSource::CreatedByWorker, false)
            .unwrap();

        assert_eq!(
            manager.get_object(&object_id),
            Some(ObjectState::PlasmaCreated)
        );

        manager.seal_object(&object_id).unwrap();

        assert_eq!(
            manager.get_object(&object_id),
            Some(ObjectState::PlasmaSealed)
        );
    }

    #[test]
    fn test_delete_object() {
        let manager = make_manager();
        let object_id = ObjectId::from_random();
        let info = ObjectInfo::new(object_id.clone(), 100, 0, vec![]);

        manager
            .create_object(info, ObjectSource::CreatedByWorker, false)
            .unwrap();
        manager.seal_object(&object_id).unwrap();
        manager.delete_object(&object_id).unwrap();

        assert!(!manager.contains(&object_id));
    }

    #[test]
    fn test_abort_object() {
        let manager = make_manager();
        let object_id = ObjectId::from_random();
        let info = ObjectInfo::new(object_id.clone(), 100, 0, vec![]);

        manager
            .create_object(info, ObjectSource::CreatedByWorker, false)
            .unwrap();
        manager.abort_object(&object_id).unwrap();

        assert!(!manager.contains(&object_id));
    }

    #[test]
    fn test_reference_counting() {
        let manager = make_manager();
        let object_id = ObjectId::from_random();
        let info = ObjectInfo::new(object_id.clone(), 100, 0, vec![]);

        manager
            .create_object(info, ObjectSource::CreatedByWorker, false)
            .unwrap();
        manager.seal_object(&object_id).unwrap();

        // Add reference
        assert!(manager.add_reference(&object_id));

        // Try to delete with reference - marks for eager deletion but returns error
        let result = manager.delete_object(&object_id);
        assert!(result.is_err());

        // Remove reference - triggers eager deletion automatically
        assert!(manager.remove_reference(&object_id));

        // Object should now be deleted (eager deletion happened)
        assert!(!manager.contains(&object_id));
    }
}
