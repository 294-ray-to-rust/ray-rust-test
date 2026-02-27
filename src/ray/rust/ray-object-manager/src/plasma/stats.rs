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

//! Object statistics collector for plasma store.

use super::common::{LocalObject, ObjectSource};
use std::sync::atomic::{AtomicI64, Ordering};

/// Collector for object store statistics.
///
/// Tracks various metrics about objects in the plasma store including
/// counts and byte sizes by state, source, and usage.
#[derive(Debug, Default)]
pub struct ObjectStatsCollector {
    // Total bytes created (cumulative)
    pub num_bytes_created_total: AtomicI64,

    // Objects by state
    pub num_objects_spillable: AtomicI64,
    pub num_bytes_spillable: AtomicI64,
    pub num_objects_unsealed: AtomicI64,
    pub num_bytes_unsealed: AtomicI64,
    pub num_objects_in_use: AtomicI64,
    pub num_bytes_in_use: AtomicI64,
    pub num_objects_evictable: AtomicI64,
    pub num_bytes_evictable: AtomicI64,

    // Objects by source
    pub num_objects_created_by_worker: AtomicI64,
    pub num_bytes_created_by_worker: AtomicI64,
    pub num_objects_restored: AtomicI64,
    pub num_bytes_restored: AtomicI64,
    pub num_objects_received: AtomicI64,
    pub num_bytes_received: AtomicI64,
    pub num_objects_errored: AtomicI64,
    pub num_bytes_errored: AtomicI64,

    // Fallback allocation tracking
    pub num_bytes_fallback_sealed: AtomicI64,
    pub num_bytes_fallback_unsealed: AtomicI64,
    pub num_bytes_primary_sealed: AtomicI64,
    pub num_bytes_primary_unsealed: AtomicI64,
}

impl ObjectStatsCollector {
    /// Create a new stats collector.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that an object was created.
    pub fn on_object_created(&self, object: &LocalObject) {
        let size = object.object_info().total_size() as i64;

        // Track total bytes created
        self.num_bytes_created_total.fetch_add(size, Ordering::SeqCst);

        // Object starts as unsealed
        self.num_objects_unsealed.fetch_add(1, Ordering::SeqCst);
        self.num_bytes_unsealed.fetch_add(size, Ordering::SeqCst);

        // Track by allocation type
        if object.is_fallback_allocated() {
            self.num_bytes_fallback_unsealed.fetch_add(size, Ordering::SeqCst);
        } else {
            self.num_bytes_primary_unsealed.fetch_add(size, Ordering::SeqCst);
        }

        // Track by source
        self.record_source_created(object.source(), size);
    }

    /// Record that an object was sealed.
    pub fn on_object_sealed(&self, object: &LocalObject) {
        let size = object.object_info().total_size() as i64;

        // Move from unsealed to sealed
        self.num_objects_unsealed.fetch_sub(1, Ordering::SeqCst);
        self.num_bytes_unsealed.fetch_sub(size, Ordering::SeqCst);

        // Track by allocation type (move from unsealed to sealed)
        if object.is_fallback_allocated() {
            self.num_bytes_fallback_unsealed.fetch_sub(size, Ordering::SeqCst);
            self.num_bytes_fallback_sealed.fetch_add(size, Ordering::SeqCst);
        } else {
            self.num_bytes_primary_unsealed.fetch_sub(size, Ordering::SeqCst);
            self.num_bytes_primary_sealed.fetch_add(size, Ordering::SeqCst);
        }

        // If ref_count is 0, it's evictable
        if object.ref_count() == 0 {
            self.num_objects_evictable.fetch_add(1, Ordering::SeqCst);
            self.num_bytes_evictable.fetch_add(size, Ordering::SeqCst);
        }

        // If ref_count is 1 and created by worker, it's spillable
        if object.ref_count() == 1 && object.source() == ObjectSource::CreatedByWorker {
            self.num_objects_spillable.fetch_add(1, Ordering::SeqCst);
            self.num_bytes_spillable.fetch_add(size, Ordering::SeqCst);
        }
    }

    /// Record that an object is being deleted.
    pub fn on_object_deleting(&self, object: &LocalObject) {
        let size = object.object_info().total_size() as i64;

        // Remove from appropriate category based on state
        if object.is_sealed() {
            // Remove from evictable if it was evictable
            if object.ref_count() == 0 {
                self.num_objects_evictable.fetch_sub(1, Ordering::SeqCst);
                self.num_bytes_evictable.fetch_sub(size, Ordering::SeqCst);
            }

            // Remove from spillable if it was spillable
            if object.ref_count() == 1 && object.source() == ObjectSource::CreatedByWorker {
                self.num_objects_spillable.fetch_sub(1, Ordering::SeqCst);
                self.num_bytes_spillable.fetch_sub(size, Ordering::SeqCst);
            }

            // Track by allocation type
            if object.is_fallback_allocated() {
                self.num_bytes_fallback_sealed.fetch_sub(size, Ordering::SeqCst);
            } else {
                self.num_bytes_primary_sealed.fetch_sub(size, Ordering::SeqCst);
            }
        } else {
            // Was unsealed
            self.num_objects_unsealed.fetch_sub(1, Ordering::SeqCst);
            self.num_bytes_unsealed.fetch_sub(size, Ordering::SeqCst);

            if object.is_fallback_allocated() {
                self.num_bytes_fallback_unsealed.fetch_sub(size, Ordering::SeqCst);
            } else {
                self.num_bytes_primary_unsealed.fetch_sub(size, Ordering::SeqCst);
            }
        }

        // Remove from in_use if it was in use
        if object.ref_count() > 0 {
            self.num_objects_in_use.fetch_sub(1, Ordering::SeqCst);
            self.num_bytes_in_use.fetch_sub(size, Ordering::SeqCst);
        }

        // Remove from source category
        self.record_source_deleted(object.source(), size);
    }

    /// Record a reference being added to an object.
    pub fn on_reference_added(&self, object: &LocalObject) {
        let size = object.object_info().total_size() as i64;
        let old_ref_count = object.ref_count() - 1; // Already incremented

        // First reference: add to in_use
        if old_ref_count == 0 {
            self.num_objects_in_use.fetch_add(1, Ordering::SeqCst);
            self.num_bytes_in_use.fetch_add(size, Ordering::SeqCst);

            // If sealed and was evictable, no longer evictable
            if object.is_sealed() {
                self.num_objects_evictable.fetch_sub(1, Ordering::SeqCst);
                self.num_bytes_evictable.fetch_sub(size, Ordering::SeqCst);
            }
        }

        // Second reference and was spillable: no longer spillable
        if old_ref_count == 1 && object.source() == ObjectSource::CreatedByWorker && object.is_sealed()
        {
            self.num_objects_spillable.fetch_sub(1, Ordering::SeqCst);
            self.num_bytes_spillable.fetch_sub(size, Ordering::SeqCst);
        }
    }

    /// Record a reference being removed from an object.
    pub fn on_reference_removed(&self, object: &LocalObject) {
        let size = object.object_info().total_size() as i64;
        let new_ref_count = object.ref_count(); // Already decremented

        // If ref count becomes 1 and created by worker and sealed: becomes spillable
        if new_ref_count == 1 && object.source() == ObjectSource::CreatedByWorker && object.is_sealed()
        {
            self.num_objects_spillable.fetch_add(1, Ordering::SeqCst);
            self.num_bytes_spillable.fetch_add(size, Ordering::SeqCst);
        }

        // If ref count becomes 0: remove from in_use, add to evictable
        if new_ref_count == 0 {
            self.num_objects_in_use.fetch_sub(1, Ordering::SeqCst);
            self.num_bytes_in_use.fetch_sub(size, Ordering::SeqCst);

            if object.is_sealed() {
                self.num_objects_evictable.fetch_add(1, Ordering::SeqCst);
                self.num_bytes_evictable.fetch_add(size, Ordering::SeqCst);
            }
        }
    }

    /// Get the current total bytes created.
    pub fn get_num_bytes_created_current(&self) -> i64 {
        self.num_bytes_fallback_sealed.load(Ordering::SeqCst)
            + self.num_bytes_fallback_unsealed.load(Ordering::SeqCst)
            + self.num_bytes_primary_sealed.load(Ordering::SeqCst)
            + self.num_bytes_primary_unsealed.load(Ordering::SeqCst)
    }

    fn record_source_created(&self, source: ObjectSource, size: i64) {
        match source {
            ObjectSource::CreatedByWorker | ObjectSource::CreatedByPlasmaFallbackAllocation => {
                self.num_objects_created_by_worker.fetch_add(1, Ordering::SeqCst);
                self.num_bytes_created_by_worker.fetch_add(size, Ordering::SeqCst);
            }
            ObjectSource::RestoredFromStorage => {
                self.num_objects_restored.fetch_add(1, Ordering::SeqCst);
                self.num_bytes_restored.fetch_add(size, Ordering::SeqCst);
            }
            ObjectSource::ReceivedFromRemoteRaylet => {
                self.num_objects_received.fetch_add(1, Ordering::SeqCst);
                self.num_bytes_received.fetch_add(size, Ordering::SeqCst);
            }
            ObjectSource::ErrorStoredByRaylet => {
                self.num_objects_errored.fetch_add(1, Ordering::SeqCst);
                self.num_bytes_errored.fetch_add(size, Ordering::SeqCst);
            }
        }
    }

    fn record_source_deleted(&self, source: ObjectSource, size: i64) {
        match source {
            ObjectSource::CreatedByWorker | ObjectSource::CreatedByPlasmaFallbackAllocation => {
                self.num_objects_created_by_worker.fetch_sub(1, Ordering::SeqCst);
                self.num_bytes_created_by_worker.fetch_sub(size, Ordering::SeqCst);
            }
            ObjectSource::RestoredFromStorage => {
                self.num_objects_restored.fetch_sub(1, Ordering::SeqCst);
                self.num_bytes_restored.fetch_sub(size, Ordering::SeqCst);
            }
            ObjectSource::ReceivedFromRemoteRaylet => {
                self.num_objects_received.fetch_sub(1, Ordering::SeqCst);
                self.num_bytes_received.fetch_sub(size, Ordering::SeqCst);
            }
            ObjectSource::ErrorStoredByRaylet => {
                self.num_objects_errored.fetch_sub(1, Ordering::SeqCst);
                self.num_bytes_errored.fetch_sub(size, Ordering::SeqCst);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plasma::{Allocation, ObjectInfo};
    use ray_common::ObjectId;

    fn make_object(source: ObjectSource, fallback: bool) -> LocalObject {
        let allocation = Allocation::new(
            std::ptr::null_mut(),
            100,
            -1,
            0,
            0,
            100,
            fallback,
        );
        let info = ObjectInfo::new(ObjectId::from_random(), 100, 0, vec![]);
        LocalObject::new(allocation, info, source)
    }

    #[test]
    fn test_stats_collector_creation() {
        let collector = ObjectStatsCollector::new();
        assert_eq!(collector.num_bytes_created_total.load(Ordering::SeqCst), 0);
        assert_eq!(collector.num_objects_unsealed.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn test_on_object_created() {
        let collector = ObjectStatsCollector::new();
        let object = make_object(ObjectSource::CreatedByWorker, false);

        collector.on_object_created(&object);

        assert_eq!(collector.num_bytes_created_total.load(Ordering::SeqCst), 100);
        assert_eq!(collector.num_objects_unsealed.load(Ordering::SeqCst), 1);
        assert_eq!(collector.num_bytes_unsealed.load(Ordering::SeqCst), 100);
        assert_eq!(collector.num_objects_created_by_worker.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_on_object_sealed() {
        let collector = ObjectStatsCollector::new();
        let mut object = make_object(ObjectSource::CreatedByWorker, false);

        collector.on_object_created(&object);
        object.seal().unwrap();
        collector.on_object_sealed(&object);

        assert_eq!(collector.num_objects_unsealed.load(Ordering::SeqCst), 0);
        assert_eq!(collector.num_bytes_unsealed.load(Ordering::SeqCst), 0);
        assert_eq!(collector.num_objects_evictable.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_stats_by_source() {
        let collector = ObjectStatsCollector::new();

        let obj1 = make_object(ObjectSource::CreatedByWorker, false);
        let obj2 = make_object(ObjectSource::RestoredFromStorage, false);
        let obj3 = make_object(ObjectSource::ReceivedFromRemoteRaylet, false);
        let obj4 = make_object(ObjectSource::ErrorStoredByRaylet, false);

        collector.on_object_created(&obj1);
        collector.on_object_created(&obj2);
        collector.on_object_created(&obj3);
        collector.on_object_created(&obj4);

        assert_eq!(collector.num_objects_created_by_worker.load(Ordering::SeqCst), 1);
        assert_eq!(collector.num_objects_restored.load(Ordering::SeqCst), 1);
        assert_eq!(collector.num_objects_received.load(Ordering::SeqCst), 1);
        assert_eq!(collector.num_objects_errored.load(Ordering::SeqCst), 1);
    }
}
