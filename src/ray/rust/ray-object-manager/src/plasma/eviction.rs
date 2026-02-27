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

//! LRU cache and eviction policy implementation.

use ray_common::ObjectId;
use std::collections::{HashMap, VecDeque};

/// LRU (Least Recently Used) cache for tracking object eviction order.
///
/// Objects are tracked by their size and evicted in LRU order when space is needed.
pub struct LRUCache {
    /// Name of this cache (for debugging).
    name: String,
    /// Original (maximum) capacity in bytes.
    original_capacity: i64,
    /// Current capacity (can be adjusted).
    capacity: i64,
    /// Currently used capacity in bytes.
    used_capacity: i64,
    /// Number of evictions performed.
    num_evictions_total: i64,
    /// Total bytes evicted.
    bytes_evicted_total: i64,
    /// LRU list: front = oldest (first to evict), back = newest.
    lru_list: VecDeque<(ObjectId, i64)>,
    /// Map from object ID to index in lru_list (for O(1) lookup).
    item_map: HashMap<ObjectId, usize>,
}

impl LRUCache {
    /// Create a new LRU cache with the given capacity.
    pub fn new(name: String, capacity: i64) -> Self {
        Self {
            name,
            original_capacity: capacity,
            capacity,
            used_capacity: 0,
            num_evictions_total: 0,
            bytes_evicted_total: 0,
            lru_list: VecDeque::new(),
            item_map: HashMap::new(),
        }
    }

    /// Add an object to the cache.
    ///
    /// The object is added as the most recently used.
    pub fn add(&mut self, key: ObjectId, size: i64) {
        if self.item_map.contains_key(&key) {
            // Already exists - remove and re-add to update position
            self.remove(&key);
        }

        self.lru_list.push_back((key.clone(), size));
        self.item_map.insert(key, self.lru_list.len() - 1);
        self.used_capacity += size;
    }

    /// Remove an object from the cache.
    ///
    /// Returns the size of the removed object, or 0 if not found.
    pub fn remove(&mut self, key: &ObjectId) -> i64 {
        if let Some(&index) = self.item_map.get(key) {
            if index < self.lru_list.len() {
                let (_, size) = self.lru_list.remove(index).unwrap();
                self.item_map.remove(key);
                self.used_capacity -= size;

                // Update indices for items after the removed one
                self.rebuild_index_map();

                return size;
            }
        }
        0
    }

    /// Rebuild the index map after a removal.
    fn rebuild_index_map(&mut self) {
        self.item_map.clear();
        for (i, (key, _)) in self.lru_list.iter().enumerate() {
            self.item_map.insert(key.clone(), i);
        }
    }

    /// Get the original (maximum) capacity.
    pub fn original_capacity(&self) -> i64 {
        self.original_capacity
    }

    /// Get the current capacity.
    pub fn capacity(&self) -> i64 {
        self.capacity
    }

    /// Get the remaining capacity.
    pub fn remaining_capacity(&self) -> i64 {
        self.capacity - self.used_capacity
    }

    /// Adjust the capacity by a delta.
    ///
    /// Positive delta increases capacity, negative decreases.
    pub fn adjust_capacity(&mut self, delta: i64) {
        self.capacity += delta;
    }

    /// Check if an object exists in the cache.
    pub fn exists(&self, key: &ObjectId) -> bool {
        self.item_map.contains_key(key)
    }

    /// Choose objects to evict to free at least `num_bytes_required` bytes.
    ///
    /// Returns the total bytes that would be freed by evicting the chosen objects.
    /// Objects are chosen in LRU order (oldest first).
    pub fn choose_objects_to_evict(
        &self,
        num_bytes_required: i64,
        objects_to_evict: &mut Vec<ObjectId>,
    ) -> i64 {
        let mut bytes_to_evict = 0i64;

        for (key, size) in &self.lru_list {
            objects_to_evict.push(key.clone());
            bytes_to_evict += size;

            if bytes_to_evict >= num_bytes_required {
                break;
            }
        }

        bytes_to_evict
    }

    /// Iterate over all objects in LRU order (oldest first).
    pub fn foreach<F>(&self, mut f: F)
    where
        F: FnMut(&ObjectId),
    {
        for (key, _) in &self.lru_list {
            f(key);
        }
    }

    /// Get all object IDs in LRU order.
    pub fn get_all_keys(&self) -> Vec<ObjectId> {
        self.lru_list.iter().map(|(k, _)| k.clone()).collect()
    }

    /// Get the number of objects in the cache.
    pub fn len(&self) -> usize {
        self.lru_list.len()
    }

    /// Check if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.lru_list.is_empty()
    }

    /// Get debug string representation.
    pub fn debug_string(&self) -> String {
        format!(
            "LRUCache(name={}, capacity={}, used={}, original={}, evictions={}, bytes_evicted={})",
            self.name,
            self.capacity,
            self.used_capacity,
            self.original_capacity,
            self.num_evictions_total,
            self.bytes_evicted_total
        )
    }

    /// Record that objects were evicted.
    pub fn record_eviction(&mut self, num_objects: i64, bytes: i64) {
        self.num_evictions_total += num_objects;
        self.bytes_evicted_total += bytes;
    }
}

/// Trait for accessing object information needed by the eviction policy.
pub trait ObjectStoreView {
    /// Get the size of an object (data + metadata).
    fn get_object_size(&self, object_id: &ObjectId) -> Option<i64>;
}

/// Trait for accessing allocator information.
pub trait AllocatorView {
    /// Get the memory footprint limit.
    fn footprint_limit(&self) -> i64;
    /// Get the currently allocated bytes.
    fn allocated(&self) -> i64;
}

/// LRU-based eviction policy that uses an LRU cache to track objects.
///
/// The policy maintains a cache of sealed objects that can be evicted.
/// Objects being accessed are temporarily removed from the cache.
pub struct LRUEvictionPolicy<S: ObjectStoreView, A: AllocatorView> {
    /// LRU cache for sealed objects.
    cache: LRUCache,
    /// Reference to the object store.
    object_store: S,
    /// Reference to the allocator.
    allocator: A,
    /// Bytes pinned by active accesses.
    pinned_memory_bytes: i64,
}

impl<S: ObjectStoreView, A: AllocatorView> LRUEvictionPolicy<S, A> {
    /// Create a new eviction policy.
    pub fn new(object_store: S, allocator: A) -> Self {
        let capacity = allocator.footprint_limit();
        Self {
            cache: LRUCache::new("eviction_cache".to_string(), capacity),
            object_store,
            allocator,
            pinned_memory_bytes: 0,
        }
    }

    /// Notify the policy that an object was created.
    pub fn object_created(&mut self, object_id: &ObjectId) {
        if let Some(size) = self.object_store.get_object_size(object_id) {
            self.cache.add(object_id.clone(), size);
        }
    }

    /// Request space for a new allocation.
    ///
    /// Returns the number of additional bytes still needed (negative if enough space was freed).
    pub fn require_space(&mut self, size: i64, objects_to_evict: &mut Vec<ObjectId>) -> i64 {
        let allocated = self.allocator.allocated();
        let footprint_limit = self.allocator.footprint_limit();

        // Calculate how much we need to free
        // We want: allocated - evicted + size <= footprint_limit
        // So: evicted >= allocated + size - footprint_limit
        let bytes_over_limit = allocated + size - footprint_limit;

        if bytes_over_limit <= 0 {
            // Already have enough space
            return bytes_over_limit;
        }

        // We need to evict at least 20% of capacity to avoid frequent evictions
        let min_eviction = footprint_limit / 5;
        let bytes_to_free = bytes_over_limit.max(min_eviction);

        let bytes_evicted = self.cache.choose_objects_to_evict(bytes_to_free, objects_to_evict);

        // Remove evicted objects from cache
        for obj in objects_to_evict.iter() {
            self.cache.remove(obj);
        }

        // Return how much more space is still needed
        bytes_over_limit - bytes_evicted
    }

    /// Notify the policy that an object is being accessed.
    ///
    /// The object is removed from the eviction cache while in use.
    pub fn begin_object_access(&mut self, object_id: &ObjectId) {
        let size = self.cache.remove(object_id);
        self.pinned_memory_bytes += size;
    }

    /// Notify the policy that object access has ended.
    ///
    /// The object is added back to the eviction cache.
    pub fn end_object_access(&mut self, object_id: &ObjectId) {
        if let Some(size) = self.object_store.get_object_size(object_id) {
            self.pinned_memory_bytes -= size;
            self.cache.add(object_id.clone(), size);
        }
    }

    /// Choose objects to evict.
    pub fn choose_objects_to_evict(
        &self,
        num_bytes_required: i64,
        objects_to_evict: &mut Vec<ObjectId>,
    ) -> i64 {
        self.cache
            .choose_objects_to_evict(num_bytes_required, objects_to_evict)
    }

    /// Remove an object from the policy.
    pub fn remove_object(&mut self, object_id: &ObjectId) {
        self.cache.remove(object_id);
    }

    /// Check if an object exists in the cache (i.e., is evictable).
    pub fn is_object_exists(&self, object_id: &ObjectId) -> bool {
        self.cache.exists(object_id)
    }

    /// Get debug string.
    pub fn debug_string(&self) -> String {
        format!(
            "EvictionPolicy(pinned={}, cache={})",
            self.pinned_memory_bytes,
            self.cache.debug_string()
        )
    }
}

/// Trait for eviction policy implementations.
///
/// This trait allows the lifecycle manager to interact with different eviction
/// policies without knowing their implementation details.
pub trait EvictionPolicy: Send + Sync {
    /// Notify the policy that an object was created.
    fn object_created(&mut self, object_id: &ObjectId);

    /// Notify the policy that an object is being accessed.
    fn begin_object_access(&mut self, object_id: &ObjectId);

    /// Notify the policy that object access has ended.
    fn end_object_access(&mut self, object_id: &ObjectId);

    /// Choose objects to evict.
    fn choose_objects_to_evict(
        &self,
        num_bytes_required: i64,
        objects_to_evict: &mut Vec<ObjectId>,
    ) -> i64;

    /// Remove an object from the policy.
    fn remove_object(&mut self, object_id: &ObjectId);
}

// Implement EvictionPolicy for LRUCache
impl EvictionPolicy for LRUCache {
    fn object_created(&mut self, object_id: &ObjectId) {
        // For LRUCache, we need size information
        // Default to adding with size 0 (will be updated when properly tracked)
        if !self.exists(object_id) {
            // Don't add here - will be added when sealed with proper size
        }
    }

    fn begin_object_access(&mut self, object_id: &ObjectId) {
        self.remove(object_id);
    }

    fn end_object_access(&mut self, object_id: &ObjectId) {
        // Object will be re-added by the lifecycle manager with correct size
        // For now, add with size 0 as placeholder
        if !self.exists(object_id) {
            self.add(object_id.clone(), 0);
        }
    }

    fn choose_objects_to_evict(
        &self,
        num_bytes_required: i64,
        objects_to_evict: &mut Vec<ObjectId>,
    ) -> i64 {
        LRUCache::choose_objects_to_evict(self, num_bytes_required, objects_to_evict)
    }

    fn remove_object(&mut self, object_id: &ObjectId) {
        self.remove(object_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lru_cache_basic() {
        let mut cache = LRUCache::new("test".to_string(), 1024);

        assert_eq!(cache.capacity(), 1024);
        assert_eq!(cache.original_capacity(), 1024);
        assert_eq!(cache.remaining_capacity(), 1024);

        let key1 = ObjectId::from_random();
        cache.add(key1.clone(), 32);

        assert_eq!(cache.remaining_capacity(), 1024 - 32);
        assert!(cache.exists(&key1));

        let key2 = ObjectId::from_random();
        cache.add(key2.clone(), 64);

        assert_eq!(cache.remaining_capacity(), 1024 - 32 - 64);

        cache.remove(&key1);
        assert_eq!(cache.remaining_capacity(), 1024 - 64);
        assert!(!cache.exists(&key1));

        cache.remove(&key2);
        assert_eq!(cache.remaining_capacity(), 1024);
    }

    #[test]
    fn test_lru_cache_choose_to_evict() {
        let mut cache = LRUCache::new("test".to_string(), 1024);

        let key1 = ObjectId::from_random();
        let key2 = ObjectId::from_random();

        cache.add(key1.clone(), 10);
        cache.add(key2.clone(), 10);

        let mut to_evict = Vec::new();
        let bytes = cache.choose_objects_to_evict(15, &mut to_evict);

        assert_eq!(bytes, 20); // Both objects
        assert_eq!(to_evict.len(), 2);

        let mut to_evict2 = Vec::new();
        let bytes2 = cache.choose_objects_to_evict(30, &mut to_evict2);

        assert_eq!(bytes2, 20); // Can only evict what we have
        assert_eq!(to_evict2.len(), 2);
    }

    #[test]
    fn test_lru_cache_foreach() {
        let mut cache = LRUCache::new("test".to_string(), 1024);

        let key1 = ObjectId::from_random();
        let key2 = ObjectId::from_random();

        cache.add(key1.clone(), 10);
        cache.add(key2.clone(), 10);

        let mut keys = Vec::new();
        cache.foreach(|k| keys.push(k.clone()));

        // LRU order: key1 first (oldest), then key2
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0], key1);
        assert_eq!(keys[1], key2);
    }

    #[test]
    fn test_lru_cache_adjust_capacity() {
        let mut cache = LRUCache::new("test".to_string(), 1024);

        cache.adjust_capacity(1024);
        assert_eq!(cache.capacity(), 2048);
        assert_eq!(cache.original_capacity(), 1024);
    }

    // Mock implementations for testing EvictionPolicy
    struct MockObjectStore {
        sizes: HashMap<ObjectId, i64>,
    }

    impl ObjectStoreView for MockObjectStore {
        fn get_object_size(&self, object_id: &ObjectId) -> Option<i64> {
            self.sizes.get(object_id).copied()
        }
    }

    struct MockAllocator {
        limit: i64,
        allocated: i64,
    }

    impl AllocatorView for MockAllocator {
        fn footprint_limit(&self) -> i64 {
            self.limit
        }
        fn allocated(&self) -> i64 {
            self.allocated
        }
    }

    #[test]
    fn test_eviction_policy_basic() {
        let key1 = ObjectId::from_random();
        let key2 = ObjectId::from_random();

        let mut sizes = HashMap::new();
        sizes.insert(key1.clone(), 10);
        sizes.insert(key2.clone(), 20);

        let store = MockObjectStore { sizes };
        let allocator = MockAllocator {
            limit: 100,
            allocated: 30,
        };

        let mut policy = LRUEvictionPolicy::new(store, allocator);

        policy.object_created(&key1);
        policy.object_created(&key2);

        assert!(policy.is_object_exists(&key1));
        assert!(policy.is_object_exists(&key2));

        // Begin access removes from cache
        policy.begin_object_access(&key1);
        assert!(!policy.is_object_exists(&key1));

        // End access adds back
        policy.end_object_access(&key1);
        assert!(policy.is_object_exists(&key1));
    }

    #[test]
    fn test_eviction_policy_require_space() {
        let key1 = ObjectId::from_random();
        let key2 = ObjectId::from_random();
        let key3 = ObjectId::from_random();
        let key4 = ObjectId::from_random();

        let mut sizes = HashMap::new();
        sizes.insert(key1.clone(), 10);
        sizes.insert(key2.clone(), 20);
        sizes.insert(key3.clone(), 30);
        sizes.insert(key4.clone(), 40);

        let store = MockObjectStore { sizes };
        let allocator = MockAllocator {
            limit: 100,
            allocated: 100, // Already full
        };

        let mut policy = LRUEvictionPolicy::new(store, allocator);

        policy.object_created(&key1);
        policy.object_created(&key2);
        policy.object_created(&key3);
        policy.object_created(&key4);

        let mut to_evict = Vec::new();
        // Need 10 bytes, but min eviction is 20% = 20 bytes
        // Should evict first two objects (10 + 20 = 30)
        let remaining = policy.require_space(10, &mut to_evict);

        assert_eq!(to_evict.len(), 2);
        assert!(remaining <= 0); // We freed enough
    }
}
