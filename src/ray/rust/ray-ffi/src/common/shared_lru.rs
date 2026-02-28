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

//! SharedLruCache FFI bridge.
//!
//! This implements a thread-safe LRU cache that matches Ray's
//! ThreadSafeSharedLruCache interface.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Node in a doubly-linked list for LRU ordering.
struct LruNode {
    key: String,
    value: Arc<String>,
    prev: Option<usize>,
    next: Option<usize>,
}

/// LRU cache implementation.
/// Uses a hashmap for O(1) lookup and a doubly-linked list for LRU ordering.
struct LruCacheInner {
    /// Maximum number of entries (0 means unlimited)
    max_entries: usize,
    /// Storage for nodes (using index-based linked list)
    nodes: Vec<Option<LruNode>>,
    /// Map from key to node index
    key_to_index: HashMap<String, usize>,
    /// Index of the head (most recently used)
    head: Option<usize>,
    /// Index of the tail (least recently used)
    tail: Option<usize>,
    /// Free list for reusing slots
    free_slots: Vec<usize>,
}

impl LruCacheInner {
    fn new(max_entries: usize) -> Self {
        Self {
            max_entries,
            nodes: Vec::new(),
            key_to_index: HashMap::new(),
            head: None,
            tail: None,
            free_slots: Vec::new(),
        }
    }

    fn move_to_front(&mut self, idx: usize) {
        if self.head == Some(idx) {
            return;
        }

        // Remove from current position
        if let Some(node) = &self.nodes[idx] {
            let prev = node.prev;
            let next = node.next;

            if let Some(prev_idx) = prev {
                if let Some(prev_node) = &mut self.nodes[prev_idx] {
                    prev_node.next = next;
                }
            }
            if let Some(next_idx) = next {
                if let Some(next_node) = &mut self.nodes[next_idx] {
                    next_node.prev = prev;
                }
            }

            if self.tail == Some(idx) {
                self.tail = prev;
            }
        }

        // Insert at front
        let old_head = self.head;
        if let Some(node) = &mut self.nodes[idx] {
            node.prev = None;
            node.next = old_head;
        }
        if let Some(old_head_idx) = old_head {
            if let Some(old_head_node) = &mut self.nodes[old_head_idx] {
                old_head_node.prev = Some(idx);
            }
        }
        self.head = Some(idx);

        if self.tail.is_none() {
            self.tail = Some(idx);
        }
    }

    fn allocate_slot(&mut self) -> usize {
        if let Some(idx) = self.free_slots.pop() {
            idx
        } else {
            let idx = self.nodes.len();
            self.nodes.push(None);
            idx
        }
    }

    fn put(&mut self, key: String, value: Arc<String>) {
        // Check if key already exists
        if let Some(&idx) = self.key_to_index.get(&key) {
            // Update value and move to front
            if let Some(node) = &mut self.nodes[idx] {
                node.value = value;
            }
            self.move_to_front(idx);
            return;
        }

        // Evict if needed
        if self.max_entries > 0 && self.key_to_index.len() >= self.max_entries {
            if let Some(tail_idx) = self.tail {
                if let Some(tail_node) = self.nodes[tail_idx].take() {
                    self.key_to_index.remove(&tail_node.key);
                    self.free_slots.push(tail_idx);

                    self.tail = tail_node.prev;
                    if let Some(new_tail_idx) = self.tail {
                        if let Some(new_tail_node) = &mut self.nodes[new_tail_idx] {
                            new_tail_node.next = None;
                        }
                    }
                    if self.tail.is_none() {
                        self.head = None;
                    }
                }
            }
        }

        // Insert new node
        let idx = self.allocate_slot();
        let old_head = self.head;

        let node = LruNode {
            key: key.clone(),
            value,
            prev: None,
            next: old_head,
        };
        self.nodes[idx] = Some(node);
        self.key_to_index.insert(key, idx);

        if let Some(old_head_idx) = old_head {
            if let Some(old_head_node) = &mut self.nodes[old_head_idx] {
                old_head_node.prev = Some(idx);
            }
        }
        self.head = Some(idx);

        if self.tail.is_none() {
            self.tail = Some(idx);
        }
    }

    fn get(&mut self, key: &str) -> Option<Arc<String>> {
        if let Some(&idx) = self.key_to_index.get(key) {
            self.move_to_front(idx);
            self.nodes[idx].as_ref().map(|n| Arc::clone(&n.value))
        } else {
            None
        }
    }

    fn delete(&mut self, key: &str) -> bool {
        if let Some(idx) = self.key_to_index.remove(key) {
            if let Some(node) = self.nodes[idx].take() {
                let prev = node.prev;
                let next = node.next;

                if let Some(prev_idx) = prev {
                    if let Some(prev_node) = &mut self.nodes[prev_idx] {
                        prev_node.next = next;
                    }
                }
                if let Some(next_idx) = next {
                    if let Some(next_node) = &mut self.nodes[next_idx] {
                        next_node.prev = prev;
                    }
                }

                if self.head == Some(idx) {
                    self.head = next;
                }
                if self.tail == Some(idx) {
                    self.tail = prev;
                }

                self.free_slots.push(idx);
                return true;
            }
        }
        false
    }

    fn clear(&mut self) {
        self.nodes.clear();
        self.key_to_index.clear();
        self.head = None;
        self.tail = None;
        self.free_slots.clear();
    }

    fn len(&self) -> usize {
        self.key_to_index.len()
    }
}

/// Thread-safe shared LRU cache for string key/value pairs.
pub struct RustSharedLruCache {
    inner: Mutex<LruCacheInner>,
    max_entries: usize,
}

impl RustSharedLruCache {
    pub fn new(max_entries: usize) -> Self {
        Self {
            inner: Mutex::new(LruCacheInner::new(max_entries)),
            max_entries,
        }
    }

    pub fn put(&self, key: String, value: String) {
        let mut inner = self.inner.lock().unwrap();
        inner.put(key, Arc::new(value));
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let mut inner = self.inner.lock().unwrap();
        inner.get(key).map(|v| (*v).clone())
    }

    pub fn delete(&self, key: &str) -> bool {
        let mut inner = self.inner.lock().unwrap();
        inner.delete(key)
    }

    pub fn clear(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.clear();
    }

    pub fn max_entries(&self) -> usize {
        self.max_entries
    }

    pub fn len(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Integer key LRU cache for testing with int keys.
pub struct RustIntLruCache {
    inner: Mutex<IntLruCacheInner>,
    max_entries: usize,
}

struct IntLruCacheInner {
    max_entries: usize,
    nodes: Vec<Option<IntLruNode>>,
    key_to_index: HashMap<i32, usize>,
    head: Option<usize>,
    tail: Option<usize>,
    free_slots: Vec<usize>,
}

struct IntLruNode {
    key: i32,
    value: i32,
    prev: Option<usize>,
    next: Option<usize>,
}

impl IntLruCacheInner {
    fn new(max_entries: usize) -> Self {
        Self {
            max_entries,
            nodes: Vec::new(),
            key_to_index: HashMap::new(),
            head: None,
            tail: None,
            free_slots: Vec::new(),
        }
    }

    fn move_to_front(&mut self, idx: usize) {
        if self.head == Some(idx) {
            return;
        }

        if let Some(node) = &self.nodes[idx] {
            let prev = node.prev;
            let next = node.next;

            if let Some(prev_idx) = prev {
                if let Some(prev_node) = &mut self.nodes[prev_idx] {
                    prev_node.next = next;
                }
            }
            if let Some(next_idx) = next {
                if let Some(next_node) = &mut self.nodes[next_idx] {
                    next_node.prev = prev;
                }
            }

            if self.tail == Some(idx) {
                self.tail = prev;
            }
        }

        let old_head = self.head;
        if let Some(node) = &mut self.nodes[idx] {
            node.prev = None;
            node.next = old_head;
        }
        if let Some(old_head_idx) = old_head {
            if let Some(old_head_node) = &mut self.nodes[old_head_idx] {
                old_head_node.prev = Some(idx);
            }
        }
        self.head = Some(idx);

        if self.tail.is_none() {
            self.tail = Some(idx);
        }
    }

    fn allocate_slot(&mut self) -> usize {
        if let Some(idx) = self.free_slots.pop() {
            idx
        } else {
            let idx = self.nodes.len();
            self.nodes.push(None);
            idx
        }
    }

    fn put(&mut self, key: i32, value: i32) {
        if let Some(&idx) = self.key_to_index.get(&key) {
            if let Some(node) = &mut self.nodes[idx] {
                node.value = value;
            }
            self.move_to_front(idx);
            return;
        }

        if self.max_entries > 0 && self.key_to_index.len() >= self.max_entries {
            if let Some(tail_idx) = self.tail {
                if let Some(tail_node) = self.nodes[tail_idx].take() {
                    self.key_to_index.remove(&tail_node.key);
                    self.free_slots.push(tail_idx);

                    self.tail = tail_node.prev;
                    if let Some(new_tail_idx) = self.tail {
                        if let Some(new_tail_node) = &mut self.nodes[new_tail_idx] {
                            new_tail_node.next = None;
                        }
                    }
                    if self.tail.is_none() {
                        self.head = None;
                    }
                }
            }
        }

        let idx = self.allocate_slot();
        let old_head = self.head;

        let node = IntLruNode {
            key,
            value,
            prev: None,
            next: old_head,
        };
        self.nodes[idx] = Some(node);
        self.key_to_index.insert(key, idx);

        if let Some(old_head_idx) = old_head {
            if let Some(old_head_node) = &mut self.nodes[old_head_idx] {
                old_head_node.prev = Some(idx);
            }
        }
        self.head = Some(idx);

        if self.tail.is_none() {
            self.tail = Some(idx);
        }
    }

    fn get(&mut self, key: i32) -> Option<i32> {
        if let Some(&idx) = self.key_to_index.get(&key) {
            self.move_to_front(idx);
            self.nodes[idx].as_ref().map(|n| n.value)
        } else {
            None
        }
    }
}

impl RustIntLruCache {
    pub fn new(max_entries: usize) -> Self {
        Self {
            inner: Mutex::new(IntLruCacheInner::new(max_entries)),
            max_entries,
        }
    }

    pub fn put(&self, key: i32, value: i32) {
        let mut inner = self.inner.lock().unwrap();
        inner.put(key, value);
    }

    pub fn get(&self, key: i32) -> Option<i32> {
        let mut inner = self.inner.lock().unwrap();
        inner.get(key)
    }

    pub fn max_entries(&self) -> usize {
        self.max_entries
    }
}

#[cxx::bridge(namespace = "ray::ffi")]
mod ffi {
    extern "Rust" {
        // String-keyed cache
        type RustSharedLruCache;

        fn shared_lru_cache_new(max_entries: usize) -> Box<RustSharedLruCache>;
        fn shared_lru_cache_put(cache: &RustSharedLruCache, key: &str, value: &str);
        fn shared_lru_cache_get(cache: &RustSharedLruCache, key: &str) -> String;
        fn shared_lru_cache_has(cache: &RustSharedLruCache, key: &str) -> bool;
        fn shared_lru_cache_delete(cache: &RustSharedLruCache, key: &str) -> bool;
        fn shared_lru_cache_clear(cache: &RustSharedLruCache);
        fn shared_lru_cache_max_entries(cache: &RustSharedLruCache) -> usize;
        fn shared_lru_cache_len(cache: &RustSharedLruCache) -> usize;

        // Int-keyed cache
        type RustIntLruCache;

        fn int_lru_cache_new(max_entries: usize) -> Box<RustIntLruCache>;
        fn int_lru_cache_put(cache: &RustIntLruCache, key: i32, value: i32);
        fn int_lru_cache_get(cache: &RustIntLruCache, key: i32) -> i32;
        fn int_lru_cache_has(cache: &RustIntLruCache, key: i32) -> bool;
        fn int_lru_cache_max_entries(cache: &RustIntLruCache) -> usize;
    }
}

fn shared_lru_cache_new(max_entries: usize) -> Box<RustSharedLruCache> {
    Box::new(RustSharedLruCache::new(max_entries))
}

fn shared_lru_cache_put(cache: &RustSharedLruCache, key: &str, value: &str) {
    cache.put(key.to_string(), value.to_string());
}

fn shared_lru_cache_get(cache: &RustSharedLruCache, key: &str) -> String {
    cache.get(key).unwrap_or_default()
}

fn shared_lru_cache_has(cache: &RustSharedLruCache, key: &str) -> bool {
    cache.get(key).is_some()
}

fn shared_lru_cache_delete(cache: &RustSharedLruCache, key: &str) -> bool {
    cache.delete(key)
}

fn shared_lru_cache_clear(cache: &RustSharedLruCache) {
    cache.clear();
}

fn shared_lru_cache_max_entries(cache: &RustSharedLruCache) -> usize {
    cache.max_entries()
}

fn shared_lru_cache_len(cache: &RustSharedLruCache) -> usize {
    cache.len()
}

fn int_lru_cache_new(max_entries: usize) -> Box<RustIntLruCache> {
    Box::new(RustIntLruCache::new(max_entries))
}

fn int_lru_cache_put(cache: &RustIntLruCache, key: i32, value: i32) {
    cache.put(key, value);
}

fn int_lru_cache_get(cache: &RustIntLruCache, key: i32) -> i32 {
    cache.get(key).unwrap_or(-1)
}

fn int_lru_cache_has(cache: &RustIntLruCache, key: i32) -> bool {
    cache.get(key).is_some()
}

fn int_lru_cache_max_entries(cache: &RustIntLruCache) -> usize {
    cache.max_entries()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_put_and_get() {
        let cache = RustSharedLruCache::new(1);

        // No value initially
        assert!(cache.get("1").is_none());

        // Put and get
        cache.put("1".to_string(), "1".to_string());
        let val = cache.get("1");
        assert_eq!(val, Some("1".to_string()));

        // Check eviction
        cache.put("2".to_string(), "2".to_string());
        assert!(cache.get("1").is_none());
        assert_eq!(cache.get("2"), Some("2".to_string()));

        // Check delete
        assert!(!cache.delete("1"));
        assert!(cache.get("1").is_none());
    }

    #[test]
    fn test_same_key() {
        let cache = RustIntLruCache::new(2);

        cache.put(1, 1);
        assert_eq!(cache.get(1), Some(1));

        cache.put(1, 2);
        assert_eq!(cache.get(1), Some(2));
    }
}
