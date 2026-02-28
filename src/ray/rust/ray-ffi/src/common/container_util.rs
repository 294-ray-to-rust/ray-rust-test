// Copyright 2025 The Ray Authors.
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

//! Container utility functions FFI bridge.
//!
//! Provides debug_string formatting, map_find_or_die, and erase_if operations.

use std::collections::BTreeMap;

/// Format a vector of integers as a debug string.
pub fn debug_string_int_vec(values: &[i32]) -> String {
    let items: Vec<String> = values.iter().map(|v| v.to_string()).collect();
    format!("[{}]", items.join(", "))
}

/// Format a vector of strings as a debug string.
pub fn debug_string_string_vec(values: &[String]) -> String {
    format!("[{}]", values.join(", "))
}

/// Format a map of int->int as a debug string.
pub fn debug_string_int_map(map: &BTreeMap<i32, i32>) -> String {
    let items: Vec<String> = map.iter().map(|(k, v)| format!("({}, {})", k, v)).collect();
    format!("[{}]", items.join(", "))
}

/// Format a pair of ints as a debug string.
pub fn debug_string_int_pair(first: i32, second: i32) -> String {
    format!("({}, {})", first, second)
}

/// Format an optional string as a debug string.
pub fn debug_string_optional_string(value: Option<&str>) -> String {
    match value {
        Some(s) => s.to_string(),
        None => "(nullopt)".to_string(),
    }
}

/// A simple int->int map for FFI.
pub struct RustIntMap {
    inner: BTreeMap<i32, i32>,
}

impl RustIntMap {
    pub fn new() -> Self {
        Self {
            inner: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, key: i32, value: i32) {
        self.inner.insert(key, value);
    }

    pub fn get(&self, key: i32) -> Option<i32> {
        self.inner.get(&key).copied()
    }

    pub fn find_or_die(&self, key: i32) -> i32 {
        *self.inner.get(&key).expect("Key not found in map")
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn debug_string(&self) -> String {
        debug_string_int_map(&self.inner)
    }
}

impl Default for RustIntMap {
    fn default() -> Self {
        Self::new()
    }
}

/// A simple int list for FFI (like std::list<int>).
pub struct RustIntList {
    inner: Vec<i32>,
}

impl RustIntList {
    pub fn new() -> Self {
        Self { inner: Vec::new() }
    }

    pub fn from_vec(values: Vec<i32>) -> Self {
        Self { inner: values }
    }

    pub fn push(&mut self, value: i32) {
        self.inner.push(value);
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn to_vec(&self) -> Vec<i32> {
        self.inner.clone()
    }

    /// Remove elements where predicate returns true.
    pub fn erase_if<F>(&mut self, pred: F)
    where
        F: Fn(i32) -> bool,
    {
        self.inner.retain(|v| !pred(*v));
    }
}

impl Default for RustIntList {
    fn default() -> Self {
        Self::new()
    }
}

/// A map of int -> deque<int> for testing erase_if on nested structures.
pub struct RustIntDequeMap {
    inner: BTreeMap<i32, Vec<i32>>,
}

impl RustIntDequeMap {
    pub fn new() -> Self {
        Self {
            inner: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, key: i32, values: Vec<i32>) {
        self.inner.insert(key, values);
    }

    pub fn get(&self, key: i32) -> Option<&Vec<i32>> {
        self.inner.get(&key)
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Remove values matching predicate from all deques, remove empty deques.
    pub fn erase_if<F>(&mut self, pred: F)
    where
        F: Fn(i32) -> bool,
    {
        // Remove matching elements from each deque
        for deque in self.inner.values_mut() {
            deque.retain(|v| !pred(*v));
        }
        // Remove entries with empty deques
        self.inner.retain(|_, v| !v.is_empty());
    }
}

impl Default for RustIntDequeMap {
    fn default() -> Self {
        Self::new()
    }
}

#[cxx::bridge(namespace = "ffi")]
mod ffi {
    extern "Rust" {
        // Debug string functions
        fn debug_string_int_vec(values: &[i32]) -> String;
        fn debug_string_string_vec(values: &[String]) -> String;
        fn debug_string_int_pair(first: i32, second: i32) -> String;
        fn debug_string_optional_string_some(value: &str) -> String;
        fn debug_string_optional_string_none() -> String;

        // RustIntMap
        type RustIntMap;
        fn int_map_new() -> Box<RustIntMap>;
        fn int_map_insert(map: &mut RustIntMap, key: i32, value: i32);
        fn int_map_get(map: &RustIntMap, key: i32) -> i32;
        fn int_map_contains(map: &RustIntMap, key: i32) -> bool;
        fn int_map_find_or_die(map: &RustIntMap, key: i32) -> i32;
        fn int_map_debug_string(map: &RustIntMap) -> String;

        // RustIntList
        type RustIntList;
        fn int_list_new() -> Box<RustIntList>;
        fn int_list_from_vec(values: Vec<i32>) -> Box<RustIntList>;
        fn int_list_push(list: &mut RustIntList, value: i32);
        fn int_list_len(list: &RustIntList) -> usize;
        fn int_list_to_vec(list: &RustIntList) -> Vec<i32>;
        fn int_list_erase_if_even(list: &mut RustIntList);

        // RustIntDequeMap
        type RustIntDequeMap;
        fn int_deque_map_new() -> Box<RustIntDequeMap>;
        fn int_deque_map_insert(map: &mut RustIntDequeMap, key: i32, values: Vec<i32>);
        fn int_deque_map_get(map: &RustIntDequeMap, key: i32) -> Vec<i32>;
        fn int_deque_map_contains(map: &RustIntDequeMap, key: i32) -> bool;
        fn int_deque_map_len(map: &RustIntDequeMap) -> usize;
        fn int_deque_map_erase_if_even(map: &mut RustIntDequeMap);
    }
}

// FFI wrapper functions
fn debug_string_optional_string_some(value: &str) -> String {
    debug_string_optional_string(Some(value))
}

fn debug_string_optional_string_none() -> String {
    debug_string_optional_string(None)
}

fn int_map_new() -> Box<RustIntMap> {
    Box::new(RustIntMap::new())
}

fn int_map_insert(map: &mut RustIntMap, key: i32, value: i32) {
    map.insert(key, value);
}

fn int_map_get(map: &RustIntMap, key: i32) -> i32 {
    map.get(key).unwrap_or(0)
}

fn int_map_contains(map: &RustIntMap, key: i32) -> bool {
    map.get(key).is_some()
}

fn int_map_find_or_die(map: &RustIntMap, key: i32) -> i32 {
    map.find_or_die(key)
}

fn int_map_debug_string(map: &RustIntMap) -> String {
    map.debug_string()
}

fn int_list_new() -> Box<RustIntList> {
    Box::new(RustIntList::new())
}

fn int_list_from_vec(values: Vec<i32>) -> Box<RustIntList> {
    Box::new(RustIntList::from_vec(values))
}

fn int_list_push(list: &mut RustIntList, value: i32) {
    list.push(value);
}

fn int_list_len(list: &RustIntList) -> usize {
    list.len()
}

fn int_list_to_vec(list: &RustIntList) -> Vec<i32> {
    list.to_vec()
}

fn int_list_erase_if_even(list: &mut RustIntList) {
    list.erase_if(|v| v % 2 == 0);
}

fn int_deque_map_new() -> Box<RustIntDequeMap> {
    Box::new(RustIntDequeMap::new())
}

fn int_deque_map_insert(map: &mut RustIntDequeMap, key: i32, values: Vec<i32>) {
    map.insert(key, values);
}

fn int_deque_map_get(map: &RustIntDequeMap, key: i32) -> Vec<i32> {
    map.get(key).cloned().unwrap_or_default()
}

fn int_deque_map_contains(map: &RustIntDequeMap, key: i32) -> bool {
    map.get(key).is_some()
}

fn int_deque_map_len(map: &RustIntDequeMap) -> usize {
    map.len()
}

fn int_deque_map_erase_if_even(map: &mut RustIntDequeMap) {
    map.erase_if(|v| v % 2 == 0);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_debug_string() {
        // Numerical values
        assert_eq!(debug_string_int_vec(&[2]), "[2]");

        // String values
        assert_eq!(debug_string_string_vec(&["hello".to_string()]), "[hello]");

        // Non-associative containers
        assert_eq!(debug_string_int_vec(&[1, 2]), "[1, 2]");
        assert_eq!(debug_string_int_vec(&[1, 2, 3]), "[1, 2, 3]");

        // Pairs
        assert_eq!(debug_string_int_pair(1, 2), "(1, 2)");

        // Optional
        assert_eq!(debug_string_optional_string(None), "(nullopt)");
        assert_eq!(
            debug_string_optional_string(Some("hello")),
            "hello"
        );

        // Maps
        let mut map = BTreeMap::new();
        map.insert(1, 2);
        map.insert(3, 4);
        assert_eq!(debug_string_int_map(&map), "[(1, 2), (3, 4)]");
    }

    #[test]
    fn test_map_find_or_die() {
        let mut map = RustIntMap::new();
        map.insert(1, 2);
        map.insert(3, 4);
        assert_eq!(map.find_or_die(1), 2);
    }

    #[test]
    #[should_panic(expected = "Key not found")]
    fn test_map_find_or_die_panics() {
        let map = RustIntMap::new();
        map.find_or_die(5);
    }

    #[test]
    fn test_erase_if_list() {
        let mut list = RustIntList::from_vec(vec![1, 2, 3, 4]);
        list.erase_if(|v| v % 2 == 0);
        assert_eq!(list.to_vec(), vec![1, 3]);

        let mut list2 = RustIntList::from_vec(vec![1, 2, 3]);
        list2.erase_if(|v| v % 2 == 0);
        assert_eq!(list2.to_vec(), vec![1, 3]);

        let mut list3 = RustIntList::new();
        list3.erase_if(|v| v % 2 == 0);
        assert!(list3.is_empty());
    }

    #[test]
    fn test_erase_if_deque_map() {
        let mut map = RustIntDequeMap::new();
        map.insert(1, vec![1, 3]);
        map.insert(2, vec![2, 4]);
        map.insert(3, vec![5, 6]);

        map.erase_if(|v| v % 2 == 0);

        assert_eq!(map.len(), 2);
        assert_eq!(map.get(1), Some(&vec![1, 3]));
        assert_eq!(map.get(2), None); // All even, removed
        assert_eq!(map.get(3), Some(&vec![5]));
    }
}
