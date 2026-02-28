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

//! gRPC utility functions FFI bridge.
//!
//! This module provides Rust implementations of gRPC-related utilities,
//! particularly map comparison functions used for comparing protobuf maps.

use std::collections::BTreeMap;

/// A simple "label in" structure that mimics protobuf's LabelIn message.
/// Contains a list of string values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RustLabelIn {
    values: Vec<String>,
}

impl RustLabelIn {
    pub fn new() -> Self {
        Self { values: Vec::new() }
    }

    pub fn add_value(&mut self, value: String) {
        self.values.push(value);
    }

    pub fn values(&self) -> &[String] {
        &self.values
    }
}

impl Default for RustLabelIn {
    fn default() -> Self {
        Self::new()
    }
}

/// A map from string keys to double values.
/// Used for testing map equality with simple types.
pub struct RustDoubleMap {
    inner: BTreeMap<String, f64>,
}

impl RustDoubleMap {
    pub fn new() -> Self {
        Self {
            inner: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, key: String, value: f64) {
        self.inner.insert(key, value);
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Check if this map equals another map.
    pub fn equals(&self, other: &RustDoubleMap) -> bool {
        if self.inner.len() != other.inner.len() {
            return false;
        }

        for (key, value) in &self.inner {
            match other.inner.get(key) {
                Some(other_value) => {
                    // For f64, we use exact equality (same as protobuf behavior)
                    if *value != *other_value {
                        return false;
                    }
                }
                None => return false,
            }
        }

        true
    }
}

impl Default for RustDoubleMap {
    fn default() -> Self {
        Self::new()
    }
}

/// A map from string keys to LabelIn values.
/// Used for testing map equality with message types.
pub struct RustLabelInMap {
    inner: BTreeMap<String, RustLabelIn>,
}

impl RustLabelInMap {
    pub fn new() -> Self {
        Self {
            inner: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, key: String, value: RustLabelIn) {
        self.inner.insert(key, value);
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Check if this map equals another map.
    /// Uses structural equality for LabelIn values.
    pub fn equals(&self, other: &RustLabelInMap) -> bool {
        if self.inner.len() != other.inner.len() {
            return false;
        }

        for (key, value) in &self.inner {
            match other.inner.get(key) {
                Some(other_value) => {
                    // Compare LabelIn values structurally
                    if value != other_value {
                        return false;
                    }
                }
                None => return false,
            }
        }

        true
    }
}

impl Default for RustLabelInMap {
    fn default() -> Self {
        Self::new()
    }
}

#[cxx::bridge(namespace = "ffi")]
mod ffi {
    extern "Rust" {
        // RustDoubleMap
        type RustDoubleMap;

        fn double_map_new() -> Box<RustDoubleMap>;
        fn double_map_insert(map: &mut RustDoubleMap, key: String, value: f64);
        fn double_map_len(map: &RustDoubleMap) -> usize;
        fn double_map_is_empty(map: &RustDoubleMap) -> bool;
        fn double_map_equals(lhs: &RustDoubleMap, rhs: &RustDoubleMap) -> bool;

        // RustLabelIn
        type RustLabelIn;

        fn label_in_new() -> Box<RustLabelIn>;
        fn label_in_clone(label_in: &RustLabelIn) -> Box<RustLabelIn>;
        fn label_in_add_value(label_in: &mut RustLabelIn, value: String);
        fn label_in_equals(lhs: &RustLabelIn, rhs: &RustLabelIn) -> bool;

        // RustLabelInMap
        type RustLabelInMap;

        fn label_in_map_new() -> Box<RustLabelInMap>;
        fn label_in_map_insert(map: &mut RustLabelInMap, key: String, value: Box<RustLabelIn>);
        fn label_in_map_len(map: &RustLabelInMap) -> usize;
        fn label_in_map_is_empty(map: &RustLabelInMap) -> bool;
        fn label_in_map_equals(lhs: &RustLabelInMap, rhs: &RustLabelInMap) -> bool;
    }
}

// RustDoubleMap FFI functions
fn double_map_new() -> Box<RustDoubleMap> {
    Box::new(RustDoubleMap::new())
}

fn double_map_insert(map: &mut RustDoubleMap, key: String, value: f64) {
    map.insert(key, value);
}

fn double_map_len(map: &RustDoubleMap) -> usize {
    map.len()
}

fn double_map_is_empty(map: &RustDoubleMap) -> bool {
    map.is_empty()
}

fn double_map_equals(lhs: &RustDoubleMap, rhs: &RustDoubleMap) -> bool {
    lhs.equals(rhs)
}

// RustLabelIn FFI functions
fn label_in_new() -> Box<RustLabelIn> {
    Box::new(RustLabelIn::new())
}

fn label_in_clone(label_in: &RustLabelIn) -> Box<RustLabelIn> {
    Box::new(label_in.clone())
}

fn label_in_add_value(label_in: &mut RustLabelIn, value: String) {
    label_in.add_value(value);
}

fn label_in_equals(lhs: &RustLabelIn, rhs: &RustLabelIn) -> bool {
    lhs == rhs
}

// RustLabelInMap FFI functions
fn label_in_map_new() -> Box<RustLabelInMap> {
    Box::new(RustLabelInMap::new())
}

fn label_in_map_insert(map: &mut RustLabelInMap, key: String, value: Box<RustLabelIn>) {
    map.insert(key, *value);
}

fn label_in_map_len(map: &RustLabelInMap) -> usize {
    map.len()
}

fn label_in_map_is_empty(map: &RustLabelInMap) -> bool {
    map.is_empty()
}

fn label_in_map_equals(lhs: &RustLabelInMap, rhs: &RustLabelInMap) -> bool {
    lhs.equals(rhs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_double_map_equality_size_mismatch() {
        let mut map1 = RustDoubleMap::new();
        let map2 = RustDoubleMap::new();
        map1.insert("key1".to_string(), 1.0);
        assert!(!map1.equals(&map2));
    }

    #[test]
    fn test_double_map_equality_missing_key() {
        let mut map1 = RustDoubleMap::new();
        let mut map2 = RustDoubleMap::new();
        map1.insert("key1".to_string(), 1.0);
        map2.insert("key2".to_string(), 1.0);
        assert!(!map1.equals(&map2));
    }

    #[test]
    fn test_double_map_equality_value_mismatch() {
        let mut map1 = RustDoubleMap::new();
        let mut map2 = RustDoubleMap::new();
        map1.insert("key1".to_string(), 1.0);
        map2.insert("key1".to_string(), 2.0);
        assert!(!map1.equals(&map2));
    }

    #[test]
    fn test_double_map_equality_equal() {
        let mut map1 = RustDoubleMap::new();
        let mut map2 = RustDoubleMap::new();
        map1.insert("key1".to_string(), 1.0);
        map2.insert("key1".to_string(), 1.0);
        assert!(map1.equals(&map2));
    }

    #[test]
    fn test_label_in_map_equality_not_equal() {
        let mut map1 = RustLabelInMap::new();
        let mut map2 = RustLabelInMap::new();

        let mut label1 = RustLabelIn::new();
        label1.add_value("value1".to_string());

        let mut label2 = RustLabelIn::new();
        label2.add_value("value2".to_string());

        map1.insert("key1".to_string(), label1);
        map2.insert("key1".to_string(), label2);

        assert!(!map1.equals(&map2));
    }

    #[test]
    fn test_label_in_map_equality_equal() {
        let mut map1 = RustLabelInMap::new();
        let mut map2 = RustLabelInMap::new();

        let mut label1 = RustLabelIn::new();
        label1.add_value("value1".to_string());

        let mut label2 = RustLabelIn::new();
        label2.add_value("value1".to_string());

        map1.insert("key1".to_string(), label1);
        map2.insert("key1".to_string(), label2);

        assert!(map1.equals(&map2));
    }
}
