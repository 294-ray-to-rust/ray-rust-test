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

//! BundleLocationIndex FFI bridge.
//!
//! This module provides a Rust implementation of the bundle location index
//! data structure used for fast bundle location lookup.

use std::collections::HashMap;

/// A bundle ID is a pair of (placement_group_id, bundle_index).
/// We represent both as byte arrays for FFI compatibility.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BundleId {
    placement_group_id: Vec<u8>,
    bundle_index: i64,
}

impl BundleId {
    pub fn new(placement_group_id: Vec<u8>, bundle_index: i64) -> Self {
        Self {
            placement_group_id,
            bundle_index,
        }
    }

    pub fn placement_group_id(&self) -> &[u8] {
        &self.placement_group_id
    }

    pub fn bundle_index(&self) -> i64 {
        self.bundle_index
    }
}

/// Bundle location: node_id where the bundle is placed.
/// We don't include BundleSpecification for simplicity - the test doesn't use it.
#[derive(Debug, Clone)]
pub struct BundleLocation {
    node_id: Vec<u8>,
}

impl BundleLocation {
    pub fn new(node_id: Vec<u8>) -> Self {
        Self { node_id }
    }

    pub fn node_id(&self) -> &[u8] {
        &self.node_id
    }
}

/// A map of bundle IDs to their locations.
pub struct BundleLocations {
    inner: HashMap<(Vec<u8>, i64), Vec<u8>>, // (pg_id, index) -> node_id
}

impl BundleLocations {
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }

    pub fn insert(&mut self, pg_id: Vec<u8>, bundle_index: i64, node_id: Vec<u8>) {
        self.inner.insert((pg_id, bundle_index), node_id);
    }

    pub fn get(&self, pg_id: &[u8], bundle_index: i64) -> Option<&Vec<u8>> {
        self.inner.get(&(pg_id.to_vec(), bundle_index))
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&(Vec<u8>, i64), &Vec<u8>)> {
        self.inner.iter()
    }

    pub fn remove(&mut self, pg_id: &[u8], bundle_index: i64) -> Option<Vec<u8>> {
        self.inner.remove(&(pg_id.to_vec(), bundle_index))
    }
}

impl Default for BundleLocations {
    fn default() -> Self {
        Self::new()
    }
}

/// A data structure that helps fast bundle location lookup.
/// Maintains two indices:
/// 1. placement_group_id -> bundle_locations
/// 2. node_id -> bundle_locations
pub struct RustBundleLocationIndex {
    // Map from placement group ID to bundle locations
    pg_to_bundles: HashMap<Vec<u8>, HashMap<i64, Vec<u8>>>,
    // Map from node ID to set of (pg_id, bundle_index) pairs
    node_to_bundles: HashMap<Vec<u8>, Vec<(Vec<u8>, i64)>>,
}

impl RustBundleLocationIndex {
    pub fn new() -> Self {
        Self {
            pg_to_bundles: HashMap::new(),
            node_to_bundles: HashMap::new(),
        }
    }

    /// Add or update bundle locations.
    pub fn add_or_update_bundle_locations(&mut self, locations: &BundleLocations) {
        for ((pg_id, bundle_index), node_id) in locations.iter() {
            self.add_or_update_bundle_location(pg_id.clone(), *bundle_index, node_id.clone());
        }
    }

    /// Add or update a single bundle location.
    pub fn add_or_update_bundle_location(
        &mut self,
        pg_id: Vec<u8>,
        bundle_index: i64,
        node_id: Vec<u8>,
    ) {
        // Check if we need to remove from an old node first
        let old_node_to_remove = self
            .pg_to_bundles
            .get(&pg_id)
            .and_then(|bundles| bundles.get(&bundle_index))
            .filter(|old_node| *old_node != &node_id)
            .cloned();

        // Remove from old node index if needed
        if let Some(old_node_id) = old_node_to_remove {
            self.remove_from_node_index(&old_node_id, &pg_id, bundle_index);
        }

        // Update pg_to_bundles
        let pg_bundles = self.pg_to_bundles.entry(pg_id.clone()).or_default();
        pg_bundles.insert(bundle_index, node_id.clone());

        // Update node_to_bundles
        let node_bundles = self.node_to_bundles.entry(node_id).or_default();
        let bundle_key = (pg_id, bundle_index);
        if !node_bundles.contains(&bundle_key) {
            node_bundles.push(bundle_key);
        }
    }

    fn remove_from_node_index(&mut self, node_id: &[u8], pg_id: &[u8], bundle_index: i64) {
        if let Some(node_bundles) = self.node_to_bundles.get_mut(node_id) {
            node_bundles.retain(|(pid, idx)| !(pid == pg_id && *idx == bundle_index));
            if node_bundles.is_empty() {
                self.node_to_bundles.remove(node_id);
            }
        }
    }

    /// Get bundle locations for a placement group.
    pub fn get_bundle_locations(&self, pg_id: &[u8]) -> Option<&HashMap<i64, Vec<u8>>> {
        self.pg_to_bundles.get(pg_id)
    }

    /// Get the node ID for a specific bundle.
    pub fn get_bundle_location(&self, pg_id: &[u8], bundle_index: i64) -> Option<&Vec<u8>> {
        self.pg_to_bundles
            .get(pg_id)
            .and_then(|bundles| bundles.get(&bundle_index))
    }

    /// Erase all bundles on a node.
    pub fn erase_node(&mut self, node_id: &[u8]) -> bool {
        if let Some(bundles) = self.node_to_bundles.remove(node_id) {
            for (pg_id, bundle_index) in bundles {
                if let Some(pg_bundles) = self.pg_to_bundles.get_mut(&pg_id) {
                    pg_bundles.remove(&bundle_index);
                    if pg_bundles.is_empty() {
                        self.pg_to_bundles.remove(&pg_id);
                    }
                }
            }
            true
        } else {
            false
        }
    }

    /// Erase all bundles for a placement group.
    pub fn erase_placement_group(&mut self, pg_id: &[u8]) -> bool {
        if let Some(bundles) = self.pg_to_bundles.remove(pg_id) {
            // Remove from node_to_bundles
            for (bundle_index, node_id) in bundles {
                if let Some(node_bundles) = self.node_to_bundles.get_mut(&node_id) {
                    node_bundles.retain(|(pid, idx)| !(pid == pg_id && *idx == bundle_index));
                    if node_bundles.is_empty() {
                        self.node_to_bundles.remove(&node_id);
                    }
                }
            }
            true
        } else {
            false
        }
    }
}

impl Default for RustBundleLocationIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cxx::bridge(namespace = "ffi")]
mod ffi {
    extern "Rust" {
        // BundleLocations
        type BundleLocations;

        fn bundle_locations_new() -> Box<BundleLocations>;
        fn bundle_locations_insert(
            locations: &mut BundleLocations,
            pg_id: &[u8],
            bundle_index: i64,
            node_id: &[u8],
        );
        fn bundle_locations_len(locations: &BundleLocations) -> usize;

        // RustBundleLocationIndex
        type RustBundleLocationIndex;

        fn bundle_location_index_new() -> Box<RustBundleLocationIndex>;
        fn bundle_location_index_add_or_update(
            index: &mut RustBundleLocationIndex,
            locations: &BundleLocations,
        );
        fn bundle_location_index_get_bundle_location(
            index: &RustBundleLocationIndex,
            pg_id: &[u8],
            bundle_index: i64,
        ) -> Vec<u8>;
        fn bundle_location_index_has_bundle_location(
            index: &RustBundleLocationIndex,
            pg_id: &[u8],
            bundle_index: i64,
        ) -> bool;
        fn bundle_location_index_has_placement_group(
            index: &RustBundleLocationIndex,
            pg_id: &[u8],
        ) -> bool;
        fn bundle_location_index_get_pg_bundle_count(
            index: &RustBundleLocationIndex,
            pg_id: &[u8],
        ) -> usize;
        fn bundle_location_index_erase_node(
            index: &mut RustBundleLocationIndex,
            node_id: &[u8],
        ) -> bool;
        fn bundle_location_index_erase_placement_group(
            index: &mut RustBundleLocationIndex,
            pg_id: &[u8],
        ) -> bool;
    }
}

// BundleLocations FFI functions
fn bundle_locations_new() -> Box<BundleLocations> {
    Box::new(BundleLocations::new())
}

fn bundle_locations_insert(
    locations: &mut BundleLocations,
    pg_id: &[u8],
    bundle_index: i64,
    node_id: &[u8],
) {
    locations.insert(pg_id.to_vec(), bundle_index, node_id.to_vec());
}

fn bundle_locations_len(locations: &BundleLocations) -> usize {
    locations.len()
}

// RustBundleLocationIndex FFI functions
fn bundle_location_index_new() -> Box<RustBundleLocationIndex> {
    Box::new(RustBundleLocationIndex::new())
}

fn bundle_location_index_add_or_update(
    index: &mut RustBundleLocationIndex,
    locations: &BundleLocations,
) {
    index.add_or_update_bundle_locations(locations);
}

fn bundle_location_index_get_bundle_location(
    index: &RustBundleLocationIndex,
    pg_id: &[u8],
    bundle_index: i64,
) -> Vec<u8> {
    index
        .get_bundle_location(pg_id, bundle_index)
        .cloned()
        .unwrap_or_default()
}

fn bundle_location_index_has_bundle_location(
    index: &RustBundleLocationIndex,
    pg_id: &[u8],
    bundle_index: i64,
) -> bool {
    index.get_bundle_location(pg_id, bundle_index).is_some()
}

fn bundle_location_index_has_placement_group(
    index: &RustBundleLocationIndex,
    pg_id: &[u8],
) -> bool {
    index.get_bundle_locations(pg_id).is_some()
}

fn bundle_location_index_get_pg_bundle_count(
    index: &RustBundleLocationIndex,
    pg_id: &[u8],
) -> usize {
    index
        .get_bundle_locations(pg_id)
        .map(|b| b.len())
        .unwrap_or(0)
}

fn bundle_location_index_erase_node(
    index: &mut RustBundleLocationIndex,
    node_id: &[u8],
) -> bool {
    index.erase_node(node_id)
}

fn bundle_location_index_erase_placement_group(
    index: &mut RustBundleLocationIndex,
    pg_id: &[u8],
) -> bool {
    index.erase_placement_group(pg_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_operations() {
        let mut index = RustBundleLocationIndex::new();

        let pg_id = vec![1u8; 16];
        let node_0 = vec![10u8; 28];
        let node_1 = vec![11u8; 28];

        // Initially empty
        assert!(!index.get_bundle_locations(&pg_id).is_some());
        assert!(!index.get_bundle_location(&pg_id, 0).is_some());

        // Add bundles
        let mut locations = BundleLocations::new();
        locations.insert(pg_id.clone(), 0, node_0.clone());
        locations.insert(pg_id.clone(), 2, node_1.clone());
        index.add_or_update_bundle_locations(&locations);

        // Verify
        assert!(index.get_bundle_locations(&pg_id).is_some());
        assert_eq!(index.get_bundle_location(&pg_id, 0), Some(&node_0));
        assert_eq!(index.get_bundle_location(&pg_id, 2), Some(&node_1));
        assert!(!index.get_bundle_location(&pg_id, 3).is_some());

        // Erase by node
        index.erase_node(&node_0);
        assert!(!index.get_bundle_location(&pg_id, 0).is_some());
        assert_eq!(index.get_bundle_location(&pg_id, 2), Some(&node_1));

        // Erase by placement group
        index.erase_placement_group(&pg_id);
        assert!(!index.get_bundle_locations(&pg_id).is_some());
    }
}
