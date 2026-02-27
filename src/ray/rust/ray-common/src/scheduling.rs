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

//! Scheduling types for Ray resource management.
//!
//! This module provides Rust equivalents of Ray's C++ scheduling types,
//! including FixedPoint, ResourceID, ResourceSet, and NodeResourceSet.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::{Add, AddAssign, Neg, Sub, SubAssign};
use std::sync::RwLock;

/// The precision of fractional resource quantity (matches kResourceUnitScaling).
pub const RESOURCE_UNIT_SCALING: i64 = 10000;

/// Predefined resource types (matches PredefinedResourcesEnum).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i64)]
pub enum PredefinedResource {
    Cpu = 0,
    Mem = 1,
    Gpu = 2,
    ObjectStoreMem = 3,
}

/// Resource label constants.
pub const CPU_RESOURCE_LABEL: &str = "CPU";
pub const GPU_RESOURCE_LABEL: &str = "GPU";
pub const OBJECT_STORE_MEMORY_RESOURCE_LABEL: &str = "object_store_memory";
pub const MEMORY_RESOURCE_LABEL: &str = "memory";
pub const BUNDLE_RESOURCE_LABEL: &str = "bundle";

/// Prefix for implicit resources.
pub const IMPLICIT_RESOURCE_PREFIX: &str = "node:__internal_implicit_resource_";

/// Fixed point data type for resource quantities.
/// This matches the C++ FixedPoint class with 64-bit internal representation.
#[derive(Clone, Copy, Default)]
pub struct FixedPoint {
    value: i64,
}

impl FixedPoint {
    /// Create a new FixedPoint with raw internal value.
    pub const fn from_raw(value: i64) -> Self {
        Self { value }
    }

    /// Create a FixedPoint from a double value.
    pub fn from_double(d: f64) -> Self {
        Self {
            value: (d * RESOURCE_UNIT_SCALING as f64) as i64,
        }
    }

    /// Create a FixedPoint from an integer value.
    pub fn from_int(i: i32) -> Self {
        Self {
            value: (i as i64) * RESOURCE_UNIT_SCALING,
        }
    }

    /// Create a FixedPoint from an i64 value.
    pub fn from_i64(i: i64) -> Self {
        Self::from_double(i as f64)
    }

    /// Create a zero FixedPoint.
    pub const fn zero() -> Self {
        Self { value: 0 }
    }

    /// Convert to double representation.
    pub fn to_double(&self) -> f64 {
        (self.value as f64).round() / RESOURCE_UNIT_SCALING as f64
    }

    /// Get the raw internal value.
    pub const fn raw(&self) -> i64 {
        self.value
    }

    /// Sum a list of FixedPoints.
    pub fn sum(list: &[FixedPoint]) -> Self {
        let mut sum = FixedPoint::zero();
        for value in list {
            sum += *value;
        }
        sum
    }

    /// Check if this is zero.
    pub fn is_zero(&self) -> bool {
        self.value == 0
    }

    /// Check if this is positive.
    pub fn is_positive(&self) -> bool {
        self.value > 0
    }

    /// Check if this is negative.
    pub fn is_negative(&self) -> bool {
        self.value < 0
    }
}

impl Add for FixedPoint {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self {
            value: self.value + other.value,
        }
    }
}

impl AddAssign for FixedPoint {
    fn add_assign(&mut self, other: Self) {
        self.value += other.value;
    }
}

impl Sub for FixedPoint {
    type Output = Self;

    fn sub(self, other: Self) -> Self {
        Self {
            value: self.value - other.value,
        }
    }
}

impl SubAssign for FixedPoint {
    fn sub_assign(&mut self, other: Self) {
        self.value -= other.value;
    }
}

impl Neg for FixedPoint {
    type Output = Self;

    fn neg(self) -> Self {
        Self { value: -self.value }
    }
}

impl Add<f64> for FixedPoint {
    type Output = Self;

    fn add(self, d: f64) -> Self {
        Self {
            value: self.value + (d * RESOURCE_UNIT_SCALING as f64) as i64,
        }
    }
}

impl Sub<f64> for FixedPoint {
    type Output = Self;

    fn sub(self, d: f64) -> Self {
        Self {
            value: self.value - (d * RESOURCE_UNIT_SCALING as f64) as i64,
        }
    }
}

impl AddAssign<f64> for FixedPoint {
    fn add_assign(&mut self, d: f64) {
        self.value += (d * RESOURCE_UNIT_SCALING as f64) as i64;
    }
}

impl AddAssign<i64> for FixedPoint {
    fn add_assign(&mut self, i: i64) {
        self.value += (i as f64 * RESOURCE_UNIT_SCALING as f64) as i64;
    }
}

impl PartialEq for FixedPoint {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl Eq for FixedPoint {}

impl PartialOrd for FixedPoint {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FixedPoint {
    fn cmp(&self, other: &Self) -> Ordering {
        self.value.cmp(&other.value)
    }
}

impl Hash for FixedPoint {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.value.hash(state);
    }
}

impl fmt::Debug for FixedPoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FixedPoint({})", self.value)
    }
}

impl fmt::Display for FixedPoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

impl From<f64> for FixedPoint {
    fn from(d: f64) -> Self {
        Self::from_double(d)
    }
}

impl From<i32> for FixedPoint {
    fn from(i: i32) -> Self {
        Self::from_int(i)
    }
}

impl From<i64> for FixedPoint {
    fn from(i: i64) -> Self {
        Self::from_i64(i)
    }
}

// Global string-to-id map for resources (singleton).
lazy_static::lazy_static! {
    static ref RESOURCE_STRING_MAP: RwLock<StringIdMap> = {
        let mut map = StringIdMap::new();
        // Pre-populate with predefined resources
        map.insert_or_die(CPU_RESOURCE_LABEL, PredefinedResource::Cpu as i64);
        map.insert_or_die(GPU_RESOURCE_LABEL, PredefinedResource::Gpu as i64);
        map.insert_or_die(OBJECT_STORE_MEMORY_RESOURCE_LABEL, PredefinedResource::ObjectStoreMem as i64);
        map.insert_or_die(MEMORY_RESOURCE_LABEL, PredefinedResource::Mem as i64);
        RwLock::new(map)
    };
}

/// A bidirectional map between string IDs and integer IDs.
#[derive(Debug)]
pub struct StringIdMap {
    string_to_int: HashMap<String, i64>,
    int_to_string: HashMap<i64, String>,
}

impl StringIdMap {
    /// Create a new empty StringIdMap.
    pub fn new() -> Self {
        Self {
            string_to_int: HashMap::new(),
            int_to_string: HashMap::new(),
        }
    }

    /// Get integer ID associated with a string ID.
    pub fn get_int(&self, string_id: &str) -> Option<i64> {
        self.string_to_int.get(string_id).copied()
    }

    /// Get string ID associated with an integer ID.
    pub fn get_string(&self, id: i64) -> Option<&str> {
        self.int_to_string.get(&id).map(|s| s.as_str())
    }

    /// Insert a string ID and get/create the associated integer ID.
    pub fn insert(&mut self, string_id: &str) -> i64 {
        if let Some(&id) = self.string_to_int.get(string_id) {
            return id;
        }

        // Use hash of string as the ID
        use std::collections::hash_map::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        string_id.hash(&mut hasher);
        let id = hasher.finish() as i64;

        self.string_to_int.insert(string_id.to_string(), id);
        self.int_to_string.insert(id, string_id.to_string());
        id
    }

    /// Insert a string ID with a specific integer ID.
    /// Panics if either already exists with different mappings.
    pub fn insert_or_die(&mut self, string_id: &str, id: i64) {
        if let Some(&existing) = self.string_to_int.get(string_id) {
            assert_eq!(existing, id, "String ID already exists with different int ID");
        }
        if let Some(existing) = self.int_to_string.get(&id) {
            assert_eq!(existing, string_id, "Int ID already exists with different string");
        }
        self.string_to_int.insert(string_id.to_string(), id);
        self.int_to_string.insert(id, string_id.to_string());
    }

    /// Get the number of entries.
    pub fn count(&self) -> usize {
        self.string_to_int.len()
    }
}

impl Default for StringIdMap {
    fn default() -> Self {
        Self::new()
    }
}

/// A resource ID that represents a string identifier with efficient integer storage.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct ResourceId {
    id: i64,
}

impl ResourceId {
    /// Create a nil ResourceId.
    pub const fn nil() -> Self {
        Self { id: -1 }
    }

    /// Create a ResourceId from a string name.
    pub fn from_name(name: &str) -> Self {
        let id = RESOURCE_STRING_MAP.write().unwrap().insert(name);
        Self { id }
    }

    /// Create a ResourceId from an integer ID.
    pub const fn from_int(id: i64) -> Self {
        Self { id }
    }

    /// Get the integer representation.
    pub const fn to_int(&self) -> i64 {
        self.id
    }

    /// Get the string representation.
    pub fn to_string(&self) -> String {
        RESOURCE_STRING_MAP
            .read()
            .unwrap()
            .get_string(self.id)
            .map(|s| s.to_string())
            .unwrap_or_default()
    }

    /// Check if this is a nil ResourceId.
    pub const fn is_nil(&self) -> bool {
        self.id == -1
    }

    /// Check if this is a predefined resource.
    pub fn is_predefined(&self) -> bool {
        self.id >= 0 && self.id < 4 // PredefinedResourcesEnum_MAX
    }

    /// Check if this is an implicit resource.
    pub fn is_implicit(&self) -> bool {
        if self.is_predefined() {
            return false;
        }
        self.to_string().starts_with(IMPLICIT_RESOURCE_PREFIX)
    }

    /// Check if this is a unit-instance resource.
    pub fn is_unit_instance(&self) -> bool {
        // GPU is the default unit-instance resource
        self.id == PredefinedResource::Gpu as i64
    }

    /// Get the CPU ResourceId.
    pub fn cpu() -> Self {
        Self::from_int(PredefinedResource::Cpu as i64)
    }

    /// Get the Memory ResourceId.
    pub fn memory() -> Self {
        Self::from_int(PredefinedResource::Mem as i64)
    }

    /// Get the GPU ResourceId.
    pub fn gpu() -> Self {
        Self::from_int(PredefinedResource::Gpu as i64)
    }

    /// Get the ObjectStoreMemory ResourceId.
    pub fn object_store_memory() -> Self {
        Self::from_int(PredefinedResource::ObjectStoreMem as i64)
    }
}

impl PartialOrd for ResourceId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ResourceId {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

impl fmt::Debug for ResourceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ResourceId({})", self.to_string())
    }
}

impl fmt::Display for ResourceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

/// A set of resources and their quantities.
/// If any resource value is changed to 0, the resource will be removed.
#[derive(Clone, Default)]
pub struct ResourceSet {
    resources: HashMap<ResourceId, FixedPoint>,
}

impl ResourceSet {
    /// Create an empty ResourceSet.
    pub fn new() -> Self {
        Self {
            resources: HashMap::new(),
        }
    }

    /// Create a ResourceSet from a string-keyed map.
    pub fn from_string_map(resource_map: &HashMap<String, f64>) -> Self {
        let mut set = Self::new();
        for (name, &value) in resource_map {
            let id = ResourceId::from_name(name);
            let fp = FixedPoint::from_double(value);
            if !fp.is_zero() {
                set.resources.insert(id, fp);
            }
        }
        set
    }

    /// Create a ResourceSet from a ResourceId-keyed map.
    pub fn from_resource_map(resource_map: &HashMap<ResourceId, FixedPoint>) -> Self {
        let mut set = Self::new();
        for (&id, &value) in resource_map {
            if !value.is_zero() {
                set.resources.insert(id, value);
            }
        }
        set
    }

    /// Get the value for a resource, or zero if not present.
    pub fn get(&self, resource_id: ResourceId) -> FixedPoint {
        self.resources.get(&resource_id).copied().unwrap_or(FixedPoint::zero())
    }

    /// Set a resource value. If zero, the resource is removed.
    pub fn set(&mut self, resource_id: ResourceId, value: FixedPoint) -> &mut Self {
        if value.is_zero() {
            self.resources.remove(&resource_id);
        } else {
            self.resources.insert(resource_id, value);
        }
        self
    }

    /// Check if a resource exists in the set.
    pub fn has(&self, resource_id: ResourceId) -> bool {
        self.resources.contains_key(&resource_id)
    }

    /// Get the number of resources in the set.
    pub fn size(&self) -> usize {
        self.resources.len()
    }

    /// Clear all resources.
    pub fn clear(&mut self) {
        self.resources.clear();
    }

    /// Check if the set is empty.
    pub fn is_empty(&self) -> bool {
        self.resources.is_empty()
    }

    /// Get an iterator over resource IDs.
    pub fn resource_ids(&self) -> impl Iterator<Item = &ResourceId> {
        self.resources.keys()
    }

    /// Get the underlying resources map.
    pub fn resources(&self) -> &HashMap<ResourceId, FixedPoint> {
        &self.resources
    }

    /// Convert to a string-keyed map with double values.
    pub fn to_resource_map(&self) -> HashMap<String, f64> {
        self.resources
            .iter()
            .map(|(&id, &value)| (id.to_string(), value.to_double()))
            .collect()
    }

    /// Get a debug string representation.
    pub fn debug_string(&self) -> String {
        let parts: Vec<String> = self
            .resources
            .iter()
            .map(|(id, value)| format!("{}: {}", id.to_string(), value.to_double()))
            .collect();
        format!("{{{}}}", parts.join(", "))
    }
}

impl PartialEq for ResourceSet {
    fn eq(&self, other: &Self) -> bool {
        self.resources == other.resources
    }
}

impl Eq for ResourceSet {}

impl Add for ResourceSet {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        let mut result = self.clone();
        result += other;
        result
    }
}

impl AddAssign for ResourceSet {
    fn add_assign(&mut self, other: Self) {
        for (&id, &value) in &other.resources {
            let current = self.get(id);
            self.set(id, current + value);
        }
    }
}

impl Sub for ResourceSet {
    type Output = Self;

    fn sub(self, other: Self) -> Self {
        let mut result = self.clone();
        result -= other;
        result
    }
}

impl SubAssign for ResourceSet {
    fn sub_assign(&mut self, other: Self) {
        for (&id, &value) in &other.resources {
            let current = self.get(id);
            self.set(id, current - value);
        }
    }
}

impl ResourceSet {
    /// Check if this set is a subset of another (all values <= other's values).
    pub fn is_subset_of(&self, other: &Self) -> bool {
        for (&id, &value) in &self.resources {
            if value > other.get(id) {
                return false;
            }
        }
        true
    }

    /// Check if this set is a superset of another (all values >= other's values).
    pub fn is_superset_of(&self, other: &Self) -> bool {
        other.is_subset_of(self)
    }
}

impl Hash for ResourceSet {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Hash based on the resource map
        let map = self.to_resource_map();
        state.write_usize(map.len());
        for (name, value) in &map {
            name.hash(state);
            value.to_bits().hash(state);
        }
    }
}

impl fmt::Debug for ResourceSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ResourceSet{}", self.debug_string())
    }
}

/// A set of node resources and their values.
/// Node resources contain both explicit resources (default value is 0)
/// and implicit resources (default value is 1).
/// Negative values are valid in this set.
#[derive(Clone, Default)]
pub struct NodeResourceSet {
    resources: HashMap<ResourceId, FixedPoint>,
}

impl NodeResourceSet {
    /// Create an empty NodeResourceSet.
    pub fn new() -> Self {
        Self {
            resources: HashMap::new(),
        }
    }

    /// Create a NodeResourceSet from a string-keyed map.
    pub fn from_string_map(resource_map: &HashMap<String, f64>) -> Self {
        let mut set = Self::new();
        for (name, &value) in resource_map {
            let id = ResourceId::from_name(name);
            let fp = FixedPoint::from_double(value);
            set.set(id, fp);
        }
        set
    }

    /// Create a NodeResourceSet from a ResourceId-keyed map.
    pub fn from_resource_map(resource_map: &HashMap<ResourceId, FixedPoint>) -> Self {
        let mut set = Self::new();
        for (&id, &value) in resource_map {
            set.set(id, value);
        }
        set
    }

    /// Get the default value for a resource.
    fn resource_default_value(&self, resource_id: ResourceId) -> FixedPoint {
        if resource_id.is_implicit() {
            FixedPoint::from_int(1)
        } else {
            FixedPoint::zero()
        }
    }

    /// Get the value for a resource, or the default if not present.
    pub fn get(&self, resource_id: ResourceId) -> FixedPoint {
        self.resources
            .get(&resource_id)
            .copied()
            .unwrap_or_else(|| self.resource_default_value(resource_id))
    }

    /// Set a resource value. If equal to default, the resource is removed.
    pub fn set(&mut self, resource_id: ResourceId, value: FixedPoint) -> &mut Self {
        let default = self.resource_default_value(resource_id);
        if value == default {
            self.resources.remove(&resource_id);
        } else {
            self.resources.insert(resource_id, value);
        }
        self
    }

    /// Check if a resource exists (value != default).
    pub fn has(&self, resource_id: ResourceId) -> bool {
        self.resources.contains_key(&resource_id)
    }

    /// Check if this set is a superset of a ResourceSet.
    pub fn is_superset_of(&self, other: &ResourceSet) -> bool {
        for (&id, &value) in other.resources() {
            if self.get(id) < value {
                return false;
            }
        }
        true
    }

    /// Remove negative values from this set.
    pub fn remove_negative(&mut self) {
        self.resources.retain(|_, v| !v.is_negative());
    }

    /// Convert to a string-keyed map with double values.
    pub fn to_resource_map(&self) -> HashMap<String, f64> {
        self.resources
            .iter()
            .map(|(&id, &value)| (id.to_string(), value.to_double()))
            .collect()
    }

    /// Get the IDs of explicit resources.
    pub fn explicit_resource_ids(&self) -> Vec<ResourceId> {
        self.resources
            .keys()
            .filter(|id| !id.is_implicit())
            .copied()
            .collect()
    }

    /// Get a debug string representation.
    pub fn debug_string(&self) -> String {
        let parts: Vec<String> = self
            .resources
            .iter()
            .map(|(id, value)| format!("{}: {}", id.to_string(), value.to_double()))
            .collect();
        format!("{{{}}}", parts.join(", "))
    }
}

impl SubAssign<ResourceSet> for NodeResourceSet {
    fn sub_assign(&mut self, other: ResourceSet) {
        for (&id, &value) in other.resources() {
            let current = self.get(id);
            self.set(id, current - value);
        }
    }
}

impl PartialEq for NodeResourceSet {
    fn eq(&self, other: &Self) -> bool {
        self.resources == other.resources
    }
}

impl Eq for NodeResourceSet {}

impl fmt::Debug for NodeResourceSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NodeResourceSet{}", self.debug_string())
    }
}

/// Convert a slice of doubles to FixedPoints.
pub fn fixed_point_vector_from_double(vector: &[f64]) -> Vec<FixedPoint> {
    vector.iter().map(|&d| FixedPoint::from_double(d)).collect()
}

/// Convert a slice of FixedPoints to doubles.
pub fn fixed_point_vector_to_double(vector: &[FixedPoint]) -> Vec<f64> {
    vector.iter().map(|fp| fp.to_double()).collect()
}

/// Convert a slice of FixedPoints to a string.
pub fn fixed_point_vector_to_string(vector: &[FixedPoint]) -> String {
    let parts: Vec<String> = vector.iter().map(|fp| fp.to_string()).collect();
    format!("[{}]", parts.join(", "))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fixed_point_from_double() {
        let fp = FixedPoint::from_double(1.5);
        assert_eq!(fp.to_double(), 1.5);
    }

    #[test]
    fn test_fixed_point_from_int() {
        let fp = FixedPoint::from_int(3);
        assert_eq!(fp.to_double(), 3.0);
    }

    #[test]
    fn test_fixed_point_addition() {
        let fp1 = FixedPoint::from_double(1.5);
        let fp2 = FixedPoint::from_double(2.5);
        let result = fp1 + fp2;
        assert_eq!(result.to_double(), 4.0);
    }

    #[test]
    fn test_fixed_point_subtraction() {
        let fp1 = FixedPoint::from_double(5.0);
        let fp2 = FixedPoint::from_double(2.5);
        let result = fp1 - fp2;
        assert_eq!(result.to_double(), 2.5);
    }

    #[test]
    fn test_fixed_point_negation() {
        let fp = FixedPoint::from_double(3.0);
        let neg = -fp;
        assert_eq!(neg.to_double(), -3.0);
    }

    #[test]
    fn test_fixed_point_comparison() {
        let fp1 = FixedPoint::from_double(1.5);
        let fp2 = FixedPoint::from_double(2.5);
        let fp3 = FixedPoint::from_double(1.5);

        assert!(fp1 < fp2);
        assert!(fp2 > fp1);
        assert!(fp1 <= fp2);
        assert!(fp2 >= fp1);
        assert_eq!(fp1, fp3);
        assert_ne!(fp1, fp2);
    }

    #[test]
    fn test_fixed_point_sum() {
        let values = vec![
            FixedPoint::from_double(1.0),
            FixedPoint::from_double(2.0),
            FixedPoint::from_double(3.0),
        ];
        let sum = FixedPoint::sum(&values);
        assert_eq!(sum.to_double(), 6.0);
    }

    #[test]
    fn test_fixed_point_add_double() {
        let fp = FixedPoint::from_double(1.0);
        let result = fp + 2.5;
        assert_eq!(result.to_double(), 3.5);
    }

    #[test]
    fn test_fixed_point_sub_double() {
        let fp = FixedPoint::from_double(5.0);
        let result = fp - 2.5;
        assert_eq!(result.to_double(), 2.5);
    }

    #[test]
    fn test_resource_id_predefined() {
        let cpu = ResourceId::cpu();
        assert!(cpu.is_predefined());
        assert!(!cpu.is_implicit());
        assert_eq!(cpu.to_string(), CPU_RESOURCE_LABEL);

        let gpu = ResourceId::gpu();
        assert!(gpu.is_predefined());
        assert!(gpu.is_unit_instance());
    }

    #[test]
    fn test_resource_id_custom() {
        let custom = ResourceId::from_name("custom_resource");
        assert!(!custom.is_predefined());
        assert!(!custom.is_nil());
        assert_eq!(custom.to_string(), "custom_resource");
    }

    #[test]
    fn test_resource_set_basic() {
        let mut set = ResourceSet::new();
        assert!(set.is_empty());

        let cpu = ResourceId::cpu();
        set.set(cpu, FixedPoint::from_double(4.0));
        assert!(!set.is_empty());
        assert_eq!(set.size(), 1);
        assert!(set.has(cpu));
        assert_eq!(set.get(cpu).to_double(), 4.0);

        // Setting to zero removes
        set.set(cpu, FixedPoint::zero());
        assert!(set.is_empty());
    }

    #[test]
    fn test_resource_set_from_string_map() {
        let mut map = HashMap::new();
        map.insert("CPU".to_string(), 4.0);
        map.insert("GPU".to_string(), 2.0);

        let set = ResourceSet::from_string_map(&map);
        assert_eq!(set.size(), 2);
        assert_eq!(set.get(ResourceId::cpu()).to_double(), 4.0);
        assert_eq!(set.get(ResourceId::gpu()).to_double(), 2.0);
    }

    #[test]
    fn test_resource_set_addition() {
        let mut set1 = ResourceSet::new();
        set1.set(ResourceId::cpu(), FixedPoint::from_double(2.0));

        let mut set2 = ResourceSet::new();
        set2.set(ResourceId::cpu(), FixedPoint::from_double(3.0));
        set2.set(ResourceId::gpu(), FixedPoint::from_double(1.0));

        let result = set1 + set2;
        assert_eq!(result.get(ResourceId::cpu()).to_double(), 5.0);
        assert_eq!(result.get(ResourceId::gpu()).to_double(), 1.0);
    }

    #[test]
    fn test_resource_set_subtraction() {
        let mut set1 = ResourceSet::new();
        set1.set(ResourceId::cpu(), FixedPoint::from_double(5.0));

        let mut set2 = ResourceSet::new();
        set2.set(ResourceId::cpu(), FixedPoint::from_double(3.0));

        let result = set1 - set2;
        assert_eq!(result.get(ResourceId::cpu()).to_double(), 2.0);
    }

    #[test]
    fn test_resource_set_subset() {
        let mut set1 = ResourceSet::new();
        set1.set(ResourceId::cpu(), FixedPoint::from_double(2.0));

        let mut set2 = ResourceSet::new();
        set2.set(ResourceId::cpu(), FixedPoint::from_double(4.0));

        assert!(set1.is_subset_of(&set2));
        assert!(!set2.is_subset_of(&set1));
        assert!(set2.is_superset_of(&set1));
    }

    #[test]
    fn test_node_resource_set_basic() {
        let mut set = NodeResourceSet::new();
        let cpu = ResourceId::cpu();

        set.set(cpu, FixedPoint::from_double(8.0));
        assert!(set.has(cpu));
        assert_eq!(set.get(cpu).to_double(), 8.0);
    }

    #[test]
    fn test_node_resource_set_superset() {
        let mut node_set = NodeResourceSet::new();
        node_set.set(ResourceId::cpu(), FixedPoint::from_double(8.0));

        let mut resource_set = ResourceSet::new();
        resource_set.set(ResourceId::cpu(), FixedPoint::from_double(4.0));

        assert!(node_set.is_superset_of(&resource_set));

        resource_set.set(ResourceId::cpu(), FixedPoint::from_double(12.0));
        assert!(!node_set.is_superset_of(&resource_set));
    }

    #[test]
    fn test_node_resource_set_sub_assign() {
        let mut node_set = NodeResourceSet::new();
        node_set.set(ResourceId::cpu(), FixedPoint::from_double(8.0));

        let mut resource_set = ResourceSet::new();
        resource_set.set(ResourceId::cpu(), FixedPoint::from_double(3.0));

        node_set -= resource_set;
        assert_eq!(node_set.get(ResourceId::cpu()).to_double(), 5.0);
    }

    #[test]
    fn test_fixed_point_vector_conversion() {
        let doubles = vec![1.0, 2.5, 3.0];
        let fixed = fixed_point_vector_from_double(&doubles);
        let back = fixed_point_vector_to_double(&fixed);
        assert_eq!(doubles, back);
    }
}
