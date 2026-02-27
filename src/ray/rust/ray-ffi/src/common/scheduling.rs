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

//! FFI bridges for scheduling types.

use ray_common::scheduling::{
    FixedPoint, NodeResourceInstanceSet, NodeResourceSet, ResourceId, ResourceRequest,
    ResourceSet, TaskResourceInstances,
};

/// Wrapper for FixedPoint for FFI.
pub struct RustFixedPoint {
    inner: FixedPoint,
}

impl RustFixedPoint {
    pub fn new(inner: FixedPoint) -> Self {
        Self { inner }
    }

    pub fn inner(&self) -> &FixedPoint {
        &self.inner
    }
}

/// Wrapper for ResourceId for FFI.
pub struct RustResourceId {
    inner: ResourceId,
}

impl RustResourceId {
    pub fn new(inner: ResourceId) -> Self {
        Self { inner }
    }

    pub fn inner(&self) -> &ResourceId {
        &self.inner
    }
}

/// Wrapper for ResourceSet for FFI.
pub struct RustResourceSet {
    inner: ResourceSet,
}

impl RustResourceSet {
    pub fn new(inner: ResourceSet) -> Self {
        Self { inner }
    }

    pub fn inner(&self) -> &ResourceSet {
        &self.inner
    }

    pub fn inner_mut(&mut self) -> &mut ResourceSet {
        &mut self.inner
    }
}

/// Wrapper for NodeResourceSet for FFI.
pub struct RustNodeResourceSet {
    inner: NodeResourceSet,
}

impl RustNodeResourceSet {
    pub fn new(inner: NodeResourceSet) -> Self {
        Self { inner }
    }

    pub fn inner(&self) -> &NodeResourceSet {
        &self.inner
    }

    pub fn inner_mut(&mut self) -> &mut NodeResourceSet {
        &mut self.inner
    }
}

// ============================================================================
// FixedPoint FFI Functions
// ============================================================================

/// Create a FixedPoint from a double.
pub fn fixed_point_from_double(value: f64) -> Box<RustFixedPoint> {
    Box::new(RustFixedPoint::new(FixedPoint::from_double(value)))
}

/// Create a FixedPoint from an integer.
pub fn fixed_point_from_int(value: i32) -> Box<RustFixedPoint> {
    Box::new(RustFixedPoint::new(FixedPoint::from_int(value)))
}

/// Create a zero FixedPoint.
pub fn fixed_point_zero() -> Box<RustFixedPoint> {
    Box::new(RustFixedPoint::new(FixedPoint::zero()))
}

/// Convert FixedPoint to double.
pub fn fixed_point_to_double(fp: &RustFixedPoint) -> f64 {
    fp.inner.to_double()
}

/// Get the raw internal value.
pub fn fixed_point_raw(fp: &RustFixedPoint) -> i64 {
    fp.inner.raw()
}

/// Add two FixedPoints.
pub fn fixed_point_add(a: &RustFixedPoint, b: &RustFixedPoint) -> Box<RustFixedPoint> {
    Box::new(RustFixedPoint::new(*a.inner() + *b.inner()))
}

/// Subtract two FixedPoints.
pub fn fixed_point_sub(a: &RustFixedPoint, b: &RustFixedPoint) -> Box<RustFixedPoint> {
    Box::new(RustFixedPoint::new(*a.inner() - *b.inner()))
}

/// Negate a FixedPoint.
pub fn fixed_point_neg(fp: &RustFixedPoint) -> Box<RustFixedPoint> {
    Box::new(RustFixedPoint::new(-*fp.inner()))
}

/// Check if a FixedPoint is less than another.
pub fn fixed_point_lt(a: &RustFixedPoint, b: &RustFixedPoint) -> bool {
    a.inner() < b.inner()
}

/// Check if a FixedPoint is greater than another.
pub fn fixed_point_gt(a: &RustFixedPoint, b: &RustFixedPoint) -> bool {
    a.inner() > b.inner()
}

/// Check if a FixedPoint is less than or equal to another.
pub fn fixed_point_le(a: &RustFixedPoint, b: &RustFixedPoint) -> bool {
    a.inner() <= b.inner()
}

/// Check if a FixedPoint is greater than or equal to another.
pub fn fixed_point_ge(a: &RustFixedPoint, b: &RustFixedPoint) -> bool {
    a.inner() >= b.inner()
}

/// Check if two FixedPoints are equal.
pub fn fixed_point_eq(a: &RustFixedPoint, b: &RustFixedPoint) -> bool {
    a.inner() == b.inner()
}

// ============================================================================
// ResourceId FFI Functions
// ============================================================================

/// Create a nil ResourceId.
pub fn resource_id_nil() -> Box<RustResourceId> {
    Box::new(RustResourceId::new(ResourceId::nil()))
}

/// Create a ResourceId from a name.
pub fn resource_id_from_name(name: &str) -> Box<RustResourceId> {
    Box::new(RustResourceId::new(ResourceId::from_name(name)))
}

/// Create a ResourceId from an integer.
pub fn resource_id_from_int(id: i64) -> Box<RustResourceId> {
    Box::new(RustResourceId::new(ResourceId::from_int(id)))
}

/// Get the integer representation.
pub fn resource_id_to_int(id: &RustResourceId) -> i64 {
    id.inner().to_int()
}

/// Get the string representation.
pub fn resource_id_to_string(id: &RustResourceId) -> String {
    id.inner().to_string()
}

/// Check if nil.
pub fn resource_id_is_nil(id: &RustResourceId) -> bool {
    id.inner().is_nil()
}

/// Check if predefined.
pub fn resource_id_is_predefined(id: &RustResourceId) -> bool {
    id.inner().is_predefined()
}

/// Check if implicit.
pub fn resource_id_is_implicit(id: &RustResourceId) -> bool {
    id.inner().is_implicit()
}

/// Check if unit instance.
pub fn resource_id_is_unit_instance(id: &RustResourceId) -> bool {
    id.inner().is_unit_instance()
}

/// Get CPU ResourceId.
pub fn resource_id_cpu() -> Box<RustResourceId> {
    Box::new(RustResourceId::new(ResourceId::cpu()))
}

/// Get Memory ResourceId.
pub fn resource_id_memory() -> Box<RustResourceId> {
    Box::new(RustResourceId::new(ResourceId::memory()))
}

/// Get GPU ResourceId.
pub fn resource_id_gpu() -> Box<RustResourceId> {
    Box::new(RustResourceId::new(ResourceId::gpu()))
}

/// Get ObjectStoreMemory ResourceId.
pub fn resource_id_object_store_memory() -> Box<RustResourceId> {
    Box::new(RustResourceId::new(ResourceId::object_store_memory()))
}

/// Check if two ResourceIds are equal.
pub fn resource_id_eq(a: &RustResourceId, b: &RustResourceId) -> bool {
    a.inner() == b.inner()
}

// ============================================================================
// ResourceSet FFI Functions
// ============================================================================

/// Create an empty ResourceSet.
pub fn resource_set_new() -> Box<RustResourceSet> {
    Box::new(RustResourceSet::new(ResourceSet::new()))
}

/// Get a resource value.
pub fn resource_set_get(set: &RustResourceSet, id: &RustResourceId) -> Box<RustFixedPoint> {
    Box::new(RustFixedPoint::new(set.inner().get(*id.inner())))
}

/// Set a resource value.
pub fn resource_set_set(set: &mut RustResourceSet, id: &RustResourceId, value: &RustFixedPoint) {
    set.inner_mut().set(*id.inner(), *value.inner());
}

/// Check if a resource exists.
pub fn resource_set_has(set: &RustResourceSet, id: &RustResourceId) -> bool {
    set.inner().has(*id.inner())
}

/// Get the size.
pub fn resource_set_size(set: &RustResourceSet) -> usize {
    set.inner().size()
}

/// Check if empty.
pub fn resource_set_is_empty(set: &RustResourceSet) -> bool {
    set.inner().is_empty()
}

/// Clear the set.
pub fn resource_set_clear(set: &mut RustResourceSet) {
    set.inner_mut().clear();
}

/// Add two ResourceSets.
pub fn resource_set_add(a: &RustResourceSet, b: &RustResourceSet) -> Box<RustResourceSet> {
    Box::new(RustResourceSet::new(a.inner().clone() + b.inner().clone()))
}

/// Subtract two ResourceSets.
pub fn resource_set_sub(a: &RustResourceSet, b: &RustResourceSet) -> Box<RustResourceSet> {
    Box::new(RustResourceSet::new(a.inner().clone() - b.inner().clone()))
}

/// Check if two ResourceSets are equal.
pub fn resource_set_eq(a: &RustResourceSet, b: &RustResourceSet) -> bool {
    a.inner() == b.inner()
}

/// Check if a is a subset of b.
pub fn resource_set_is_subset(a: &RustResourceSet, b: &RustResourceSet) -> bool {
    a.inner().is_subset_of(b.inner())
}

/// Get the debug string.
pub fn resource_set_debug_string(set: &RustResourceSet) -> String {
    set.inner().debug_string()
}

// ============================================================================
// NodeResourceSet FFI Functions
// ============================================================================

/// Create an empty NodeResourceSet.
pub fn node_resource_set_new() -> Box<RustNodeResourceSet> {
    Box::new(RustNodeResourceSet::new(NodeResourceSet::new()))
}

/// Get a resource value.
pub fn node_resource_set_get(
    set: &RustNodeResourceSet,
    id: &RustResourceId,
) -> Box<RustFixedPoint> {
    Box::new(RustFixedPoint::new(set.inner().get(*id.inner())))
}

/// Set a resource value.
pub fn node_resource_set_set(
    set: &mut RustNodeResourceSet,
    id: &RustResourceId,
    value: &RustFixedPoint,
) {
    set.inner_mut().set(*id.inner(), *value.inner());
}

/// Check if a resource exists.
pub fn node_resource_set_has(set: &RustNodeResourceSet, id: &RustResourceId) -> bool {
    set.inner().has(*id.inner())
}

/// Check if the node set is a superset of a resource set.
pub fn node_resource_set_is_superset(
    node_set: &RustNodeResourceSet,
    resource_set: &RustResourceSet,
) -> bool {
    node_set.inner().is_superset_of(resource_set.inner())
}

/// Remove negative values.
pub fn node_resource_set_remove_negative(set: &mut RustNodeResourceSet) {
    set.inner_mut().remove_negative();
}

/// Get the debug string.
pub fn node_resource_set_debug_string(set: &RustNodeResourceSet) -> String {
    set.inner().debug_string()
}

/// Check if two NodeResourceSets are equal.
pub fn node_resource_set_eq(a: &RustNodeResourceSet, b: &RustNodeResourceSet) -> bool {
    a.inner() == b.inner()
}

// ============================================================================
// ResourceRequest Wrapper and FFI Functions
// ============================================================================

/// Wrapper for ResourceRequest for FFI.
pub struct RustResourceRequest {
    inner: ResourceRequest,
}

impl RustResourceRequest {
    pub fn new(inner: ResourceRequest) -> Self {
        Self { inner }
    }

    pub fn inner(&self) -> &ResourceRequest {
        &self.inner
    }

    pub fn inner_mut(&mut self) -> &mut ResourceRequest {
        &mut self.inner
    }
}

/// Create an empty ResourceRequest.
pub fn resource_request_new() -> Box<RustResourceRequest> {
    Box::new(RustResourceRequest::new(ResourceRequest::new()))
}

/// Get a resource value.
pub fn resource_request_get(
    req: &RustResourceRequest,
    id: &RustResourceId,
) -> Box<RustFixedPoint> {
    Box::new(RustFixedPoint::new(req.inner().get(*id.inner())))
}

/// Set a resource value.
pub fn resource_request_set(
    req: &mut RustResourceRequest,
    id: &RustResourceId,
    value: &RustFixedPoint,
) {
    req.inner_mut().set(*id.inner(), *value.inner());
}

/// Check if a resource exists.
pub fn resource_request_has(req: &RustResourceRequest, id: &RustResourceId) -> bool {
    req.inner().has(*id.inner())
}

/// Get the size.
pub fn resource_request_size(req: &RustResourceRequest) -> usize {
    req.inner().size()
}

/// Check if empty.
pub fn resource_request_is_empty(req: &RustResourceRequest) -> bool {
    req.inner().is_empty()
}

/// Clear the request.
pub fn resource_request_clear(req: &mut RustResourceRequest) {
    req.inner_mut().clear();
}

/// Check equality.
pub fn resource_request_eq(a: &RustResourceRequest, b: &RustResourceRequest) -> bool {
    a.inner() == b.inner()
}

/// Check if a <= b (less or equal comparison for scheduling).
pub fn resource_request_le(a: &RustResourceRequest, b: &RustResourceRequest) -> bool {
    a.inner().is_less_or_equal(b.inner())
}

/// Check if a >= b (greater or equal comparison for scheduling).
pub fn resource_request_ge(a: &RustResourceRequest, b: &RustResourceRequest) -> bool {
    a.inner().is_greater_or_equal(b.inner())
}

/// Add two ResourceRequests.
pub fn resource_request_add(
    a: &RustResourceRequest,
    b: &RustResourceRequest,
) -> Box<RustResourceRequest> {
    Box::new(RustResourceRequest::new(
        a.inner().clone() + b.inner().clone(),
    ))
}

/// Subtract two ResourceRequests.
pub fn resource_request_sub(
    a: &RustResourceRequest,
    b: &RustResourceRequest,
) -> Box<RustResourceRequest> {
    Box::new(RustResourceRequest::new(
        a.inner().clone() - b.inner().clone(),
    ))
}

/// Get the number of resource IDs in the request (for iteration).
pub fn resource_request_resource_id_count(req: &RustResourceRequest) -> usize {
    req.inner().size()
}

/// Get resource ID at index (for iteration). Returns as int64.
pub fn resource_request_resource_id_at(req: &RustResourceRequest, index: usize) -> i64 {
    req.inner()
        .resource_ids()
        .nth(index)
        .map(|id| id.to_int())
        .unwrap_or(-1)
}

/// Convert to a resource map as a string (for debugging/comparison).
pub fn resource_request_to_string_map(req: &RustResourceRequest) -> String {
    let map = req.inner().to_resource_map();
    let parts: Vec<String> = map.iter().map(|(k, v)| format!("{}:{}", k, v)).collect();
    parts.join(",")
}

// ============================================================================
// TaskResourceInstances Wrapper and FFI Functions
// ============================================================================

/// Wrapper for TaskResourceInstances for FFI.
pub struct RustTaskResourceInstances {
    inner: TaskResourceInstances,
}

impl RustTaskResourceInstances {
    pub fn new(inner: TaskResourceInstances) -> Self {
        Self { inner }
    }

    pub fn inner(&self) -> &TaskResourceInstances {
        &self.inner
    }

    pub fn inner_mut(&mut self) -> &mut TaskResourceInstances {
        &mut self.inner
    }
}

/// Create an empty TaskResourceInstances.
pub fn task_resource_instances_new() -> Box<RustTaskResourceInstances> {
    Box::new(RustTaskResourceInstances::new(TaskResourceInstances::new()))
}

/// Create TaskResourceInstances from a ResourceSet.
pub fn task_resource_instances_from_resource_set(
    set: &RustResourceSet,
) -> Box<RustTaskResourceInstances> {
    Box::new(RustTaskResourceInstances::new(
        TaskResourceInstances::from_resource_set(set.inner()),
    ))
}

/// Check if a resource exists.
pub fn task_resource_instances_has(
    inst: &RustTaskResourceInstances,
    id: &RustResourceId,
) -> bool {
    inst.inner().has(*id.inner())
}

/// Get the number of instances for a resource.
pub fn task_resource_instances_get_count(
    inst: &RustTaskResourceInstances,
    id: &RustResourceId,
) -> usize {
    inst.inner().get(*id.inner()).len()
}

/// Get instance value at index for a resource.
pub fn task_resource_instances_get_at(
    inst: &RustTaskResourceInstances,
    id: &RustResourceId,
    index: usize,
) -> f64 {
    let instances = inst.inner().get(*id.inner());
    instances.get(index).map(|fp| fp.to_double()).unwrap_or(0.0)
}

/// Set instances for a resource from an array of doubles.
pub fn task_resource_instances_set(
    inst: &mut RustTaskResourceInstances,
    id: &RustResourceId,
    values: &[f64],
) {
    let instances: Vec<FixedPoint> = values.iter().map(|&d| FixedPoint::from_double(d)).collect();
    inst.inner_mut().set(*id.inner(), instances);
}

/// Remove a resource.
pub fn task_resource_instances_remove(inst: &mut RustTaskResourceInstances, id: &RustResourceId) {
    inst.inner_mut().remove(*id.inner());
}

/// Get the number of resources.
pub fn task_resource_instances_size(inst: &RustTaskResourceInstances) -> usize {
    inst.inner().size()
}

/// Check if empty.
pub fn task_resource_instances_is_empty(inst: &RustTaskResourceInstances) -> bool {
    inst.inner().is_empty()
}

/// Get the sum of all instances for a resource.
pub fn task_resource_instances_sum(inst: &RustTaskResourceInstances, id: &RustResourceId) -> f64 {
    inst.inner().sum(*id.inner()).to_double()
}

/// Get the number of resource IDs (for iteration).
pub fn task_resource_instances_resource_id_count(inst: &RustTaskResourceInstances) -> usize {
    inst.inner().size()
}

/// Get resource ID at index (for iteration). Returns as int64.
pub fn task_resource_instances_resource_id_at(
    inst: &RustTaskResourceInstances,
    index: usize,
) -> i64 {
    inst.inner()
        .resource_ids()
        .nth(index)
        .map(|id| id.to_int())
        .unwrap_or(-1)
}

/// Convert to ResourceSet.
pub fn task_resource_instances_to_resource_set(
    inst: &RustTaskResourceInstances,
) -> Box<RustResourceSet> {
    Box::new(RustResourceSet::new(inst.inner().to_resource_set()))
}

/// Check equality.
pub fn task_resource_instances_eq(
    a: &RustTaskResourceInstances,
    b: &RustTaskResourceInstances,
) -> bool {
    a.inner() == b.inner()
}

// ============================================================================
// NodeResourceInstanceSet Wrapper and FFI Functions
// ============================================================================

/// Wrapper for NodeResourceInstanceSet for FFI.
pub struct RustNodeResourceInstanceSet {
    inner: NodeResourceInstanceSet,
}

impl RustNodeResourceInstanceSet {
    pub fn new(inner: NodeResourceInstanceSet) -> Self {
        Self { inner }
    }

    pub fn inner(&self) -> &NodeResourceInstanceSet {
        &self.inner
    }

    pub fn inner_mut(&mut self) -> &mut NodeResourceInstanceSet {
        &mut self.inner
    }
}

/// Create an empty NodeResourceInstanceSet.
pub fn node_resource_instance_set_new() -> Box<RustNodeResourceInstanceSet> {
    Box::new(RustNodeResourceInstanceSet::new(
        NodeResourceInstanceSet::new(),
    ))
}

/// Create NodeResourceInstanceSet from a NodeResourceSet.
pub fn node_resource_instance_set_from_node_resource_set(
    set: &RustNodeResourceSet,
) -> Box<RustNodeResourceInstanceSet> {
    Box::new(RustNodeResourceInstanceSet::new(
        NodeResourceInstanceSet::from_node_resource_set(set.inner()),
    ))
}

/// Check if a resource exists.
pub fn node_resource_instance_set_has(
    inst: &RustNodeResourceInstanceSet,
    id: &RustResourceId,
) -> bool {
    inst.inner().has(*id.inner())
}

/// Get the number of instances for a resource.
pub fn node_resource_instance_set_get_count(
    inst: &RustNodeResourceInstanceSet,
    id: &RustResourceId,
) -> usize {
    inst.inner().get(*id.inner()).len()
}

/// Get instance value at index for a resource.
pub fn node_resource_instance_set_get_at(
    inst: &RustNodeResourceInstanceSet,
    id: &RustResourceId,
    index: usize,
) -> f64 {
    let instances = inst.inner().get(*id.inner());
    instances.get(index).map(|fp| fp.to_double()).unwrap_or(0.0)
}

/// Set instances for a resource from an array of doubles.
pub fn node_resource_instance_set_set(
    inst: &mut RustNodeResourceInstanceSet,
    id: &RustResourceId,
    values: &[f64],
) {
    let instances: Vec<FixedPoint> = values.iter().map(|&d| FixedPoint::from_double(d)).collect();
    inst.inner_mut().set(*id.inner(), instances);
}

/// Remove a resource.
pub fn node_resource_instance_set_remove(
    inst: &mut RustNodeResourceInstanceSet,
    id: &RustResourceId,
) {
    inst.inner_mut().remove(*id.inner());
}

/// Get the sum of all instances for a resource.
pub fn node_resource_instance_set_sum(
    inst: &RustNodeResourceInstanceSet,
    id: &RustResourceId,
) -> f64 {
    inst.inner().sum(*id.inner())
}

/// Check equality.
pub fn node_resource_instance_set_eq(
    a: &RustNodeResourceInstanceSet,
    b: &RustNodeResourceInstanceSet,
) -> bool {
    a.inner() == b.inner()
}

/// Add to instances.
pub fn node_resource_instance_set_add(
    inst: &mut RustNodeResourceInstanceSet,
    id: &RustResourceId,
    values: &[f64],
) {
    let to_add: Vec<FixedPoint> = values.iter().map(|&d| FixedPoint::from_double(d)).collect();
    inst.inner_mut().add(*id.inner(), to_add);
}

/// Subtract from instances (allowing negative values).
pub fn node_resource_instance_set_subtract(
    inst: &mut RustNodeResourceInstanceSet,
    id: &RustResourceId,
    values: &[f64],
    allow_negative: bool,
) {
    let to_sub: Vec<FixedPoint> = values.iter().map(|&d| FixedPoint::from_double(d)).collect();
    inst.inner_mut().subtract(*id.inner(), to_sub, allow_negative);
}

/// Free resources back to the set.
pub fn node_resource_instance_set_free(
    inst: &mut RustNodeResourceInstanceSet,
    id: &RustResourceId,
    values: &[f64],
) {
    let freed: Vec<FixedPoint> = values.iter().map(|&d| FixedPoint::from_double(d)).collect();
    inst.inner_mut().free(*id.inner(), freed);
}

/// Get the number of resources stored (for iteration).
pub fn node_resource_instance_set_resource_count(inst: &RustNodeResourceInstanceSet) -> usize {
    inst.inner().resources().len()
}

/// Try to allocate resources. Returns true if successful.
pub fn node_resource_instance_set_try_allocate(
    inst: &mut RustNodeResourceInstanceSet,
    request: &RustResourceSet,
) -> bool {
    inst.inner_mut().try_allocate(request.inner()).is_some()
}

#[cxx::bridge(namespace = "ray::ffi")]
mod ffi {
    extern "Rust" {
        // FixedPoint types and functions
        type RustFixedPoint;

        fn fixed_point_from_double(value: f64) -> Box<RustFixedPoint>;
        fn fixed_point_from_int(value: i32) -> Box<RustFixedPoint>;
        fn fixed_point_zero() -> Box<RustFixedPoint>;
        fn fixed_point_to_double(fp: &RustFixedPoint) -> f64;
        fn fixed_point_raw(fp: &RustFixedPoint) -> i64;
        fn fixed_point_add(a: &RustFixedPoint, b: &RustFixedPoint) -> Box<RustFixedPoint>;
        fn fixed_point_sub(a: &RustFixedPoint, b: &RustFixedPoint) -> Box<RustFixedPoint>;
        fn fixed_point_neg(fp: &RustFixedPoint) -> Box<RustFixedPoint>;
        fn fixed_point_lt(a: &RustFixedPoint, b: &RustFixedPoint) -> bool;
        fn fixed_point_gt(a: &RustFixedPoint, b: &RustFixedPoint) -> bool;
        fn fixed_point_le(a: &RustFixedPoint, b: &RustFixedPoint) -> bool;
        fn fixed_point_ge(a: &RustFixedPoint, b: &RustFixedPoint) -> bool;
        fn fixed_point_eq(a: &RustFixedPoint, b: &RustFixedPoint) -> bool;

        // ResourceId types and functions
        type RustResourceId;

        fn resource_id_nil() -> Box<RustResourceId>;
        fn resource_id_from_name(name: &str) -> Box<RustResourceId>;
        fn resource_id_from_int(id: i64) -> Box<RustResourceId>;
        fn resource_id_to_int(id: &RustResourceId) -> i64;
        fn resource_id_to_string(id: &RustResourceId) -> String;
        fn resource_id_is_nil(id: &RustResourceId) -> bool;
        fn resource_id_is_predefined(id: &RustResourceId) -> bool;
        fn resource_id_is_implicit(id: &RustResourceId) -> bool;
        fn resource_id_is_unit_instance(id: &RustResourceId) -> bool;
        fn resource_id_cpu() -> Box<RustResourceId>;
        fn resource_id_memory() -> Box<RustResourceId>;
        fn resource_id_gpu() -> Box<RustResourceId>;
        fn resource_id_object_store_memory() -> Box<RustResourceId>;
        fn resource_id_eq(a: &RustResourceId, b: &RustResourceId) -> bool;

        // ResourceSet types and functions
        type RustResourceSet;

        fn resource_set_new() -> Box<RustResourceSet>;
        fn resource_set_get(set: &RustResourceSet, id: &RustResourceId) -> Box<RustFixedPoint>;
        fn resource_set_set(
            set: &mut RustResourceSet,
            id: &RustResourceId,
            value: &RustFixedPoint,
        );
        fn resource_set_has(set: &RustResourceSet, id: &RustResourceId) -> bool;
        fn resource_set_size(set: &RustResourceSet) -> usize;
        fn resource_set_is_empty(set: &RustResourceSet) -> bool;
        fn resource_set_clear(set: &mut RustResourceSet);
        fn resource_set_add(a: &RustResourceSet, b: &RustResourceSet) -> Box<RustResourceSet>;
        fn resource_set_sub(a: &RustResourceSet, b: &RustResourceSet) -> Box<RustResourceSet>;
        fn resource_set_eq(a: &RustResourceSet, b: &RustResourceSet) -> bool;
        fn resource_set_is_subset(a: &RustResourceSet, b: &RustResourceSet) -> bool;
        fn resource_set_debug_string(set: &RustResourceSet) -> String;

        // NodeResourceSet types and functions
        type RustNodeResourceSet;

        fn node_resource_set_new() -> Box<RustNodeResourceSet>;
        fn node_resource_set_get(
            set: &RustNodeResourceSet,
            id: &RustResourceId,
        ) -> Box<RustFixedPoint>;
        fn node_resource_set_set(
            set: &mut RustNodeResourceSet,
            id: &RustResourceId,
            value: &RustFixedPoint,
        );
        fn node_resource_set_has(set: &RustNodeResourceSet, id: &RustResourceId) -> bool;
        fn node_resource_set_is_superset(
            node_set: &RustNodeResourceSet,
            resource_set: &RustResourceSet,
        ) -> bool;
        fn node_resource_set_remove_negative(set: &mut RustNodeResourceSet);
        fn node_resource_set_debug_string(set: &RustNodeResourceSet) -> String;
        fn node_resource_set_eq(a: &RustNodeResourceSet, b: &RustNodeResourceSet) -> bool;

        // ResourceRequest types and functions
        type RustResourceRequest;

        fn resource_request_new() -> Box<RustResourceRequest>;
        fn resource_request_get(
            req: &RustResourceRequest,
            id: &RustResourceId,
        ) -> Box<RustFixedPoint>;
        fn resource_request_set(
            req: &mut RustResourceRequest,
            id: &RustResourceId,
            value: &RustFixedPoint,
        );
        fn resource_request_has(req: &RustResourceRequest, id: &RustResourceId) -> bool;
        fn resource_request_size(req: &RustResourceRequest) -> usize;
        fn resource_request_is_empty(req: &RustResourceRequest) -> bool;
        fn resource_request_clear(req: &mut RustResourceRequest);
        fn resource_request_eq(a: &RustResourceRequest, b: &RustResourceRequest) -> bool;
        fn resource_request_le(a: &RustResourceRequest, b: &RustResourceRequest) -> bool;
        fn resource_request_ge(a: &RustResourceRequest, b: &RustResourceRequest) -> bool;
        fn resource_request_add(
            a: &RustResourceRequest,
            b: &RustResourceRequest,
        ) -> Box<RustResourceRequest>;
        fn resource_request_sub(
            a: &RustResourceRequest,
            b: &RustResourceRequest,
        ) -> Box<RustResourceRequest>;
        fn resource_request_resource_id_count(req: &RustResourceRequest) -> usize;
        fn resource_request_resource_id_at(req: &RustResourceRequest, index: usize) -> i64;
        fn resource_request_to_string_map(req: &RustResourceRequest) -> String;

        // TaskResourceInstances types and functions
        type RustTaskResourceInstances;

        fn task_resource_instances_new() -> Box<RustTaskResourceInstances>;
        fn task_resource_instances_from_resource_set(
            set: &RustResourceSet,
        ) -> Box<RustTaskResourceInstances>;
        fn task_resource_instances_has(
            inst: &RustTaskResourceInstances,
            id: &RustResourceId,
        ) -> bool;
        fn task_resource_instances_get_count(
            inst: &RustTaskResourceInstances,
            id: &RustResourceId,
        ) -> usize;
        fn task_resource_instances_get_at(
            inst: &RustTaskResourceInstances,
            id: &RustResourceId,
            index: usize,
        ) -> f64;
        fn task_resource_instances_set(
            inst: &mut RustTaskResourceInstances,
            id: &RustResourceId,
            values: &[f64],
        );
        fn task_resource_instances_remove(
            inst: &mut RustTaskResourceInstances,
            id: &RustResourceId,
        );
        fn task_resource_instances_size(inst: &RustTaskResourceInstances) -> usize;
        fn task_resource_instances_is_empty(inst: &RustTaskResourceInstances) -> bool;
        fn task_resource_instances_sum(
            inst: &RustTaskResourceInstances,
            id: &RustResourceId,
        ) -> f64;
        fn task_resource_instances_resource_id_count(inst: &RustTaskResourceInstances) -> usize;
        fn task_resource_instances_resource_id_at(
            inst: &RustTaskResourceInstances,
            index: usize,
        ) -> i64;
        fn task_resource_instances_to_resource_set(
            inst: &RustTaskResourceInstances,
        ) -> Box<RustResourceSet>;
        fn task_resource_instances_eq(
            a: &RustTaskResourceInstances,
            b: &RustTaskResourceInstances,
        ) -> bool;

        // NodeResourceInstanceSet types and functions
        type RustNodeResourceInstanceSet;

        fn node_resource_instance_set_new() -> Box<RustNodeResourceInstanceSet>;
        fn node_resource_instance_set_from_node_resource_set(
            set: &RustNodeResourceSet,
        ) -> Box<RustNodeResourceInstanceSet>;
        fn node_resource_instance_set_has(
            inst: &RustNodeResourceInstanceSet,
            id: &RustResourceId,
        ) -> bool;
        fn node_resource_instance_set_get_count(
            inst: &RustNodeResourceInstanceSet,
            id: &RustResourceId,
        ) -> usize;
        fn node_resource_instance_set_get_at(
            inst: &RustNodeResourceInstanceSet,
            id: &RustResourceId,
            index: usize,
        ) -> f64;
        fn node_resource_instance_set_set(
            inst: &mut RustNodeResourceInstanceSet,
            id: &RustResourceId,
            values: &[f64],
        );
        fn node_resource_instance_set_remove(
            inst: &mut RustNodeResourceInstanceSet,
            id: &RustResourceId,
        );
        fn node_resource_instance_set_sum(
            inst: &RustNodeResourceInstanceSet,
            id: &RustResourceId,
        ) -> f64;
        fn node_resource_instance_set_eq(
            a: &RustNodeResourceInstanceSet,
            b: &RustNodeResourceInstanceSet,
        ) -> bool;
        fn node_resource_instance_set_add(
            inst: &mut RustNodeResourceInstanceSet,
            id: &RustResourceId,
            values: &[f64],
        );
        fn node_resource_instance_set_subtract(
            inst: &mut RustNodeResourceInstanceSet,
            id: &RustResourceId,
            values: &[f64],
            allow_negative: bool,
        );
        fn node_resource_instance_set_free(
            inst: &mut RustNodeResourceInstanceSet,
            id: &RustResourceId,
            values: &[f64],
        );
        fn node_resource_instance_set_resource_count(inst: &RustNodeResourceInstanceSet) -> usize;
        fn node_resource_instance_set_try_allocate(
            inst: &mut RustNodeResourceInstanceSet,
            request: &RustResourceSet,
        ) -> bool;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fixed_point_ffi() {
        let fp1 = fixed_point_from_double(1.5);
        let fp2 = fixed_point_from_double(2.5);

        assert_eq!(fixed_point_to_double(&fp1), 1.5);
        assert_eq!(fixed_point_to_double(&fp2), 2.5);

        let sum = fixed_point_add(&fp1, &fp2);
        assert_eq!(fixed_point_to_double(&sum), 4.0);

        let diff = fixed_point_sub(&fp2, &fp1);
        assert_eq!(fixed_point_to_double(&diff), 1.0);

        assert!(fixed_point_lt(&fp1, &fp2));
        assert!(fixed_point_gt(&fp2, &fp1));
        assert!(fixed_point_eq(&fp1, &fp1));
    }

    #[test]
    fn test_resource_id_ffi() {
        let cpu = resource_id_cpu();
        assert!(resource_id_is_predefined(&cpu));
        assert_eq!(resource_id_to_string(&cpu), "CPU");

        let custom = resource_id_from_name("custom_resource");
        assert!(!resource_id_is_predefined(&custom));
        assert_eq!(resource_id_to_string(&custom), "custom_resource");

        let nil = resource_id_nil();
        assert!(resource_id_is_nil(&nil));
    }

    #[test]
    fn test_resource_set_ffi() {
        let mut set = resource_set_new();
        assert!(resource_set_is_empty(&set));

        let cpu = resource_id_cpu();
        let value = fixed_point_from_double(4.0);
        resource_set_set(&mut set, &cpu, &value);

        assert!(!resource_set_is_empty(&set));
        assert_eq!(resource_set_size(&set), 1);
        assert!(resource_set_has(&set, &cpu));

        let retrieved = resource_set_get(&set, &cpu);
        assert_eq!(fixed_point_to_double(&retrieved), 4.0);
    }

    #[test]
    fn test_node_resource_set_ffi() {
        let mut node_set = node_resource_set_new();
        let cpu = resource_id_cpu();
        let value = fixed_point_from_double(8.0);
        node_resource_set_set(&mut node_set, &cpu, &value);

        let retrieved = node_resource_set_get(&node_set, &cpu);
        assert_eq!(fixed_point_to_double(&retrieved), 8.0);

        let mut resource_set = resource_set_new();
        let request_value = fixed_point_from_double(4.0);
        resource_set_set(&mut resource_set, &cpu, &request_value);

        assert!(node_resource_set_is_superset(&node_set, &resource_set));
    }
}
