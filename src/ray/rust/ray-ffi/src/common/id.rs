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

//! ID types FFI bridge for C++ interop.
//!
//! This module provides CXX bridges that expose the Rust ID type
//! implementations to C++ code.

use ray_common::id::{ActorId, JobId, ObjectId, RayId, TaskId, UniqueId};

/// Wrapper for JobId exposed via FFI.
pub struct RustJobId {
    inner: JobId,
}

impl RustJobId {
    pub fn new(inner: JobId) -> Self {
        Self { inner }
    }

    pub fn inner(&self) -> &JobId {
        &self.inner
    }
}

/// Wrapper for ActorId exposed via FFI.
pub struct RustActorId {
    inner: ActorId,
}

impl RustActorId {
    pub fn new(inner: ActorId) -> Self {
        Self { inner }
    }

    pub fn inner(&self) -> &ActorId {
        &self.inner
    }
}

/// Wrapper for TaskId exposed via FFI.
pub struct RustTaskId {
    inner: TaskId,
}

impl RustTaskId {
    pub fn new(inner: TaskId) -> Self {
        Self { inner }
    }

    pub fn inner(&self) -> &TaskId {
        &self.inner
    }
}

/// Wrapper for ObjectId exposed via FFI.
pub struct RustObjectId {
    inner: ObjectId,
}

impl RustObjectId {
    pub fn new(inner: ObjectId) -> Self {
        Self { inner }
    }

    pub fn inner(&self) -> &ObjectId {
        &self.inner
    }
}

/// Wrapper for UniqueId exposed via FFI.
pub struct RustUniqueId {
    inner: UniqueId,
}

impl RustUniqueId {
    pub fn new(inner: UniqueId) -> Self {
        Self { inner }
    }

    pub fn inner(&self) -> &UniqueId {
        &self.inner
    }
}

#[cxx::bridge(namespace = "ray::ffi")]
mod ffi {
    extern "Rust" {
        // JobId
        type RustJobId;

        fn job_id_from_int(id: u32) -> Box<RustJobId>;
        fn job_id_from_binary(data: &[u8]) -> Result<Box<RustJobId>>;
        fn job_id_from_hex(hex: &str) -> Result<Box<RustJobId>>;
        fn job_id_nil() -> Box<RustJobId>;
        fn job_id_to_int(id: &RustJobId) -> u32;
        fn job_id_to_hex(id: &RustJobId) -> String;
        fn job_id_to_binary(id: &RustJobId) -> Vec<u8>;
        fn job_id_is_nil(id: &RustJobId) -> bool;
        fn job_id_hash(id: &RustJobId) -> u64;
        fn job_id_eq(a: &RustJobId, b: &RustJobId) -> bool;
        fn job_id_clone(id: &RustJobId) -> Box<RustJobId>;

        // ActorId
        type RustActorId;

        fn actor_id_from_binary(data: &[u8]) -> Result<Box<RustActorId>>;
        fn actor_id_from_hex(hex: &str) -> Result<Box<RustActorId>>;
        fn actor_id_nil() -> Box<RustActorId>;
        fn actor_id_nil_from_job(job_id: &RustJobId) -> Box<RustActorId>;
        fn actor_id_to_hex(id: &RustActorId) -> String;
        fn actor_id_to_binary(id: &RustActorId) -> Vec<u8>;
        fn actor_id_is_nil(id: &RustActorId) -> bool;
        fn actor_id_job_id(id: &RustActorId) -> Box<RustJobId>;
        fn actor_id_hash(id: &RustActorId) -> u64;
        fn actor_id_eq(a: &RustActorId, b: &RustActorId) -> bool;
        fn actor_id_clone(id: &RustActorId) -> Box<RustActorId>;

        // TaskId
        type RustTaskId;

        fn task_id_from_binary(data: &[u8]) -> Result<Box<RustTaskId>>;
        fn task_id_from_hex(hex: &str) -> Result<Box<RustTaskId>>;
        fn task_id_from_random(job_id: &RustJobId) -> Box<RustTaskId>;
        fn task_id_nil() -> Box<RustTaskId>;
        fn task_id_for_actor_creation_task(actor_id: &RustActorId) -> Box<RustTaskId>;
        fn task_id_to_hex(id: &RustTaskId) -> String;
        fn task_id_to_binary(id: &RustTaskId) -> Vec<u8>;
        fn task_id_is_nil(id: &RustTaskId) -> bool;
        fn task_id_is_for_actor_creation_task(id: &RustTaskId) -> bool;
        fn task_id_actor_id(id: &RustTaskId) -> Box<RustActorId>;
        fn task_id_job_id(id: &RustTaskId) -> Box<RustJobId>;
        fn task_id_hash(id: &RustTaskId) -> u64;
        fn task_id_eq(a: &RustTaskId, b: &RustTaskId) -> bool;
        fn task_id_clone(id: &RustTaskId) -> Box<RustTaskId>;

        // ObjectId
        type RustObjectId;

        fn object_id_from_binary(data: &[u8]) -> Result<Box<RustObjectId>>;
        fn object_id_from_hex(hex: &str) -> Result<Box<RustObjectId>>;
        fn object_id_from_index(task_id: &RustTaskId, index: u32) -> Box<RustObjectId>;
        fn object_id_from_random() -> Box<RustObjectId>;
        fn object_id_nil() -> Box<RustObjectId>;
        fn object_id_to_hex(id: &RustObjectId) -> String;
        fn object_id_to_binary(id: &RustObjectId) -> Vec<u8>;
        fn object_id_is_nil(id: &RustObjectId) -> bool;
        fn object_id_object_index(id: &RustObjectId) -> u32;
        fn object_id_task_id(id: &RustObjectId) -> Box<RustTaskId>;
        fn object_id_hash(id: &RustObjectId) -> u64;
        fn object_id_eq(a: &RustObjectId, b: &RustObjectId) -> bool;
        fn object_id_clone(id: &RustObjectId) -> Box<RustObjectId>;

        // UniqueId
        type RustUniqueId;

        fn unique_id_from_binary(data: &[u8]) -> Result<Box<RustUniqueId>>;
        fn unique_id_from_hex(hex: &str) -> Result<Box<RustUniqueId>>;
        fn unique_id_from_random() -> Box<RustUniqueId>;
        fn unique_id_nil() -> Box<RustUniqueId>;
        fn unique_id_to_hex(id: &RustUniqueId) -> String;
        fn unique_id_to_binary(id: &RustUniqueId) -> Vec<u8>;
        fn unique_id_is_nil(id: &RustUniqueId) -> bool;
        fn unique_id_hash(id: &RustUniqueId) -> u64;
        fn unique_id_eq(a: &RustUniqueId, b: &RustUniqueId) -> bool;
        fn unique_id_clone(id: &RustUniqueId) -> Box<RustUniqueId>;
    }
}

// JobId FFI implementations

fn job_id_from_int(id: u32) -> Box<RustJobId> {
    Box::new(RustJobId::new(JobId::from_int(id)))
}

fn job_id_from_binary(data: &[u8]) -> Result<Box<RustJobId>, String> {
    JobId::from_binary(data)
        .map(|id| Box::new(RustJobId::new(id)))
        .ok_or_else(|| format!("Invalid binary length for JobId: expected 4, got {}", data.len()))
}

fn job_id_from_hex(hex: &str) -> Result<Box<RustJobId>, String> {
    JobId::from_hex(hex)
        .map(|id| Box::new(RustJobId::new(id)))
        .ok_or_else(|| format!("Invalid hex string for JobId: {}", hex))
}

fn job_id_nil() -> Box<RustJobId> {
    Box::new(RustJobId::new(JobId::nil()))
}

fn job_id_to_int(id: &RustJobId) -> u32 {
    id.inner.to_int()
}

fn job_id_to_hex(id: &RustJobId) -> String {
    id.inner.to_hex()
}

fn job_id_to_binary(id: &RustJobId) -> Vec<u8> {
    id.inner.to_binary()
}

fn job_id_is_nil(id: &RustJobId) -> bool {
    id.inner.is_nil()
}

fn job_id_hash(id: &RustJobId) -> u64 {
    id.inner.compute_hash()
}

fn job_id_eq(a: &RustJobId, b: &RustJobId) -> bool {
    a.inner == b.inner
}

fn job_id_clone(id: &RustJobId) -> Box<RustJobId> {
    Box::new(RustJobId::new(id.inner.clone()))
}

// ActorId FFI implementations

fn actor_id_from_binary(data: &[u8]) -> Result<Box<RustActorId>, String> {
    ActorId::from_binary(data)
        .map(|id| Box::new(RustActorId::new(id)))
        .ok_or_else(|| format!("Invalid binary length for ActorId: expected 16, got {}", data.len()))
}

fn actor_id_from_hex(hex: &str) -> Result<Box<RustActorId>, String> {
    ActorId::from_hex(hex)
        .map(|id| Box::new(RustActorId::new(id)))
        .ok_or_else(|| format!("Invalid hex string for ActorId: {}", hex))
}

fn actor_id_nil() -> Box<RustActorId> {
    Box::new(RustActorId::new(ActorId::nil()))
}

fn actor_id_nil_from_job(job_id: &RustJobId) -> Box<RustActorId> {
    Box::new(RustActorId::new(ActorId::nil_from_job(&job_id.inner)))
}

fn actor_id_to_hex(id: &RustActorId) -> String {
    id.inner.to_hex()
}

fn actor_id_to_binary(id: &RustActorId) -> Vec<u8> {
    id.inner.to_binary()
}

fn actor_id_is_nil(id: &RustActorId) -> bool {
    id.inner.is_nil()
}

fn actor_id_job_id(id: &RustActorId) -> Box<RustJobId> {
    Box::new(RustJobId::new(id.inner.job_id()))
}

fn actor_id_hash(id: &RustActorId) -> u64 {
    id.inner.compute_hash()
}

fn actor_id_eq(a: &RustActorId, b: &RustActorId) -> bool {
    a.inner == b.inner
}

fn actor_id_clone(id: &RustActorId) -> Box<RustActorId> {
    Box::new(RustActorId::new(id.inner.clone()))
}

// TaskId FFI implementations

fn task_id_from_binary(data: &[u8]) -> Result<Box<RustTaskId>, String> {
    TaskId::from_binary(data)
        .map(|id| Box::new(RustTaskId::new(id)))
        .ok_or_else(|| format!("Invalid binary length for TaskId: expected 24, got {}", data.len()))
}

fn task_id_from_hex(hex: &str) -> Result<Box<RustTaskId>, String> {
    TaskId::from_hex(hex)
        .map(|id| Box::new(RustTaskId::new(id)))
        .ok_or_else(|| format!("Invalid hex string for TaskId: {}", hex))
}

fn task_id_from_random(job_id: &RustJobId) -> Box<RustTaskId> {
    Box::new(RustTaskId::new(TaskId::from_random(&job_id.inner)))
}

fn task_id_nil() -> Box<RustTaskId> {
    Box::new(RustTaskId::new(TaskId::nil()))
}

fn task_id_for_actor_creation_task(actor_id: &RustActorId) -> Box<RustTaskId> {
    Box::new(RustTaskId::new(TaskId::for_actor_creation_task(&actor_id.inner)))
}

fn task_id_to_hex(id: &RustTaskId) -> String {
    id.inner.to_hex()
}

fn task_id_to_binary(id: &RustTaskId) -> Vec<u8> {
    id.inner.to_binary()
}

fn task_id_is_nil(id: &RustTaskId) -> bool {
    id.inner.is_nil()
}

fn task_id_is_for_actor_creation_task(id: &RustTaskId) -> bool {
    id.inner.is_for_actor_creation_task()
}

fn task_id_actor_id(id: &RustTaskId) -> Box<RustActorId> {
    Box::new(RustActorId::new(id.inner.actor_id()))
}

fn task_id_job_id(id: &RustTaskId) -> Box<RustJobId> {
    Box::new(RustJobId::new(id.inner.job_id()))
}

fn task_id_hash(id: &RustTaskId) -> u64 {
    id.inner.compute_hash()
}

fn task_id_eq(a: &RustTaskId, b: &RustTaskId) -> bool {
    a.inner == b.inner
}

fn task_id_clone(id: &RustTaskId) -> Box<RustTaskId> {
    Box::new(RustTaskId::new(id.inner.clone()))
}

// ObjectId FFI implementations

fn object_id_from_binary(data: &[u8]) -> Result<Box<RustObjectId>, String> {
    ObjectId::from_binary(data)
        .map(|id| Box::new(RustObjectId::new(id)))
        .ok_or_else(|| format!("Invalid binary length for ObjectId: expected 28, got {}", data.len()))
}

fn object_id_from_hex(hex: &str) -> Result<Box<RustObjectId>, String> {
    ObjectId::from_hex(hex)
        .map(|id| Box::new(RustObjectId::new(id)))
        .ok_or_else(|| format!("Invalid hex string for ObjectId: {}", hex))
}

fn object_id_from_index(task_id: &RustTaskId, index: u32) -> Box<RustObjectId> {
    Box::new(RustObjectId::new(ObjectId::from_index(&task_id.inner, index)))
}

fn object_id_from_random() -> Box<RustObjectId> {
    Box::new(RustObjectId::new(ObjectId::from_random()))
}

fn object_id_nil() -> Box<RustObjectId> {
    Box::new(RustObjectId::new(ObjectId::nil()))
}

fn object_id_to_hex(id: &RustObjectId) -> String {
    id.inner.to_hex()
}

fn object_id_to_binary(id: &RustObjectId) -> Vec<u8> {
    id.inner.to_binary()
}

fn object_id_is_nil(id: &RustObjectId) -> bool {
    id.inner.is_nil()
}

fn object_id_object_index(id: &RustObjectId) -> u32 {
    id.inner.object_index()
}

fn object_id_task_id(id: &RustObjectId) -> Box<RustTaskId> {
    Box::new(RustTaskId::new(id.inner.task_id()))
}

fn object_id_hash(id: &RustObjectId) -> u64 {
    id.inner.compute_hash()
}

fn object_id_eq(a: &RustObjectId, b: &RustObjectId) -> bool {
    a.inner == b.inner
}

fn object_id_clone(id: &RustObjectId) -> Box<RustObjectId> {
    Box::new(RustObjectId::new(id.inner.clone()))
}

// UniqueId FFI implementations

fn unique_id_from_binary(data: &[u8]) -> Result<Box<RustUniqueId>, String> {
    UniqueId::from_binary(data)
        .map(|id| Box::new(RustUniqueId::new(id)))
        .ok_or_else(|| format!("Invalid binary length for UniqueId: expected 28, got {}", data.len()))
}

fn unique_id_from_hex(hex: &str) -> Result<Box<RustUniqueId>, String> {
    UniqueId::from_hex(hex)
        .map(|id| Box::new(RustUniqueId::new(id)))
        .ok_or_else(|| format!("Invalid hex string for UniqueId: {}", hex))
}

fn unique_id_from_random() -> Box<RustUniqueId> {
    Box::new(RustUniqueId::new(UniqueId::from_random()))
}

fn unique_id_nil() -> Box<RustUniqueId> {
    Box::new(RustUniqueId::new(UniqueId::nil()))
}

fn unique_id_to_hex(id: &RustUniqueId) -> String {
    id.inner.to_hex()
}

fn unique_id_to_binary(id: &RustUniqueId) -> Vec<u8> {
    id.inner.to_binary()
}

fn unique_id_is_nil(id: &RustUniqueId) -> bool {
    id.inner.is_nil()
}

fn unique_id_hash(id: &RustUniqueId) -> u64 {
    id.inner.compute_hash()
}

fn unique_id_eq(a: &RustUniqueId, b: &RustUniqueId) -> bool {
    a.inner == b.inner
}

fn unique_id_clone(id: &RustUniqueId) -> Box<RustUniqueId> {
    Box::new(RustUniqueId::new(id.inner.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_job_id_roundtrip() {
        let id = job_id_from_int(12345);
        assert_eq!(job_id_to_int(&id), 12345);
    }

    #[test]
    fn test_job_id_hex_roundtrip() {
        let id = job_id_from_int(0x12345678);
        let hex = job_id_to_hex(&id);
        let restored = job_id_from_hex(&hex).unwrap();
        assert!(job_id_eq(&id, &restored));
    }

    #[test]
    fn test_job_id_nil() {
        let nil = job_id_nil();
        assert!(job_id_is_nil(&nil));

        let non_nil = job_id_from_int(1);
        assert!(!job_id_is_nil(&non_nil));
    }

    #[test]
    fn test_actor_id_job_id() {
        let job_id = job_id_from_int(42);
        let actor_id = actor_id_nil_from_job(&job_id);
        let extracted_job_id = actor_id_job_id(&actor_id);
        assert!(job_id_eq(&job_id, &extracted_job_id));
    }

    #[test]
    fn test_task_id_actor_creation() {
        let job_id = job_id_from_int(1);
        let actor_id = actor_id_nil_from_job(&job_id);
        let task_id = task_id_for_actor_creation_task(&actor_id);
        assert!(task_id_is_for_actor_creation_task(&task_id));
    }

    #[test]
    fn test_object_id_from_index() {
        let job_id = job_id_from_int(1);
        let task_id = task_id_from_random(&job_id);
        let object_id = object_id_from_index(&task_id, 5);

        assert_eq!(object_id_object_index(&object_id), 5);
        let extracted_task_id = object_id_task_id(&object_id);
        assert!(task_id_eq(&task_id, &extracted_task_id));
    }

    #[test]
    fn test_unique_id_random() {
        let id1 = unique_id_from_random();
        let id2 = unique_id_from_random();
        assert!(!unique_id_eq(&id1, &id2));
    }
}
