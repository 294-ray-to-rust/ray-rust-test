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

//! ID types for Ray entities.
//!
//! This module provides Rust equivalents of Ray's C++ ID types,
//! with fixed-size byte arrays matching the C++ implementations.

use rand::Rng;
use std::fmt;
use std::hash::{Hash, Hasher};

/// Length of UniqueID in bytes (matches kUniqueIDSize in constants.h).
pub const UNIQUE_ID_SIZE: usize = 28;

/// Length of JobID in bytes.
pub const JOB_ID_SIZE: usize = 4;

/// Length of ActorID unique bytes.
const ACTOR_ID_UNIQUE_BYTES: usize = 12;

/// Length of ActorID in bytes (unique bytes + JobID).
pub const ACTOR_ID_SIZE: usize = ACTOR_ID_UNIQUE_BYTES + JOB_ID_SIZE;

/// Length of TaskID unique bytes.
const TASK_ID_UNIQUE_BYTES: usize = 8;

/// Length of TaskID in bytes (unique bytes + ActorID).
pub const TASK_ID_SIZE: usize = TASK_ID_UNIQUE_BYTES + ACTOR_ID_SIZE;

/// Length of ObjectID index bytes.
const OBJECT_ID_INDEX_BYTES: usize = 4;

/// Length of ObjectID in bytes (index bytes + TaskID).
pub const OBJECT_ID_SIZE: usize = OBJECT_ID_INDEX_BYTES + TASK_ID_SIZE;

/// Length of PlacementGroupID unique bytes.
const PLACEMENT_GROUP_ID_UNIQUE_BYTES: usize = 14;

/// Length of PlacementGroupID in bytes (unique bytes + JobID).
pub const PLACEMENT_GROUP_ID_SIZE: usize = PLACEMENT_GROUP_ID_UNIQUE_BYTES + JOB_ID_SIZE;

/// Length of LeaseID unique bytes.
const LEASE_ID_UNIQUE_BYTES: usize = 4;

/// Length of LeaseID in bytes (unique bytes + WorkerID/UniqueID).
pub const LEASE_ID_SIZE: usize = LEASE_ID_UNIQUE_BYTES + UNIQUE_ID_SIZE;

/// MurmurHash64A implementation matching the C++ version.
fn murmur_hash_64a(data: &[u8], seed: u64) -> u64 {
    const M: u64 = 0xc6a4a7935bd1e995;
    const R: i32 = 47;

    let len = data.len();
    let mut h: u64 = seed ^ ((len as u64).wrapping_mul(M));

    let n_blocks = len / 8;
    for i in 0..n_blocks {
        let mut k = u64::from_le_bytes([
            data[i * 8],
            data[i * 8 + 1],
            data[i * 8 + 2],
            data[i * 8 + 3],
            data[i * 8 + 4],
            data[i * 8 + 5],
            data[i * 8 + 6],
            data[i * 8 + 7],
        ]);

        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);

        h ^= k;
        h = h.wrapping_mul(M);
    }

    let tail = &data[n_blocks * 8..];
    let tail_len = tail.len();
    if tail_len > 0 {
        let mut k: u64 = 0;
        for i in (0..tail_len).rev() {
            k = (k << 8) | (tail[i] as u64);
        }
        h ^= k;
        h = h.wrapping_mul(M);
    }

    h ^= h >> R;
    h = h.wrapping_mul(M);
    h ^= h >> R;

    h
}

/// Trait for Ray ID types.
pub trait RayId: Sized + Clone + PartialEq + Eq + Hash {
    /// The size of this ID type in bytes.
    const SIZE: usize;

    /// Returns the ID data as a byte slice.
    fn data(&self) -> &[u8];

    /// Returns the ID data as a mutable byte slice.
    fn data_mut(&mut self) -> &mut [u8];

    /// Create an ID from binary data.
    fn from_binary(data: &[u8]) -> Option<Self>;

    /// Create a nil ID (all 0xFF bytes).
    fn nil() -> Self;

    /// Check if this ID is nil.
    fn is_nil(&self) -> bool {
        self.data().iter().all(|&b| b == 0xFF)
    }

    /// Convert to binary representation.
    fn to_binary(&self) -> Vec<u8> {
        self.data().to_vec()
    }

    /// Convert to hexadecimal string.
    fn to_hex(&self) -> String {
        hex::encode(self.data())
    }

    /// Create an ID from a hexadecimal string.
    fn from_hex(hex_str: &str) -> Option<Self> {
        let bytes = hex::decode(hex_str).ok()?;
        Self::from_binary(&bytes)
    }

    /// Compute a hash of this ID.
    fn compute_hash(&self) -> u64 {
        murmur_hash_64a(self.data(), 0)
    }
}

/// A 28-byte unique identifier (matches C++ UniqueID).
#[derive(Clone)]
pub struct UniqueId {
    data: [u8; UNIQUE_ID_SIZE],
    hash: u64,
}

impl UniqueId {
    /// Create a new UniqueId with the given data.
    pub fn new(data: [u8; UNIQUE_ID_SIZE]) -> Self {
        let hash = murmur_hash_64a(&data, 0);
        Self { data, hash }
    }

    /// Generate a random UniqueId.
    pub fn from_random() -> Self {
        let mut data = [0u8; UNIQUE_ID_SIZE];
        rand::thread_rng().fill(&mut data);
        Self::new(data)
    }
}

impl RayId for UniqueId {
    const SIZE: usize = UNIQUE_ID_SIZE;

    fn data(&self) -> &[u8] {
        &self.data
    }

    fn data_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }

    fn from_binary(data: &[u8]) -> Option<Self> {
        if data.len() != UNIQUE_ID_SIZE {
            return None;
        }
        let mut arr = [0u8; UNIQUE_ID_SIZE];
        arr.copy_from_slice(data);
        Some(Self::new(arr))
    }

    fn nil() -> Self {
        Self::new([0xFF; UNIQUE_ID_SIZE])
    }
}

impl PartialEq for UniqueId {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl Eq for UniqueId {}

impl Hash for UniqueId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash);
    }
}

impl fmt::Debug for UniqueId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "UniqueId({})", self.to_hex())
    }
}

impl fmt::Display for UniqueId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

/// A 4-byte job identifier (matches C++ JobID).
#[derive(Clone)]
pub struct JobId {
    data: [u8; JOB_ID_SIZE],
    hash: u64,
}

impl JobId {
    /// Create a new JobId with the given data.
    pub fn new(data: [u8; JOB_ID_SIZE]) -> Self {
        let hash = murmur_hash_64a(&data, 0);
        Self { data, hash }
    }

    /// Create a JobId from an integer value.
    pub fn from_int(value: u32) -> Self {
        Self::new(value.to_be_bytes())
    }

    /// Convert the JobId to an integer value.
    pub fn to_int(&self) -> u32 {
        u32::from_be_bytes(self.data)
    }
}

impl RayId for JobId {
    const SIZE: usize = JOB_ID_SIZE;

    fn data(&self) -> &[u8] {
        &self.data
    }

    fn data_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }

    fn from_binary(data: &[u8]) -> Option<Self> {
        if data.len() != JOB_ID_SIZE {
            return None;
        }
        let mut arr = [0u8; JOB_ID_SIZE];
        arr.copy_from_slice(data);
        Some(Self::new(arr))
    }

    fn nil() -> Self {
        Self::new([0xFF; JOB_ID_SIZE])
    }
}

impl PartialEq for JobId {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl Eq for JobId {}

impl Hash for JobId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash);
    }
}

impl fmt::Debug for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "JobId({})", self.to_hex())
    }
}

impl fmt::Display for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

/// A 16-byte actor identifier (matches C++ ActorID).
#[derive(Clone)]
pub struct ActorId {
    data: [u8; ACTOR_ID_SIZE],
    hash: u64,
}

impl ActorId {
    /// Create a new ActorId with the given data.
    pub fn new(data: [u8; ACTOR_ID_SIZE]) -> Self {
        let hash = murmur_hash_64a(&data, 0);
        Self { data, hash }
    }

    /// Get the JobId embedded in this ActorId.
    pub fn job_id(&self) -> JobId {
        let mut job_data = [0u8; JOB_ID_SIZE];
        job_data.copy_from_slice(&self.data[ACTOR_ID_UNIQUE_BYTES..]);
        JobId::new(job_data)
    }

    /// Create a nil ActorId for a specific job.
    pub fn nil_from_job(job_id: &JobId) -> Self {
        let mut data = [0xFF; ACTOR_ID_SIZE];
        data[ACTOR_ID_UNIQUE_BYTES..].copy_from_slice(job_id.data());
        Self::new(data)
    }

    /// Create an ActorId by hashing job_id, parent_task_id, and counter.
    /// This matches the C++ ActorID::Of method.
    pub fn of(job_id: &JobId, parent_task_id: &TaskId, parent_task_counter: usize) -> Self {
        let mut data = [0u8; ACTOR_ID_SIZE];

        // Hash the parent task ID and counter to create unique bytes
        let mut hash_input = Vec::with_capacity(TASK_ID_SIZE + 8);
        hash_input.extend_from_slice(parent_task_id.data());
        hash_input.extend_from_slice(&(parent_task_counter as u64).to_le_bytes());

        let hash = murmur_hash_64a(&hash_input, 0);
        let hash_bytes = hash.to_le_bytes();

        // Fill unique bytes with hash
        data[..8].copy_from_slice(&hash_bytes);
        // Fill remaining unique bytes with more hashing
        let hash2 = murmur_hash_64a(&hash_input, 1);
        data[8..ACTOR_ID_UNIQUE_BYTES].copy_from_slice(&hash2.to_le_bytes()[..4]);

        // Append job ID
        data[ACTOR_ID_UNIQUE_BYTES..].copy_from_slice(job_id.data());

        Self::new(data)
    }
}

impl RayId for ActorId {
    const SIZE: usize = ACTOR_ID_SIZE;

    fn data(&self) -> &[u8] {
        &self.data
    }

    fn data_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }

    fn from_binary(data: &[u8]) -> Option<Self> {
        if data.len() != ACTOR_ID_SIZE {
            return None;
        }
        let mut arr = [0u8; ACTOR_ID_SIZE];
        arr.copy_from_slice(data);
        Some(Self::new(arr))
    }

    fn nil() -> Self {
        Self::new([0xFF; ACTOR_ID_SIZE])
    }
}

impl PartialEq for ActorId {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl Eq for ActorId {}

impl Hash for ActorId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash);
    }
}

impl fmt::Debug for ActorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ActorId({})", self.to_hex())
    }
}

impl fmt::Display for ActorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

/// A 24-byte task identifier (matches C++ TaskID).
#[derive(Clone)]
pub struct TaskId {
    data: [u8; TASK_ID_SIZE],
    hash: u64,
}

impl TaskId {
    /// Create a new TaskId with the given data.
    pub fn new(data: [u8; TASK_ID_SIZE]) -> Self {
        let hash = murmur_hash_64a(&data, 0);
        Self { data, hash }
    }

    /// Get the ActorId embedded in this TaskId.
    pub fn actor_id(&self) -> ActorId {
        let mut actor_data = [0u8; ACTOR_ID_SIZE];
        actor_data.copy_from_slice(&self.data[TASK_ID_UNIQUE_BYTES..]);
        ActorId::new(actor_data)
    }

    /// Get the JobId embedded in this TaskId.
    pub fn job_id(&self) -> JobId {
        self.actor_id().job_id()
    }

    /// Check if this is an actor creation task.
    pub fn is_for_actor_creation_task(&self) -> bool {
        // First 8 bytes are zeros for actor creation tasks
        self.data[..TASK_ID_UNIQUE_BYTES].iter().all(|&b| b == 0)
    }

    /// Create a TaskId for an actor creation task.
    pub fn for_actor_creation_task(actor_id: &ActorId) -> Self {
        let mut data = [0u8; TASK_ID_SIZE];
        // First 8 bytes are zeros
        data[TASK_ID_UNIQUE_BYTES..].copy_from_slice(actor_id.data());
        Self::new(data)
    }

    /// Create a random TaskId for a job.
    pub fn from_random(job_id: &JobId) -> Self {
        let mut data = [0u8; TASK_ID_SIZE];
        rand::thread_rng().fill(&mut data[..TASK_ID_UNIQUE_BYTES]);

        // Create a nil actor ID with the job ID
        let actor_id = ActorId::nil_from_job(job_id);
        data[TASK_ID_UNIQUE_BYTES..].copy_from_slice(actor_id.data());

        Self::new(data)
    }

    /// Create a TaskId for the driver task.
    pub fn for_driver_task(job_id: &JobId) -> Self {
        let mut data = [0u8; TASK_ID_SIZE];
        // First 8 bytes set to 1 for driver task
        data[0] = 1;
        // Use nil actor ID with job ID
        let actor_id = ActorId::nil_from_job(job_id);
        data[TASK_ID_UNIQUE_BYTES..].copy_from_slice(actor_id.data());
        Self::new(data)
    }

    /// Create a TaskId for an actor task.
    pub fn for_actor_task(
        job_id: &JobId,
        parent_task_id: &TaskId,
        parent_task_counter: usize,
        actor_id: &ActorId,
    ) -> Self {
        // Hash parent task and counter to create unique bytes
        let mut hash_input = Vec::with_capacity(TASK_ID_SIZE + 8);
        hash_input.extend_from_slice(parent_task_id.data());
        hash_input.extend_from_slice(&(parent_task_counter as u64).to_le_bytes());

        let hash = murmur_hash_64a(&hash_input, 0);
        let hash_bytes = hash.to_le_bytes();

        let mut data = [0u8; TASK_ID_SIZE];
        data[..TASK_ID_UNIQUE_BYTES].copy_from_slice(&hash_bytes);
        data[TASK_ID_UNIQUE_BYTES..].copy_from_slice(actor_id.data());

        // Ensure this doesn't look like an actor creation task
        if data[..TASK_ID_UNIQUE_BYTES].iter().all(|&b| b == 0) {
            data[0] = 1;
        }

        let _ = job_id; // Used in C++ for validation
        Self::new(data)
    }

    /// Create a TaskId for a normal (non-actor) task.
    pub fn for_normal_task(
        job_id: &JobId,
        parent_task_id: &TaskId,
        parent_task_counter: usize,
    ) -> Self {
        // Hash parent task and counter to create unique bytes
        let mut hash_input = Vec::with_capacity(TASK_ID_SIZE + 8);
        hash_input.extend_from_slice(parent_task_id.data());
        hash_input.extend_from_slice(&(parent_task_counter as u64).to_le_bytes());

        let hash = murmur_hash_64a(&hash_input, 0);
        let hash_bytes = hash.to_le_bytes();

        let mut data = [0u8; TASK_ID_SIZE];
        data[..TASK_ID_UNIQUE_BYTES].copy_from_slice(&hash_bytes);

        // Use nil actor ID with job ID
        let actor_id = ActorId::nil_from_job(job_id);
        data[TASK_ID_UNIQUE_BYTES..].copy_from_slice(actor_id.data());

        Self::new(data)
    }

    /// Create a TaskId for a specific execution attempt of a task.
    /// This is used to make task IDs unique across retries.
    pub fn for_execution_attempt(task_id: &TaskId, attempt_number: u64) -> Self {
        if attempt_number == 0 {
            // For first attempt, return a modified version to ensure it's different
            let mut data = task_id.data.clone();
            // XOR the first byte with the attempt to ensure difference
            data[0] ^= 0x80;
            return Self::new(data);
        }

        // Hash the original task ID with the attempt number
        let mut hash_input = Vec::with_capacity(TASK_ID_SIZE + 8);
        hash_input.extend_from_slice(task_id.data());
        hash_input.extend_from_slice(&attempt_number.to_le_bytes());

        let hash = murmur_hash_64a(&hash_input, 0);
        let hash_bytes = hash.to_le_bytes();

        let mut data = [0u8; TASK_ID_SIZE];
        data[..TASK_ID_UNIQUE_BYTES].copy_from_slice(&hash_bytes);
        // Keep the actor ID portion from the original task
        data[TASK_ID_UNIQUE_BYTES..].copy_from_slice(&task_id.data[TASK_ID_UNIQUE_BYTES..]);

        Self::new(data)
    }
}

impl RayId for TaskId {
    const SIZE: usize = TASK_ID_SIZE;

    fn data(&self) -> &[u8] {
        &self.data
    }

    fn data_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }

    fn from_binary(data: &[u8]) -> Option<Self> {
        if data.len() != TASK_ID_SIZE {
            return None;
        }
        let mut arr = [0u8; TASK_ID_SIZE];
        arr.copy_from_slice(data);
        Some(Self::new(arr))
    }

    fn nil() -> Self {
        Self::new([0xFF; TASK_ID_SIZE])
    }
}

impl PartialEq for TaskId {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl Eq for TaskId {}

impl Hash for TaskId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash);
    }
}

impl fmt::Debug for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TaskId({})", self.to_hex())
    }
}

impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

/// A 28-byte object identifier (matches C++ ObjectID).
#[derive(Clone)]
pub struct ObjectId {
    data: [u8; OBJECT_ID_SIZE],
    hash: u64,
}

impl ObjectId {
    /// Create a new ObjectId with the given data.
    pub fn new(data: [u8; OBJECT_ID_SIZE]) -> Self {
        let hash = murmur_hash_64a(&data, 0);
        Self { data, hash }
    }

    /// Create an ObjectId from a task ID and index.
    pub fn from_index(task_id: &TaskId, index: u32) -> Self {
        let mut data = [0u8; OBJECT_ID_SIZE];
        data[..OBJECT_ID_INDEX_BYTES].copy_from_slice(&index.to_be_bytes());
        data[OBJECT_ID_INDEX_BYTES..].copy_from_slice(task_id.data());
        Self::new(data)
    }

    /// Get the object index.
    pub fn object_index(&self) -> u32 {
        u32::from_be_bytes([self.data[0], self.data[1], self.data[2], self.data[3]])
    }

    /// Get the TaskId embedded in this ObjectId.
    pub fn task_id(&self) -> TaskId {
        let mut task_data = [0u8; TASK_ID_SIZE];
        task_data.copy_from_slice(&self.data[OBJECT_ID_INDEX_BYTES..]);
        TaskId::new(task_data)
    }

    /// Generate a random ObjectId.
    pub fn from_random() -> Self {
        let mut data = [0u8; OBJECT_ID_SIZE];
        rand::thread_rng().fill(&mut data);
        Self::new(data)
    }
}

impl RayId for ObjectId {
    const SIZE: usize = OBJECT_ID_SIZE;

    fn data(&self) -> &[u8] {
        &self.data
    }

    fn data_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }

    fn from_binary(data: &[u8]) -> Option<Self> {
        if data.len() != OBJECT_ID_SIZE {
            return None;
        }
        let mut arr = [0u8; OBJECT_ID_SIZE];
        arr.copy_from_slice(data);
        Some(Self::new(arr))
    }

    fn nil() -> Self {
        Self::new([0xFF; OBJECT_ID_SIZE])
    }
}

impl PartialEq for ObjectId {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl Eq for ObjectId {}

impl Hash for ObjectId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash);
    }
}

impl fmt::Debug for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ObjectId({})", self.to_hex())
    }
}

impl fmt::Display for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

/// Type alias for WorkerId (same as UniqueId).
pub type WorkerId = UniqueId;

/// Type alias for NodeId (same as UniqueId).
pub type NodeId = UniqueId;

/// Type alias for FunctionId (same as UniqueId).
pub type FunctionId = UniqueId;

/// Type alias for ActorClassId (same as UniqueId).
pub type ActorClassId = UniqueId;

/// Type alias for ConfigId (same as UniqueId).
pub type ConfigId = UniqueId;

/// Type alias for ClusterId (same as UniqueId).
pub type ClusterId = UniqueId;

/// An 18-byte placement group identifier (matches C++ PlacementGroupID).
#[derive(Clone)]
pub struct PlacementGroupId {
    data: [u8; PLACEMENT_GROUP_ID_SIZE],
    hash: u64,
}

impl PlacementGroupId {
    /// Create a new PlacementGroupId with the given data.
    pub fn new(data: [u8; PLACEMENT_GROUP_ID_SIZE]) -> Self {
        let hash = murmur_hash_64a(&data, 0);
        Self { data, hash }
    }

    /// Get the JobId embedded in this PlacementGroupId.
    pub fn job_id(&self) -> JobId {
        let mut job_data = [0u8; JOB_ID_SIZE];
        job_data.copy_from_slice(&self.data[PLACEMENT_GROUP_ID_UNIQUE_BYTES..]);
        JobId::new(job_data)
    }

    /// Create a PlacementGroupId by hashing the job_id.
    /// This matches the C++ PlacementGroupID::Of method.
    pub fn of(job_id: &JobId) -> Self {
        let mut data = [0u8; PLACEMENT_GROUP_ID_SIZE];

        // Generate random unique bytes
        rand::thread_rng().fill(&mut data[..PLACEMENT_GROUP_ID_UNIQUE_BYTES]);

        // Append job ID
        data[PLACEMENT_GROUP_ID_UNIQUE_BYTES..].copy_from_slice(job_id.data());

        Self::new(data)
    }
}

impl RayId for PlacementGroupId {
    const SIZE: usize = PLACEMENT_GROUP_ID_SIZE;

    fn data(&self) -> &[u8] {
        &self.data
    }

    fn data_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }

    fn from_binary(data: &[u8]) -> Option<Self> {
        if data.len() != PLACEMENT_GROUP_ID_SIZE {
            return None;
        }
        let mut arr = [0u8; PLACEMENT_GROUP_ID_SIZE];
        arr.copy_from_slice(data);
        Some(Self::new(arr))
    }

    fn nil() -> Self {
        Self::new([0xFF; PLACEMENT_GROUP_ID_SIZE])
    }
}

impl PartialEq for PlacementGroupId {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl Eq for PlacementGroupId {}

impl Hash for PlacementGroupId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash);
    }
}

impl fmt::Debug for PlacementGroupId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PlacementGroupId({})", self.to_hex())
    }
}

impl fmt::Display for PlacementGroupId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

/// A 32-byte lease identifier (matches C++ LeaseID).
#[derive(Clone)]
pub struct LeaseId {
    data: [u8; LEASE_ID_SIZE],
    hash: u64,
}

impl LeaseId {
    /// Create a new LeaseId with the given data.
    pub fn new(data: [u8; LEASE_ID_SIZE]) -> Self {
        let hash = murmur_hash_64a(&data, 0);
        Self { data, hash }
    }

    /// Get the WorkerId embedded in this LeaseId.
    pub fn worker_id(&self) -> WorkerId {
        let mut worker_data = [0u8; UNIQUE_ID_SIZE];
        worker_data.copy_from_slice(&self.data[LEASE_ID_UNIQUE_BYTES..]);
        UniqueId::new(worker_data)
    }

    /// Create a LeaseId from a worker ID and counter.
    pub fn from_worker(worker_id: &WorkerId, counter: u32) -> Self {
        let mut data = [0u8; LEASE_ID_SIZE];

        // First 4 bytes are the counter
        data[..LEASE_ID_UNIQUE_BYTES].copy_from_slice(&counter.to_be_bytes());

        // Remaining bytes are the worker ID
        data[LEASE_ID_UNIQUE_BYTES..].copy_from_slice(worker_id.data());

        Self::new(data)
    }

    /// Create a random LeaseId.
    pub fn from_random() -> Self {
        let mut data = [0u8; LEASE_ID_SIZE];
        rand::thread_rng().fill(&mut data);
        Self::new(data)
    }
}

impl RayId for LeaseId {
    const SIZE: usize = LEASE_ID_SIZE;

    fn data(&self) -> &[u8] {
        &self.data
    }

    fn data_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }

    fn from_binary(data: &[u8]) -> Option<Self> {
        if data.len() != LEASE_ID_SIZE {
            return None;
        }
        let mut arr = [0u8; LEASE_ID_SIZE];
        arr.copy_from_slice(data);
        Some(Self::new(arr))
    }

    fn nil() -> Self {
        Self::new([0xFF; LEASE_ID_SIZE])
    }
}

impl PartialEq for LeaseId {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl Eq for LeaseId {}

impl Hash for LeaseId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash);
    }
}

impl fmt::Debug for LeaseId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LeaseId({})", self.to_hex())
    }
}

impl fmt::Display for LeaseId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_job_id_size() {
        assert_eq!(JobId::SIZE, 4);
    }

    #[test]
    fn test_actor_id_size() {
        assert_eq!(ActorId::SIZE, 16);
    }

    #[test]
    fn test_task_id_size() {
        assert_eq!(TaskId::SIZE, 24);
    }

    #[test]
    fn test_object_id_size() {
        assert_eq!(ObjectId::SIZE, 28);
    }

    #[test]
    fn test_job_id_from_int() {
        let job_id = JobId::from_int(12345);
        assert_eq!(job_id.to_int(), 12345);
    }

    #[test]
    fn test_job_id_nil() {
        let nil = JobId::nil();
        assert!(nil.is_nil());

        let non_nil = JobId::from_int(1);
        assert!(!non_nil.is_nil());
    }

    #[test]
    fn test_job_id_hex_conversion() {
        let job_id = JobId::from_int(0x12345678);
        let hex = job_id.to_hex();
        assert_eq!(hex, "12345678");

        let parsed = JobId::from_hex(&hex).unwrap();
        assert_eq!(job_id, parsed);
    }

    #[test]
    fn test_unique_id_random() {
        let id1 = UniqueId::from_random();
        let id2 = UniqueId::from_random();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_unique_id_hash() {
        let id = UniqueId::from_random();
        let mut set = HashSet::new();
        set.insert(id.clone());
        assert!(set.contains(&id));
    }

    #[test]
    fn test_actor_id_job_id() {
        let job_id = JobId::from_int(42);
        let actor_id = ActorId::nil_from_job(&job_id);

        assert_eq!(actor_id.job_id(), job_id);
    }

    #[test]
    fn test_task_id_actor_creation() {
        let job_id = JobId::from_int(1);
        let actor_id = ActorId::nil_from_job(&job_id);
        let task_id = TaskId::for_actor_creation_task(&actor_id);

        assert!(task_id.is_for_actor_creation_task());
        assert_eq!(task_id.actor_id(), actor_id);
    }

    #[test]
    fn test_object_id_from_index() {
        let job_id = JobId::from_int(1);
        let task_id = TaskId::from_random(&job_id);
        let object_id = ObjectId::from_index(&task_id, 5);

        assert_eq!(object_id.object_index(), 5);
        assert_eq!(object_id.task_id(), task_id);
    }

    #[test]
    fn test_id_binary_roundtrip() {
        let original = UniqueId::from_random();
        let binary = original.to_binary();
        let restored = UniqueId::from_binary(&binary).unwrap();
        assert_eq!(original, restored);
    }

    #[test]
    fn test_id_invalid_binary_length() {
        let short_data = vec![0u8; 10];
        assert!(JobId::from_binary(&short_data).is_none());
        assert!(UniqueId::from_binary(&short_data).is_none());
    }

    #[test]
    fn test_murmur_hash() {
        // Test that hash is deterministic
        let data = b"test data";
        let hash1 = murmur_hash_64a(data, 0);
        let hash2 = murmur_hash_64a(data, 0);
        assert_eq!(hash1, hash2);

        // Different data produces different hashes
        let other_data = b"other data";
        let hash3 = murmur_hash_64a(other_data, 0);
        assert_ne!(hash1, hash3);

        // Different seeds produce different hashes
        let hash4 = murmur_hash_64a(data, 1);
        assert_ne!(hash1, hash4);
    }

    #[test]
    fn test_actor_id_of() {
        let job_id = JobId::from_int(199);
        let driver_task_id = TaskId::for_driver_task(&job_id);
        let actor_id = ActorId::of(&job_id, &driver_task_id, 1);

        assert_eq!(actor_id.job_id(), job_id);
        assert!(!actor_id.is_nil());
    }

    #[test]
    fn test_task_id_for_driver_task() {
        let job_id = JobId::from_int(199);
        let driver_task_id = TaskId::for_driver_task(&job_id);

        assert!(!driver_task_id.is_nil());
        assert!(!driver_task_id.is_for_actor_creation_task());
    }

    #[test]
    fn test_task_id_for_actor_task() {
        let job_id = JobId::from_int(199);
        let driver_task_id = TaskId::for_driver_task(&job_id);
        let actor_id = ActorId::of(&job_id, &driver_task_id, 1);
        let task_id = TaskId::for_actor_task(&job_id, &driver_task_id, 1, &actor_id);

        assert_eq!(task_id.actor_id(), actor_id);
        assert!(!task_id.is_for_actor_creation_task());
    }

    #[test]
    fn test_task_id_for_normal_task() {
        let job_id = JobId::from_int(199);
        let driver_task_id = TaskId::for_driver_task(&job_id);
        let task_id = TaskId::for_normal_task(&job_id, &driver_task_id, 0);

        assert!(!task_id.is_nil());
        assert!(!task_id.is_for_actor_creation_task());
    }

    #[test]
    fn test_task_id_for_execution_attempt() {
        let job_id = JobId::from_int(199);
        let task_id = TaskId::from_random(&job_id);

        // Different attempts should produce different task IDs
        let attempt0 = TaskId::for_execution_attempt(&task_id, 0);
        let attempt1 = TaskId::for_execution_attempt(&task_id, 1);

        assert_ne!(task_id, attempt0);
        assert_ne!(task_id, attempt1);
        assert_ne!(attempt0, attempt1);

        // Same attempt should produce same ID
        let attempt1_again = TaskId::for_execution_attempt(&task_id, 1);
        assert_eq!(attempt1, attempt1_again);

        // Check for overflow handling
        assert_ne!(
            TaskId::for_execution_attempt(&task_id, 0),
            TaskId::for_execution_attempt(&task_id, 256)
        );
    }

    #[test]
    fn test_placement_group_id_size() {
        assert_eq!(PlacementGroupId::SIZE, 18);
    }

    #[test]
    fn test_placement_group_id_of() {
        let job_id = JobId::from_int(1);
        let pg_id = PlacementGroupId::of(&job_id);

        assert_eq!(pg_id.job_id(), job_id);
        assert!(!pg_id.is_nil());
    }

    #[test]
    fn test_placement_group_id_roundtrip() {
        let job_id = JobId::from_int(1);
        let pg_id1 = PlacementGroupId::of(&job_id);
        let binary = pg_id1.to_binary();
        let pg_id2 = PlacementGroupId::from_binary(&binary).unwrap();

        assert_eq!(pg_id1, pg_id2);

        let hex = pg_id1.to_hex();
        let pg_id3 = PlacementGroupId::from_hex(&hex).unwrap();
        assert_eq!(pg_id1, pg_id3);
    }

    #[test]
    fn test_lease_id_size() {
        assert_eq!(LeaseId::SIZE, 32);
    }

    #[test]
    fn test_lease_id_from_worker() {
        let worker_id = WorkerId::from_random();
        let lease_id = LeaseId::from_worker(&worker_id, 2);

        assert!(!lease_id.is_nil());
        assert_eq!(lease_id.worker_id(), worker_id);
    }

    #[test]
    fn test_lease_id_different_counters() {
        let worker_id = WorkerId::from_random();
        let lease1 = LeaseId::from_worker(&worker_id, 1);
        let lease2 = LeaseId::from_worker(&worker_id, 2);

        assert_ne!(lease1, lease2);
        assert_eq!(lease1.worker_id(), lease2.worker_id());
    }

    #[test]
    fn test_lease_id_roundtrip() {
        let worker_id = WorkerId::from_random();
        let lease_id = LeaseId::from_worker(&worker_id, 2);

        let from_hex = LeaseId::from_hex(&lease_id.to_hex()).unwrap();
        let from_binary = LeaseId::from_binary(&lease_id.to_binary()).unwrap();

        assert_eq!(lease_id, from_hex);
        assert_eq!(lease_id, from_binary);
        assert_eq!(lease_id.worker_id(), from_hex.worker_id());
    }

    #[test]
    fn test_lease_id_random() {
        let random_lease = LeaseId::from_random();
        assert!(!random_lease.is_nil());
    }
}
