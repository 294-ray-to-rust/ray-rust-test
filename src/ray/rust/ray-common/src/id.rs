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
}
