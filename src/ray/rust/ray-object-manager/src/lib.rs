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

//! Ray Object Manager - Distributed object storage for Ray.
//!
//! This crate provides the Rust implementation of the plasma object store
//! and object manager for Ray's distributed object storage system.

pub mod plasma;

// Re-export commonly used types
pub use plasma::allocator::{Allocator, AllocatorStats, HeapAllocator, NullAllocator};
pub use plasma::common::{
    Allocation, LocalObject, ObjectInfo, ObjectSource, ObjectState, PlasmaError,
    PlasmaObjectHeader, PlasmaResult,
};
pub use plasma::store::{ObjectRef, ObjectStore, ObjectStoreConfig, ObjectStoreStats};
