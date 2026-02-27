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

//! Plasma object store implementation.

pub mod allocator;
pub mod common;
pub mod eviction;
pub mod lifecycle;
pub mod stats;
pub mod store;

// Re-export types from common
pub use common::{
    Allocation, LocalObject, ObjectInfo, ObjectSource, ObjectState, PlasmaError,
    PlasmaObjectHeader, PlasmaResult,
};

// Re-export types from allocator
pub use allocator::{Allocator, AllocatorStats, HeapAllocator, NullAllocator, PlasmaAllocator};

// Re-export types from store
pub use store::{ObjectRef, ObjectStore, ObjectStoreConfig, ObjectStoreStats};

// Re-export types from eviction
pub use eviction::{AllocatorView, EvictionPolicy, LRUCache, LRUEvictionPolicy, ObjectStoreView};

// Re-export types from stats
pub use stats::ObjectStatsCollector;

// Re-export types from lifecycle
pub use lifecycle::ObjectLifecycleManager;
