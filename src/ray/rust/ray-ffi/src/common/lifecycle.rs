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

//! FFI bindings for Object Lifecycle Manager and Stats Collector.

use parking_lot::RwLock;
use ray_common::id::RayId;
use ray_common::ObjectId;
use ray_object_manager::plasma::allocator::HeapAllocator;
use ray_object_manager::plasma::eviction::LRUCache;
use ray_object_manager::plasma::lifecycle::ObjectLifecycleManager;
use ray_object_manager::{ObjectInfo, ObjectSource, PlasmaError};
use std::sync::atomic::Ordering;
use std::sync::Arc;

/// FFI-safe wrapper for ObjectLifecycleManager.
pub struct RustObjectLifecycleManager {
    inner: ObjectLifecycleManager<HeapAllocator, LRUCache>,
}

/// FFI-safe wrapper for detailed stats collector snapshot.
pub struct RustStatsCollectorSnapshot {
    pub num_bytes_created_total: i64,
    pub num_objects_spillable: i64,
    pub num_bytes_spillable: i64,
    pub num_objects_unsealed: i64,
    pub num_bytes_unsealed: i64,
    pub num_objects_in_use: i64,
    pub num_bytes_in_use: i64,
    pub num_objects_evictable: i64,
    pub num_bytes_evictable: i64,
    pub num_objects_created_by_worker: i64,
    pub num_bytes_created_by_worker: i64,
    pub num_objects_restored: i64,
    pub num_bytes_restored: i64,
    pub num_objects_received: i64,
    pub num_bytes_received: i64,
    pub num_objects_errored: i64,
    pub num_bytes_errored: i64,
}

/// Result of a lifecycle operation.
pub struct RustLifecycleResult {
    success: bool,
    error_code: u8,
    error_message: String,
}

// Error codes
const ERROR_NONE: u8 = 0;
const ERROR_OBJECT_EXISTS: u8 = 1;
const ERROR_OBJECT_NOT_FOUND: u8 = 2;
const ERROR_OBJECT_ALREADY_SEALED: u8 = 3;
const ERROR_OUT_OF_MEMORY: u8 = 4;
const ERROR_OBJECT_NOT_SEALED: u8 = 7;
const ERROR_INVALID_REQUEST: u8 = 8;
const ERROR_UNEXPECTED: u8 = 10;

#[cxx::bridge(namespace = "ray::ffi")]
mod ffi {
    extern "Rust" {
        type RustObjectLifecycleManager;
        type RustStatsCollectorSnapshot;
        type RustLifecycleResult;

        // LifecycleManager creation
        fn lifecycle_manager_new(capacity: usize) -> Box<RustObjectLifecycleManager>;

        // LifecycleManager operations
        fn lifecycle_manager_create_object(
            manager: &RustObjectLifecycleManager,
            object_id_bytes: &[u8],
            data_size: usize,
            metadata_size: usize,
            source: u8,
            fallback: bool,
        ) -> Box<RustLifecycleResult>;

        fn lifecycle_manager_seal_object(
            manager: &RustObjectLifecycleManager,
            object_id_bytes: &[u8],
        ) -> Box<RustLifecycleResult>;

        fn lifecycle_manager_abort_object(
            manager: &RustObjectLifecycleManager,
            object_id_bytes: &[u8],
        ) -> Box<RustLifecycleResult>;

        fn lifecycle_manager_delete_object(
            manager: &RustObjectLifecycleManager,
            object_id_bytes: &[u8],
        ) -> Box<RustLifecycleResult>;

        fn lifecycle_manager_add_reference(
            manager: &RustObjectLifecycleManager,
            object_id_bytes: &[u8],
        ) -> bool;

        fn lifecycle_manager_remove_reference(
            manager: &RustObjectLifecycleManager,
            object_id_bytes: &[u8],
        ) -> bool;

        fn lifecycle_manager_contains(
            manager: &RustObjectLifecycleManager,
            object_id_bytes: &[u8],
        ) -> bool;

        fn lifecycle_manager_len(manager: &RustObjectLifecycleManager) -> usize;
        fn lifecycle_manager_is_empty(manager: &RustObjectLifecycleManager) -> bool;

        // Get stats snapshot
        fn lifecycle_manager_get_stats(
            manager: &RustObjectLifecycleManager,
        ) -> Box<RustStatsCollectorSnapshot>;

        // RustStatsCollectorSnapshot accessors
        fn stats_snapshot_num_bytes_created_total(stats: &RustStatsCollectorSnapshot) -> i64;
        fn stats_snapshot_num_objects_spillable(stats: &RustStatsCollectorSnapshot) -> i64;
        fn stats_snapshot_num_bytes_spillable(stats: &RustStatsCollectorSnapshot) -> i64;
        fn stats_snapshot_num_objects_unsealed(stats: &RustStatsCollectorSnapshot) -> i64;
        fn stats_snapshot_num_bytes_unsealed(stats: &RustStatsCollectorSnapshot) -> i64;
        fn stats_snapshot_num_objects_in_use(stats: &RustStatsCollectorSnapshot) -> i64;
        fn stats_snapshot_num_bytes_in_use(stats: &RustStatsCollectorSnapshot) -> i64;
        fn stats_snapshot_num_objects_evictable(stats: &RustStatsCollectorSnapshot) -> i64;
        fn stats_snapshot_num_bytes_evictable(stats: &RustStatsCollectorSnapshot) -> i64;
        fn stats_snapshot_num_objects_created_by_worker(stats: &RustStatsCollectorSnapshot) -> i64;
        fn stats_snapshot_num_bytes_created_by_worker(stats: &RustStatsCollectorSnapshot) -> i64;
        fn stats_snapshot_num_objects_restored(stats: &RustStatsCollectorSnapshot) -> i64;
        fn stats_snapshot_num_bytes_restored(stats: &RustStatsCollectorSnapshot) -> i64;
        fn stats_snapshot_num_objects_received(stats: &RustStatsCollectorSnapshot) -> i64;
        fn stats_snapshot_num_bytes_received(stats: &RustStatsCollectorSnapshot) -> i64;
        fn stats_snapshot_num_objects_errored(stats: &RustStatsCollectorSnapshot) -> i64;
        fn stats_snapshot_num_bytes_errored(stats: &RustStatsCollectorSnapshot) -> i64;

        // RustLifecycleResult accessors
        fn lifecycle_result_success(result: &RustLifecycleResult) -> bool;
        fn lifecycle_result_error_code(result: &RustLifecycleResult) -> u8;
        fn lifecycle_result_error_message(result: &RustLifecycleResult) -> &str;
    }
}

// Helper functions

fn bytes_to_object_id(bytes: &[u8]) -> ObjectId {
    ObjectId::from_binary(bytes).expect("Invalid object ID binary")
}

fn source_from_u8(source: u8) -> ObjectSource {
    match source {
        0 => ObjectSource::CreatedByWorker,
        1 => ObjectSource::RestoredFromStorage,
        2 => ObjectSource::ReceivedFromRemoteRaylet,
        3 => ObjectSource::ErrorStoredByRaylet,
        4 => ObjectSource::CreatedByPlasmaFallbackAllocation,
        _ => ObjectSource::CreatedByWorker,
    }
}

fn error_to_code(err: &PlasmaError) -> u8 {
    match err {
        PlasmaError::ObjectExists(_) => ERROR_OBJECT_EXISTS,
        PlasmaError::ObjectNotFound(_) => ERROR_OBJECT_NOT_FOUND,
        PlasmaError::ObjectAlreadySealed(_) => ERROR_OBJECT_ALREADY_SEALED,
        PlasmaError::OutOfMemory => ERROR_OUT_OF_MEMORY,
        PlasmaError::ObjectNotSealed(_) => ERROR_OBJECT_NOT_SEALED,
        PlasmaError::InvalidRequest(_) => ERROR_INVALID_REQUEST,
        _ => ERROR_UNEXPECTED,
    }
}

fn make_result<T>(result: Result<T, PlasmaError>) -> Box<RustLifecycleResult> {
    match result {
        Ok(_) => Box::new(RustLifecycleResult {
            success: true,
            error_code: ERROR_NONE,
            error_message: String::new(),
        }),
        Err(err) => Box::new(RustLifecycleResult {
            success: false,
            error_code: error_to_code(&err),
            error_message: err.to_string(),
        }),
    }
}

// FFI function implementations

fn lifecycle_manager_new(capacity: usize) -> Box<RustObjectLifecycleManager> {
    let allocator = Arc::new(HeapAllocator::new(capacity));
    let eviction = Arc::new(RwLock::new(LRUCache::new(
        "lifecycle_cache".to_string(),
        capacity as i64,
    )));
    Box::new(RustObjectLifecycleManager {
        inner: ObjectLifecycleManager::new(allocator, eviction),
    })
}

fn lifecycle_manager_create_object(
    manager: &RustObjectLifecycleManager,
    object_id_bytes: &[u8],
    data_size: usize,
    metadata_size: usize,
    source: u8,
    fallback: bool,
) -> Box<RustLifecycleResult> {
    let object_id = bytes_to_object_id(object_id_bytes);
    let info = ObjectInfo::new(object_id, data_size, metadata_size, vec![]);
    let source = source_from_u8(source);
    let result = manager.inner.create_object(info, source, fallback);
    make_result(result)
}

fn lifecycle_manager_seal_object(
    manager: &RustObjectLifecycleManager,
    object_id_bytes: &[u8],
) -> Box<RustLifecycleResult> {
    let object_id = bytes_to_object_id(object_id_bytes);
    let result = manager.inner.seal_object(&object_id);
    make_result(result)
}

fn lifecycle_manager_abort_object(
    manager: &RustObjectLifecycleManager,
    object_id_bytes: &[u8],
) -> Box<RustLifecycleResult> {
    let object_id = bytes_to_object_id(object_id_bytes);
    let result = manager.inner.abort_object(&object_id);
    make_result(result)
}

fn lifecycle_manager_delete_object(
    manager: &RustObjectLifecycleManager,
    object_id_bytes: &[u8],
) -> Box<RustLifecycleResult> {
    let object_id = bytes_to_object_id(object_id_bytes);
    let result = manager.inner.delete_object(&object_id);
    make_result(result)
}

fn lifecycle_manager_add_reference(
    manager: &RustObjectLifecycleManager,
    object_id_bytes: &[u8],
) -> bool {
    let object_id = bytes_to_object_id(object_id_bytes);
    manager.inner.add_reference(&object_id)
}

fn lifecycle_manager_remove_reference(
    manager: &RustObjectLifecycleManager,
    object_id_bytes: &[u8],
) -> bool {
    let object_id = bytes_to_object_id(object_id_bytes);
    manager.inner.remove_reference(&object_id)
}

fn lifecycle_manager_contains(
    manager: &RustObjectLifecycleManager,
    object_id_bytes: &[u8],
) -> bool {
    let object_id = bytes_to_object_id(object_id_bytes);
    manager.inner.contains(&object_id)
}

fn lifecycle_manager_len(manager: &RustObjectLifecycleManager) -> usize {
    manager.inner.len()
}

fn lifecycle_manager_is_empty(manager: &RustObjectLifecycleManager) -> bool {
    manager.inner.is_empty()
}

fn lifecycle_manager_get_stats(
    manager: &RustObjectLifecycleManager,
) -> Box<RustStatsCollectorSnapshot> {
    let stats = manager.inner.stats_collector();
    Box::new(RustStatsCollectorSnapshot {
        num_bytes_created_total: stats.num_bytes_created_total.load(Ordering::SeqCst),
        num_objects_spillable: stats.num_objects_spillable.load(Ordering::SeqCst),
        num_bytes_spillable: stats.num_bytes_spillable.load(Ordering::SeqCst),
        num_objects_unsealed: stats.num_objects_unsealed.load(Ordering::SeqCst),
        num_bytes_unsealed: stats.num_bytes_unsealed.load(Ordering::SeqCst),
        num_objects_in_use: stats.num_objects_in_use.load(Ordering::SeqCst),
        num_bytes_in_use: stats.num_bytes_in_use.load(Ordering::SeqCst),
        num_objects_evictable: stats.num_objects_evictable.load(Ordering::SeqCst),
        num_bytes_evictable: stats.num_bytes_evictable.load(Ordering::SeqCst),
        num_objects_created_by_worker: stats.num_objects_created_by_worker.load(Ordering::SeqCst),
        num_bytes_created_by_worker: stats.num_bytes_created_by_worker.load(Ordering::SeqCst),
        num_objects_restored: stats.num_objects_restored.load(Ordering::SeqCst),
        num_bytes_restored: stats.num_bytes_restored.load(Ordering::SeqCst),
        num_objects_received: stats.num_objects_received.load(Ordering::SeqCst),
        num_bytes_received: stats.num_bytes_received.load(Ordering::SeqCst),
        num_objects_errored: stats.num_objects_errored.load(Ordering::SeqCst),
        num_bytes_errored: stats.num_bytes_errored.load(Ordering::SeqCst),
    })
}

// Stats snapshot accessors
fn stats_snapshot_num_bytes_created_total(stats: &RustStatsCollectorSnapshot) -> i64 {
    stats.num_bytes_created_total
}

fn stats_snapshot_num_objects_spillable(stats: &RustStatsCollectorSnapshot) -> i64 {
    stats.num_objects_spillable
}

fn stats_snapshot_num_bytes_spillable(stats: &RustStatsCollectorSnapshot) -> i64 {
    stats.num_bytes_spillable
}

fn stats_snapshot_num_objects_unsealed(stats: &RustStatsCollectorSnapshot) -> i64 {
    stats.num_objects_unsealed
}

fn stats_snapshot_num_bytes_unsealed(stats: &RustStatsCollectorSnapshot) -> i64 {
    stats.num_bytes_unsealed
}

fn stats_snapshot_num_objects_in_use(stats: &RustStatsCollectorSnapshot) -> i64 {
    stats.num_objects_in_use
}

fn stats_snapshot_num_bytes_in_use(stats: &RustStatsCollectorSnapshot) -> i64 {
    stats.num_bytes_in_use
}

fn stats_snapshot_num_objects_evictable(stats: &RustStatsCollectorSnapshot) -> i64 {
    stats.num_objects_evictable
}

fn stats_snapshot_num_bytes_evictable(stats: &RustStatsCollectorSnapshot) -> i64 {
    stats.num_bytes_evictable
}

fn stats_snapshot_num_objects_created_by_worker(stats: &RustStatsCollectorSnapshot) -> i64 {
    stats.num_objects_created_by_worker
}

fn stats_snapshot_num_bytes_created_by_worker(stats: &RustStatsCollectorSnapshot) -> i64 {
    stats.num_bytes_created_by_worker
}

fn stats_snapshot_num_objects_restored(stats: &RustStatsCollectorSnapshot) -> i64 {
    stats.num_objects_restored
}

fn stats_snapshot_num_bytes_restored(stats: &RustStatsCollectorSnapshot) -> i64 {
    stats.num_bytes_restored
}

fn stats_snapshot_num_objects_received(stats: &RustStatsCollectorSnapshot) -> i64 {
    stats.num_objects_received
}

fn stats_snapshot_num_bytes_received(stats: &RustStatsCollectorSnapshot) -> i64 {
    stats.num_bytes_received
}

fn stats_snapshot_num_objects_errored(stats: &RustStatsCollectorSnapshot) -> i64 {
    stats.num_objects_errored
}

fn stats_snapshot_num_bytes_errored(stats: &RustStatsCollectorSnapshot) -> i64 {
    stats.num_bytes_errored
}

// RustLifecycleResult accessors
fn lifecycle_result_success(result: &RustLifecycleResult) -> bool {
    result.success
}

fn lifecycle_result_error_code(result: &RustLifecycleResult) -> u8 {
    result.error_code
}

fn lifecycle_result_error_message(result: &RustLifecycleResult) -> &str {
    &result.error_message
}
