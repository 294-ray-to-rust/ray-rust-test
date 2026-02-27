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

//! FFI bindings for Plasma object store types.

use ray_common::id::RayId;
use ray_common::ObjectId;
use ray_object_manager::{ObjectSource, ObjectState, ObjectStore, ObjectStoreConfig, PlasmaError};

/// FFI-safe wrapper for ObjectState.
pub struct RustObjectState(ObjectState);

/// FFI-safe wrapper for ObjectSource.
pub struct RustObjectSource(ObjectSource);

/// FFI-safe wrapper for PlasmaError.
pub struct RustPlasmaError {
    code: u8,
    message: String,
}

/// FFI-safe wrapper for ObjectStore.
pub struct RustObjectStore {
    inner: ObjectStore,
}

/// FFI-safe wrapper for ObjectStoreStats snapshot.
pub struct RustObjectStoreStats {
    pub num_objects: usize,
    pub num_bytes_used: i64,
    pub num_bytes_created_total: i64,
    pub num_objects_created_total: usize,
    pub num_bytes_sealed: i64,
    pub num_objects_sealed: usize,
    pub num_fallback_allocations: usize,
    pub num_bytes_fallback: i64,
}

/// Result of a plasma operation - either success or error code + message.
pub struct RustPlasmaResult {
    success: bool,
    error_code: u8,
    error_message: String,
}

// Plasma error codes (defined outside bridge for internal use)
const PLASMA_ERROR_NONE: u8 = 0;
const PLASMA_ERROR_OBJECT_EXISTS: u8 = 1;
const PLASMA_ERROR_OBJECT_NOT_FOUND: u8 = 2;
const PLASMA_ERROR_OBJECT_ALREADY_SEALED: u8 = 3;
const PLASMA_ERROR_OUT_OF_MEMORY: u8 = 4;
const PLASMA_ERROR_TRANSIENT_OUT_OF_MEMORY: u8 = 5;
const PLASMA_ERROR_OUT_OF_DISK: u8 = 6;
const PLASMA_ERROR_OBJECT_NOT_SEALED: u8 = 7;
const PLASMA_ERROR_INVALID_REQUEST: u8 = 8;
const PLASMA_ERROR_IO_ERROR: u8 = 9;
const PLASMA_ERROR_UNEXPECTED: u8 = 10;

#[cxx::bridge(namespace = "ray::ffi")]
mod ffi {
    extern "Rust" {
        type RustObjectStore;
        type RustPlasmaResult;
        type RustObjectStoreStats;

        // ObjectStore creation
        fn object_store_new(capacity: usize) -> Box<RustObjectStore>;
        fn object_store_new_with_config(
            capacity: usize,
            enable_fallback: bool,
            min_fallback_size: usize,
        ) -> Box<RustObjectStore>;

        // ObjectStore operations
        fn object_store_create_object(
            store: &RustObjectStore,
            object_id_bytes: &[u8],
            data_size: usize,
            metadata_size: usize,
            source: u8,
            owner_address: &[u8],
        ) -> Box<RustPlasmaResult>;

        fn object_store_seal_object(
            store: &RustObjectStore,
            object_id_bytes: &[u8],
        ) -> Box<RustPlasmaResult>;

        fn object_store_get_object(
            store: &RustObjectStore,
            object_id_bytes: &[u8],
        ) -> Box<RustPlasmaResult>;

        fn object_store_release_object(
            store: &RustObjectStore,
            object_id_bytes: &[u8],
        ) -> Box<RustPlasmaResult>;

        fn object_store_delete_object(
            store: &RustObjectStore,
            object_id_bytes: &[u8],
        ) -> Box<RustPlasmaResult>;

        fn object_store_abort_object(
            store: &RustObjectStore,
            object_id_bytes: &[u8],
        ) -> Box<RustPlasmaResult>;

        fn object_store_contains(store: &RustObjectStore, object_id_bytes: &[u8]) -> bool;
        fn object_store_is_sealed(store: &RustObjectStore, object_id_bytes: &[u8]) -> bool;
        fn object_store_len(store: &RustObjectStore) -> usize;
        fn object_store_is_empty(store: &RustObjectStore) -> bool;
        fn object_store_capacity(store: &RustObjectStore) -> usize;
        fn object_store_available_capacity(store: &RustObjectStore) -> usize;

        fn object_store_evict(store: &RustObjectStore, bytes_needed: usize) -> usize;

        // Stats
        fn object_store_get_stats(store: &RustObjectStore) -> Box<RustObjectStoreStats>;

        // RustObjectStoreStats accessors
        fn stats_num_objects(stats: &RustObjectStoreStats) -> usize;
        fn stats_num_bytes_used(stats: &RustObjectStoreStats) -> i64;
        fn stats_num_bytes_created_total(stats: &RustObjectStoreStats) -> i64;
        fn stats_num_objects_created_total(stats: &RustObjectStoreStats) -> usize;
        fn stats_num_bytes_sealed(stats: &RustObjectStoreStats) -> i64;
        fn stats_num_objects_sealed(stats: &RustObjectStoreStats) -> usize;
        fn stats_num_fallback_allocations(stats: &RustObjectStoreStats) -> usize;
        fn stats_num_bytes_fallback(stats: &RustObjectStoreStats) -> i64;

        // RustPlasmaResult accessors
        fn plasma_result_success(result: &RustPlasmaResult) -> bool;
        fn plasma_result_error_code(result: &RustPlasmaResult) -> u8;
        fn plasma_result_error_message(result: &RustPlasmaResult) -> &str;
    }
}

// Implementation of FFI functions

fn object_store_new(capacity: usize) -> Box<RustObjectStore> {
    Box::new(RustObjectStore {
        inner: ObjectStore::with_capacity(capacity),
    })
}

fn object_store_new_with_config(
    capacity: usize,
    enable_fallback: bool,
    min_fallback_size: usize,
) -> Box<RustObjectStore> {
    let config = ObjectStoreConfig {
        capacity,
        fallback_directory: None,
        enable_fallback,
        min_fallback_size,
    };
    Box::new(RustObjectStore {
        inner: ObjectStore::new(config),
    })
}

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

fn plasma_error_to_code(err: &PlasmaError) -> u8 {
    match err {
        PlasmaError::ObjectExists(_) => PLASMA_ERROR_OBJECT_EXISTS,
        PlasmaError::ObjectNotFound(_) => PLASMA_ERROR_OBJECT_NOT_FOUND,
        PlasmaError::ObjectAlreadySealed(_) => PLASMA_ERROR_OBJECT_ALREADY_SEALED,
        PlasmaError::OutOfMemory => PLASMA_ERROR_OUT_OF_MEMORY,
        PlasmaError::TransientOutOfMemory => PLASMA_ERROR_TRANSIENT_OUT_OF_MEMORY,
        PlasmaError::OutOfDisk => PLASMA_ERROR_OUT_OF_DISK,
        PlasmaError::ObjectNotSealed(_) => PLASMA_ERROR_OBJECT_NOT_SEALED,
        PlasmaError::InvalidRequest(_) => PLASMA_ERROR_INVALID_REQUEST,
        PlasmaError::IoError(_) => PLASMA_ERROR_IO_ERROR,
        PlasmaError::Unexpected(_) => PLASMA_ERROR_UNEXPECTED,
    }
}

fn make_result<T>(result: Result<T, PlasmaError>) -> Box<RustPlasmaResult> {
    match result {
        Ok(_) => Box::new(RustPlasmaResult {
            success: true,
            error_code: PLASMA_ERROR_NONE,
            error_message: String::new(),
        }),
        Err(err) => Box::new(RustPlasmaResult {
            success: false,
            error_code: plasma_error_to_code(&err),
            error_message: err.to_string(),
        }),
    }
}

fn object_store_create_object(
    store: &RustObjectStore,
    object_id_bytes: &[u8],
    data_size: usize,
    metadata_size: usize,
    source: u8,
    owner_address: &[u8],
) -> Box<RustPlasmaResult> {
    let object_id = bytes_to_object_id(object_id_bytes);
    let source = source_from_u8(source);
    let result = store.inner.create_object(
        object_id,
        data_size,
        metadata_size,
        source,
        owner_address.to_vec(),
    );
    make_result(result)
}

fn object_store_seal_object(
    store: &RustObjectStore,
    object_id_bytes: &[u8],
) -> Box<RustPlasmaResult> {
    let object_id = bytes_to_object_id(object_id_bytes);
    let result = store.inner.seal_object(&object_id);
    make_result(result)
}

fn object_store_get_object(
    store: &RustObjectStore,
    object_id_bytes: &[u8],
) -> Box<RustPlasmaResult> {
    let object_id = bytes_to_object_id(object_id_bytes);
    let result = store.inner.get_object(&object_id);
    make_result(result)
}

fn object_store_release_object(
    store: &RustObjectStore,
    object_id_bytes: &[u8],
) -> Box<RustPlasmaResult> {
    let object_id = bytes_to_object_id(object_id_bytes);
    let result = store.inner.release_object(&object_id);
    make_result(result)
}

fn object_store_delete_object(
    store: &RustObjectStore,
    object_id_bytes: &[u8],
) -> Box<RustPlasmaResult> {
    let object_id = bytes_to_object_id(object_id_bytes);
    let result = store.inner.delete_object(&object_id);
    make_result(result)
}

fn object_store_abort_object(
    store: &RustObjectStore,
    object_id_bytes: &[u8],
) -> Box<RustPlasmaResult> {
    let object_id = bytes_to_object_id(object_id_bytes);
    let result = store.inner.abort_object(&object_id);
    make_result(result)
}

fn object_store_contains(store: &RustObjectStore, object_id_bytes: &[u8]) -> bool {
    let object_id = bytes_to_object_id(object_id_bytes);
    store.inner.contains(&object_id)
}

fn object_store_is_sealed(store: &RustObjectStore, object_id_bytes: &[u8]) -> bool {
    let object_id = bytes_to_object_id(object_id_bytes);
    store.inner.is_sealed(&object_id)
}

fn object_store_len(store: &RustObjectStore) -> usize {
    store.inner.len()
}

fn object_store_is_empty(store: &RustObjectStore) -> bool {
    store.inner.is_empty()
}

fn object_store_capacity(store: &RustObjectStore) -> usize {
    store.inner.config().capacity
}

fn object_store_available_capacity(store: &RustObjectStore) -> usize {
    store.inner.available_capacity()
}

fn object_store_evict(store: &RustObjectStore, bytes_needed: usize) -> usize {
    store.inner.evict(bytes_needed).unwrap_or(0)
}

fn object_store_get_stats(store: &RustObjectStore) -> Box<RustObjectStoreStats> {
    let stats = store.inner.stats();
    Box::new(RustObjectStoreStats {
        num_objects: stats.num_objects.load(std::sync::atomic::Ordering::SeqCst),
        num_bytes_used: stats.num_bytes_used.load(std::sync::atomic::Ordering::SeqCst),
        num_bytes_created_total: stats
            .num_bytes_created_total
            .load(std::sync::atomic::Ordering::SeqCst),
        num_objects_created_total: stats
            .num_objects_created_total
            .load(std::sync::atomic::Ordering::SeqCst),
        num_bytes_sealed: stats
            .num_bytes_sealed
            .load(std::sync::atomic::Ordering::SeqCst),
        num_objects_sealed: stats
            .num_objects_sealed
            .load(std::sync::atomic::Ordering::SeqCst),
        num_fallback_allocations: stats
            .num_fallback_allocations
            .load(std::sync::atomic::Ordering::SeqCst),
        num_bytes_fallback: stats
            .num_bytes_fallback
            .load(std::sync::atomic::Ordering::SeqCst),
    })
}

// Stats accessors
fn stats_num_objects(stats: &RustObjectStoreStats) -> usize {
    stats.num_objects
}

fn stats_num_bytes_used(stats: &RustObjectStoreStats) -> i64 {
    stats.num_bytes_used
}

fn stats_num_bytes_created_total(stats: &RustObjectStoreStats) -> i64 {
    stats.num_bytes_created_total
}

fn stats_num_objects_created_total(stats: &RustObjectStoreStats) -> usize {
    stats.num_objects_created_total
}

fn stats_num_bytes_sealed(stats: &RustObjectStoreStats) -> i64 {
    stats.num_bytes_sealed
}

fn stats_num_objects_sealed(stats: &RustObjectStoreStats) -> usize {
    stats.num_objects_sealed
}

fn stats_num_fallback_allocations(stats: &RustObjectStoreStats) -> usize {
    stats.num_fallback_allocations
}

fn stats_num_bytes_fallback(stats: &RustObjectStoreStats) -> i64 {
    stats.num_bytes_fallback
}

// Result accessors
fn plasma_result_success(result: &RustPlasmaResult) -> bool {
    result.success
}

fn plasma_result_error_code(result: &RustPlasmaResult) -> u8 {
    result.error_code
}

fn plasma_result_error_message(result: &RustPlasmaResult) -> &str {
    &result.error_message
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_store_ffi() {
        let store = object_store_new(1024 * 1024);
        assert!(object_store_is_empty(&store));
        assert_eq!(object_store_capacity(&store), 1024 * 1024);
    }

    #[test]
    fn test_create_and_seal() {
        let store = object_store_new(1024 * 1024);

        let object_id = ObjectId::from_random();
        let id_bytes = object_id.binary();

        let result = object_store_create_object(&store, &id_bytes, 100, 0, 0, &[]);
        assert!(plasma_result_success(&result));

        assert!(object_store_contains(&store, &id_bytes));
        assert!(!object_store_is_sealed(&store, &id_bytes));

        let result = object_store_seal_object(&store, &id_bytes);
        assert!(plasma_result_success(&result));
        assert!(object_store_is_sealed(&store, &id_bytes));
    }

    #[test]
    fn test_error_handling() {
        let store = object_store_new(100);

        let object_id = ObjectId::from_random();
        let id_bytes = object_id.binary();

        // Try to create object larger than capacity
        let result = object_store_create_object(&store, &id_bytes, 200, 0, 0, &[]);
        assert!(!plasma_result_success(&result));
        assert_eq!(plasma_result_error_code(&result), PLASMA_ERROR_OUT_OF_MEMORY);
    }
}
