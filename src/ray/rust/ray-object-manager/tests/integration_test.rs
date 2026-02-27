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

//! Integration tests for the ray-object-manager crate.

use ray_common::ObjectId;
use ray_object_manager::{
    Allocator, HeapAllocator, ObjectSource, ObjectStore, ObjectStoreConfig, PlasmaError,
};
use std::sync::Arc;
use std::thread;

#[test]
fn test_object_store_basic_workflow() {
    let store = ObjectStore::with_capacity(10 * 1024 * 1024); // 10MB

    // Create an object
    let object_id = ObjectId::from_random();
    store
        .create_object(
            object_id.clone(),
            1024,
            64,
            ObjectSource::CreatedByWorker,
            vec![1, 2, 3, 4],
        )
        .unwrap();

    // Verify object exists but is not sealed
    assert!(store.contains(&object_id));
    assert!(!store.is_sealed(&object_id));

    // Try to get - should fail (not sealed)
    let result = store.get_object(&object_id);
    assert!(matches!(result, Err(PlasmaError::ObjectNotSealed(_))));

    // Seal the object
    store.seal_object(&object_id).unwrap();
    assert!(store.is_sealed(&object_id));

    // Now we can get it
    let obj_ref = store.get_object(&object_id).unwrap();
    assert_eq!(obj_ref.data_size, 1024);
    assert_eq!(obj_ref.metadata_size, 64);

    // Release and delete
    store.release_object(&object_id).unwrap();
    store.delete_object(&object_id).unwrap();
    assert!(!store.contains(&object_id));
}

#[test]
fn test_object_store_concurrent_access() {
    let store = Arc::new(ObjectStore::with_capacity(10 * 1024 * 1024));
    let num_threads = 4;
    let objects_per_thread = 100;

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let store = Arc::clone(&store);
            thread::spawn(move || {
                for i in 0..objects_per_thread {
                    let object_id = ObjectId::from_random();

                    // Create object
                    store
                        .create_object(
                            object_id.clone(),
                            100 + i,
                            10,
                            ObjectSource::CreatedByWorker,
                            vec![thread_id as u8],
                        )
                        .unwrap();

                    // Seal it
                    store.seal_object(&object_id).unwrap();

                    // Get and release
                    let _ref = store.get_object(&object_id).unwrap();
                    store.release_object(&object_id).unwrap();

                    // Delete
                    store.delete_object(&object_id).unwrap();
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // Store should be empty
    assert!(store.is_empty());
}

#[test]
fn test_heap_allocator_stress() {
    let allocator = HeapAllocator::new(1024 * 1024); // 1MB

    let mut allocations = Vec::new();

    // Allocate many small blocks
    for i in 0..100 {
        let size = (i % 10 + 1) * 100;
        if let Ok(alloc) = allocator.allocate(size) {
            allocations.push(alloc);
        }
    }

    let peak = allocator.stats().peak_bytes.load(std::sync::atomic::Ordering::SeqCst);
    assert!(peak > 0);

    // Free half
    let to_free: Vec<_> = allocations.drain(..allocations.len() / 2).collect();
    for alloc in to_free {
        allocator.free(&alloc).unwrap();
    }

    // Allocate more
    for _ in 0..20 {
        if let Ok(alloc) = allocator.allocate(500) {
            allocations.push(alloc);
        }
    }

    // Free all
    for alloc in allocations {
        allocator.free(&alloc).unwrap();
    }

    assert_eq!(
        allocator.stats().bytes_allocated.load(std::sync::atomic::Ordering::SeqCst),
        0
    );
}

#[test]
fn test_object_store_eviction() {
    let config = ObjectStoreConfig {
        capacity: 1000,
        ..Default::default()
    };
    let store = ObjectStore::new(config);

    // Create objects that fill the store
    let mut object_ids = Vec::new();
    for _ in 0..5 {
        let object_id = ObjectId::from_random();
        store
            .create_object(
                object_id.clone(),
                100,
                0,
                ObjectSource::CreatedByWorker,
                vec![],
            )
            .unwrap();
        store.seal_object(&object_id).unwrap();
        object_ids.push(object_id);
    }

    assert_eq!(store.len(), 5);

    // Evict enough for a new 200 byte object
    let freed = store.evict(200).unwrap();
    assert!(freed >= 200);

    // Some objects should have been evicted
    assert!(store.len() < 5);
}

#[test]
fn test_object_store_stats() {
    let store = ObjectStore::with_capacity(1024 * 1024);

    // Initial stats
    assert_eq!(store.stats().num_objects(), 0);
    assert_eq!(store.stats().bytes_used(), 0);

    // Create and seal objects
    let obj1 = ObjectId::from_random();
    let obj2 = ObjectId::from_random();

    store
        .create_object(obj1.clone(), 100, 0, ObjectSource::CreatedByWorker, vec![])
        .unwrap();
    store
        .create_object(obj2.clone(), 200, 0, ObjectSource::CreatedByWorker, vec![])
        .unwrap();

    assert_eq!(store.stats().num_objects(), 2);
    assert_eq!(store.stats().bytes_used(), 300);

    store.seal_object(&obj1).unwrap();
    store.seal_object(&obj2).unwrap();

    assert_eq!(
        store.stats().num_objects_sealed.load(std::sync::atomic::Ordering::SeqCst),
        2
    );

    // Delete one
    store.delete_object(&obj1).unwrap();

    assert_eq!(store.stats().num_objects(), 1);
    assert_eq!(store.stats().bytes_used(), 200);
}

#[test]
fn test_object_abort() {
    let store = ObjectStore::with_capacity(1024 * 1024);
    let object_id = ObjectId::from_random();

    // Create object
    store
        .create_object(
            object_id.clone(),
            100,
            0,
            ObjectSource::CreatedByWorker,
            vec![],
        )
        .unwrap();

    assert!(store.contains(&object_id));

    // Abort before sealing
    store.abort_object(&object_id).unwrap();

    assert!(!store.contains(&object_id));
    assert_eq!(store.stats().num_objects(), 0);
}

#[test]
fn test_object_sources() {
    let store = ObjectStore::with_capacity(1024 * 1024);

    let sources = [
        ObjectSource::CreatedByWorker,
        ObjectSource::RestoredFromStorage,
        ObjectSource::ReceivedFromRemoteRaylet,
        ObjectSource::ErrorStoredByRaylet,
        ObjectSource::CreatedByPlasmaFallbackAllocation,
    ];

    for source in sources {
        let object_id = ObjectId::from_random();
        store
            .create_object(object_id.clone(), 100, 0, source, vec![])
            .unwrap();
        store.seal_object(&object_id).unwrap();
    }

    assert_eq!(store.len(), 5);
}

#[test]
fn test_allocator_trait() {
    fn use_allocator<A: Allocator>(allocator: &A) -> bool {
        let alloc = allocator.allocate(100);
        match alloc {
            Ok(a) => {
                let _ = allocator.free(&a);
                true
            }
            Err(_) => false,
        }
    }

    let heap = HeapAllocator::new(1024);
    assert!(use_allocator(&heap));

    let null = ray_object_manager::NullAllocator;
    assert!(!use_allocator(&null));
}
