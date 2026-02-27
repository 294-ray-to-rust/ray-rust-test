# Ray Rust FFI Tests

## Test Locations

C++ tests that call Rust code through FFI:

| Our FFI Test | Ray's Original C++ Test | Tests |
|--------------|-------------------------|-------|
| `src/ray/ffi/tests/rust_status_test.cc` | `src/ray/common/tests/status_test.cc` | 5 |
| `src/ray/ffi/tests/rust_id_test.cc` | `src/ray/common/tests/id_test.cc` | 30 |
| `src/ray/ffi/tests/rust_scheduling_ids_test.cc` | `src/ray/common/scheduling/tests/scheduling_ids_test.cc` | 16 |
| `src/ray/ffi/tests/rust_resource_set_test.cc` | `src/ray/common/scheduling/tests/resource_set_test.cc` | 10 |
| `src/ray/ffi/tests/rust_resource_request_test.cc` | `src/ray/common/scheduling/tests/resource_request_test.cc` | 7 |
| `src/ray/ffi/tests/rust_resource_instance_set_test.cc` | `src/ray/common/scheduling/tests/resource_instance_set_test.cc` | 12 |
| `src/ray/ffi/tests/rust_object_store_test.cc` | `src/ray/object_manager/plasma/tests/object_store_test.cc` | 25 |
| `src/ray/ffi/tests/rust_eviction_policy_test.cc` | `src/ray/object_manager/plasma/tests/eviction_policy_test.cc` | 9 |

**Total: 114 tests**

## Running the Tests

### Build and run with Docker:

```bash
# Build the Docker image (from repo root)
docker build -f Dockerfile.test -t ray-rust-test .

# Run all FFI tests
docker run --rm ray-rust-test bash -c "cd /ray && bazel test //src/ray/ffi/tests:all --test_output=all"

# Run individual test suites
docker run --rm ray-rust-test bash -c "cd /ray && bazel test //src/ray/ffi/tests:rust_status_test --test_output=all"
docker run --rm ray-rust-test bash -c "cd /ray && bazel test //src/ray/ffi/tests:rust_id_test --test_output=all"
docker run --rm ray-rust-test bash -c "cd /ray && bazel test //src/ray/ffi/tests:rust_scheduling_ids_test --test_output=all"
docker run --rm ray-rust-test bash -c "cd /ray && bazel test //src/ray/ffi/tests:rust_resource_set_test --test_output=all"
docker run --rm ray-rust-test bash -c "cd /ray && bazel test //src/ray/ffi/tests:rust_resource_request_test --test_output=all"
docker run --rm ray-rust-test bash -c "cd /ray && bazel test //src/ray/ffi/tests:rust_resource_instance_set_test --test_output=all"
docker run --rm ray-rust-test bash -c "cd /ray && bazel test //src/ray/ffi/tests:rust_object_store_test --test_output=all"
docker run --rm ray-rust-test bash -c "cd /ray && bazel test //src/ray/ffi/tests:rust_eviction_policy_test --test_output=all"
```

### Expected output:

```
Executed 8 out of 8 tests: 8 tests pass.
```

## Batch Status

### Batch 1: Common + Util (Complete)
- Status (5 tests)
- ID Types - ObjectId, TaskId, ActorId, WorkerId, NodeId, etc. (30 tests)
- Scheduling - FixedPoint, ResourceId, ResourceSet, NodeResourceSet (26 tests)
- ResourceRequest, TaskResourceInstances (7 tests)
- NodeResourceInstanceSet (12 tests)

### Batch 2: Object Manager + Plasma (In Progress)
**Completed:**
- ObjectStore/Plasma (25 tests)
- LRUCache/Eviction Policy (9 tests)

**Remaining Plasma tests:**
- `fallback_allocator_test.cc` (~3 tests)
- `mutable_object_test.cc` (~4 tests)
- `obj_lifecycle_mgr_test.cc` (~5 tests)
- `stats_collector_test.cc` (~3 tests)

**Object Manager tests (not started):**
- `pull_manager_test.cc` (29 tests)
- `push_manager_test.cc` (8 tests)
- `object_manager_test.cc` (1 test)
- `object_buffer_pool_test.cc` (4 tests)
- `create_request_queue_test.cc` (13 tests)
- `get_request_queue_test.cc` (7 tests)
- `ownership_object_directory_test.cc` (8 tests)
- `spilled_object_test.cc` (14 tests)
