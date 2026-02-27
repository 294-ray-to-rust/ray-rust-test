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

// Tests for Rust ObjectStore FFI implementation.
// These tests mirror the C++ plasma tests to verify functional equivalence.

#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "ray/ffi/rust_id.h"
#include "ray/ffi/rust_plasma.h"

namespace ray {

// Helper to generate a random object ID binary
std::string RandomObjectIdBinary() {
  RustObjectId obj_id = RustObjectId::FromRandom();
  return obj_id.Binary();
}

// ============================================================================
// Basic ObjectStore Tests
// ============================================================================

class RustObjectStoreTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // 1MB capacity store for tests
    store_ = std::make_unique<RustObjectStore>(1024 * 1024);
  }

  std::unique_ptr<RustObjectStore> store_;
};

TEST_F(RustObjectStoreTest, EmptyStore) {
  EXPECT_TRUE(store_->IsEmpty());
  EXPECT_EQ(store_->Size(), 0u);
  EXPECT_EQ(store_->Capacity(), 1024u * 1024u);
  EXPECT_EQ(store_->AvailableCapacity(), 1024u * 1024u);
}

TEST_F(RustObjectStoreTest, CreateObject) {
  std::string object_id = RandomObjectIdBinary();

  auto result = store_->CreateObject(
      object_id, 100, 0, RustObjectSourceEnum::CreatedByWorker, {});

  EXPECT_TRUE(result.ok());
  EXPECT_FALSE(store_->IsEmpty());
  EXPECT_EQ(store_->Size(), 1u);
  EXPECT_TRUE(store_->Contains(object_id));
  EXPECT_FALSE(store_->IsSealed(object_id));
}

TEST_F(RustObjectStoreTest, CreateObjectWithMetadata) {
  std::string object_id = RandomObjectIdBinary();

  auto result = store_->CreateObject(
      object_id, 100, 64, RustObjectSourceEnum::CreatedByWorker, {1, 2, 3, 4});

  EXPECT_TRUE(result.ok());
  EXPECT_TRUE(store_->Contains(object_id));
}

TEST_F(RustObjectStoreTest, CreateDuplicateObject) {
  std::string object_id = RandomObjectIdBinary();

  auto result1 = store_->CreateObject(
      object_id, 100, 0, RustObjectSourceEnum::CreatedByWorker, {});
  EXPECT_TRUE(result1.ok());

  auto result2 = store_->CreateObject(
      object_id, 100, 0, RustObjectSourceEnum::CreatedByWorker, {});
  EXPECT_FALSE(result2.ok());
  EXPECT_EQ(result2.error_code(), RustPlasmaErrorCode::ObjectExists);
}

TEST_F(RustObjectStoreTest, SealObject) {
  std::string object_id = RandomObjectIdBinary();

  store_->CreateObject(object_id, 100, 0, RustObjectSourceEnum::CreatedByWorker, {});
  EXPECT_FALSE(store_->IsSealed(object_id));

  auto result = store_->SealObject(object_id);
  EXPECT_TRUE(result.ok());
  EXPECT_TRUE(store_->IsSealed(object_id));
}

TEST_F(RustObjectStoreTest, SealNonexistentObject) {
  std::string object_id = RandomObjectIdBinary();

  auto result = store_->SealObject(object_id);
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.error_code(), RustPlasmaErrorCode::ObjectNotFound);
}

TEST_F(RustObjectStoreTest, SealAlreadySealedObject) {
  std::string object_id = RandomObjectIdBinary();

  store_->CreateObject(object_id, 100, 0, RustObjectSourceEnum::CreatedByWorker, {});
  store_->SealObject(object_id);

  auto result = store_->SealObject(object_id);
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.error_code(), RustPlasmaErrorCode::ObjectAlreadySealed);
}

TEST_F(RustObjectStoreTest, GetObject) {
  std::string object_id = RandomObjectIdBinary();

  store_->CreateObject(object_id, 100, 0, RustObjectSourceEnum::CreatedByWorker, {});

  // Cannot get unsealed object
  auto result1 = store_->GetObject(object_id);
  EXPECT_FALSE(result1.ok());
  EXPECT_EQ(result1.error_code(), RustPlasmaErrorCode::ObjectNotSealed);

  // Can get sealed object
  store_->SealObject(object_id);
  auto result2 = store_->GetObject(object_id);
  EXPECT_TRUE(result2.ok());
}

TEST_F(RustObjectStoreTest, GetNonexistentObject) {
  std::string object_id = RandomObjectIdBinary();

  auto result = store_->GetObject(object_id);
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.error_code(), RustPlasmaErrorCode::ObjectNotFound);
}

TEST_F(RustObjectStoreTest, ReleaseObject) {
  std::string object_id = RandomObjectIdBinary();

  store_->CreateObject(object_id, 100, 0, RustObjectSourceEnum::CreatedByWorker, {});
  store_->SealObject(object_id);
  store_->GetObject(object_id);  // Increment ref count

  auto result = store_->ReleaseObject(object_id);
  EXPECT_TRUE(result.ok());
}

TEST_F(RustObjectStoreTest, DeleteObject) {
  std::string object_id = RandomObjectIdBinary();

  store_->CreateObject(object_id, 100, 0, RustObjectSourceEnum::CreatedByWorker, {});
  store_->SealObject(object_id);

  EXPECT_TRUE(store_->Contains(object_id));

  auto result = store_->DeleteObject(object_id);
  EXPECT_TRUE(result.ok());
  EXPECT_FALSE(store_->Contains(object_id));
}

TEST_F(RustObjectStoreTest, DeleteNonexistentObject) {
  std::string object_id = RandomObjectIdBinary();

  auto result = store_->DeleteObject(object_id);
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.error_code(), RustPlasmaErrorCode::ObjectNotFound);
}

TEST_F(RustObjectStoreTest, DeleteReferencedObject) {
  std::string object_id = RandomObjectIdBinary();

  store_->CreateObject(object_id, 100, 0, RustObjectSourceEnum::CreatedByWorker, {});
  store_->SealObject(object_id);
  store_->GetObject(object_id);  // Increment ref count

  // Cannot delete referenced object
  auto result = store_->DeleteObject(object_id);
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.error_code(), RustPlasmaErrorCode::InvalidRequest);

  // Release and then delete succeeds
  store_->ReleaseObject(object_id);
  auto result2 = store_->DeleteObject(object_id);
  EXPECT_TRUE(result2.ok());
}

TEST_F(RustObjectStoreTest, AbortObject) {
  std::string object_id = RandomObjectIdBinary();

  store_->CreateObject(object_id, 100, 0, RustObjectSourceEnum::CreatedByWorker, {});
  EXPECT_TRUE(store_->Contains(object_id));

  auto result = store_->AbortObject(object_id);
  EXPECT_TRUE(result.ok());
  EXPECT_FALSE(store_->Contains(object_id));
}

TEST_F(RustObjectStoreTest, AbortSealedObject) {
  std::string object_id = RandomObjectIdBinary();

  store_->CreateObject(object_id, 100, 0, RustObjectSourceEnum::CreatedByWorker, {});
  store_->SealObject(object_id);

  auto result = store_->AbortObject(object_id);
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.error_code(), RustPlasmaErrorCode::ObjectAlreadySealed);
}

// ============================================================================
// Memory Management Tests
// ============================================================================

class RustObjectStoreMemoryTest : public ::testing::Test {};

TEST_F(RustObjectStoreMemoryTest, OutOfMemory) {
  RustObjectStore store(100);  // Very small store

  std::string object_id = RandomObjectIdBinary();

  auto result = store.CreateObject(
      object_id, 200, 0, RustObjectSourceEnum::CreatedByWorker, {});

  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.error_code(), RustPlasmaErrorCode::OutOfMemory);
}

TEST_F(RustObjectStoreMemoryTest, CapacityTracking) {
  RustObjectStore store(1000);

  std::string obj1 = RandomObjectIdBinary();
  std::string obj2 = RandomObjectIdBinary();

  EXPECT_EQ(store.AvailableCapacity(), 1000u);

  store.CreateObject(obj1, 100, 0, RustObjectSourceEnum::CreatedByWorker, {});
  EXPECT_EQ(store.AvailableCapacity(), 900u);

  store.CreateObject(obj2, 200, 0, RustObjectSourceEnum::CreatedByWorker, {});
  EXPECT_EQ(store.AvailableCapacity(), 700u);

  store.SealObject(obj1);
  store.DeleteObject(obj1);
  EXPECT_EQ(store.AvailableCapacity(), 800u);
}

TEST_F(RustObjectStoreMemoryTest, Eviction) {
  RustObjectStore store(500);

  // Create and seal multiple objects
  std::vector<std::string> object_ids;
  for (int i = 0; i < 5; i++) {
    std::string obj_id = RandomObjectIdBinary();
    store.CreateObject(obj_id, 80, 0, RustObjectSourceEnum::CreatedByWorker, {});
    store.SealObject(obj_id);
    object_ids.push_back(obj_id);
  }

  EXPECT_EQ(store.Size(), 5u);

  // Evict enough space for a new 200 byte object
  size_t evicted = store.Evict(200);
  EXPECT_GE(evicted, 160u);  // At least 2 objects evicted
  EXPECT_LT(store.Size(), 5u);
}

// ============================================================================
// Statistics Tests
// ============================================================================

class RustObjectStoreStatsTest : public ::testing::Test {};

TEST_F(RustObjectStoreStatsTest, InitialStats) {
  RustObjectStore store(1024 * 1024);

  auto stats = store.GetStats();
  EXPECT_EQ(stats.num_objects(), 0u);
  EXPECT_EQ(stats.num_bytes_used(), 0);
  EXPECT_EQ(stats.num_objects_sealed(), 0u);
}

TEST_F(RustObjectStoreStatsTest, CreateUpdatesStats) {
  RustObjectStore store(1024 * 1024);
  std::string object_id = RandomObjectIdBinary();

  store.CreateObject(object_id, 100, 0, RustObjectSourceEnum::CreatedByWorker, {});

  auto stats = store.GetStats();
  EXPECT_EQ(stats.num_objects(), 1u);
  EXPECT_EQ(stats.num_bytes_used(), 100);
  EXPECT_EQ(stats.num_bytes_created_total(), 100);
  EXPECT_EQ(stats.num_objects_created_total(), 1u);
}

TEST_F(RustObjectStoreStatsTest, SealUpdatesStats) {
  RustObjectStore store(1024 * 1024);
  std::string object_id = RandomObjectIdBinary();

  store.CreateObject(object_id, 100, 0, RustObjectSourceEnum::CreatedByWorker, {});
  store.SealObject(object_id);

  auto stats = store.GetStats();
  EXPECT_EQ(stats.num_objects_sealed(), 1u);
  EXPECT_EQ(stats.num_bytes_sealed(), 100);
}

TEST_F(RustObjectStoreStatsTest, DeleteUpdatesStats) {
  RustObjectStore store(1024 * 1024);
  std::string object_id = RandomObjectIdBinary();

  store.CreateObject(object_id, 100, 0, RustObjectSourceEnum::CreatedByWorker, {});
  store.SealObject(object_id);

  auto stats1 = store.GetStats();
  EXPECT_EQ(stats1.num_objects(), 1u);
  EXPECT_EQ(stats1.num_bytes_used(), 100);

  store.DeleteObject(object_id);

  auto stats2 = store.GetStats();
  EXPECT_EQ(stats2.num_objects(), 0u);
  EXPECT_EQ(stats2.num_bytes_used(), 0);
  // Total created should still be 1
  EXPECT_EQ(stats2.num_objects_created_total(), 1u);
}

// ============================================================================
// Object Source Tests
// ============================================================================

class RustObjectSourceTest : public ::testing::Test {};

TEST_F(RustObjectSourceTest, AllSourceTypes) {
  RustObjectStore store(1024 * 1024);

  auto sources = {
      RustObjectSourceEnum::CreatedByWorker,
      RustObjectSourceEnum::RestoredFromStorage,
      RustObjectSourceEnum::ReceivedFromRemoteRaylet,
      RustObjectSourceEnum::ErrorStoredByRaylet,
      RustObjectSourceEnum::CreatedByPlasmaFallbackAllocation,
  };

  for (auto source : sources) {
    std::string obj_id = RandomObjectIdBinary();
    auto result = store.CreateObject(obj_id, 100, 0, source, {});
    EXPECT_TRUE(result.ok());
  }

  EXPECT_EQ(store.Size(), 5u);
}

// ============================================================================
// Pass-through Tests (Complete Lifecycle)
// ============================================================================

class RustObjectStorePassThroughTest : public ::testing::Test {};

TEST_F(RustObjectStorePassThroughTest, CompleteLifecycle) {
  RustObjectStore store(1024 * 1024);
  std::string object_id = RandomObjectIdBinary();

  // Create
  auto create_result = store.CreateObject(
      object_id, 100, 64, RustObjectSourceEnum::CreatedByWorker, {1, 2, 3});
  EXPECT_TRUE(create_result.ok());
  EXPECT_TRUE(store.Contains(object_id));
  EXPECT_FALSE(store.IsSealed(object_id));

  // Seal
  auto seal_result = store.SealObject(object_id);
  EXPECT_TRUE(seal_result.ok());
  EXPECT_TRUE(store.IsSealed(object_id));

  // Get (increments ref count)
  auto get_result = store.GetObject(object_id);
  EXPECT_TRUE(get_result.ok());

  // Release
  auto release_result = store.ReleaseObject(object_id);
  EXPECT_TRUE(release_result.ok());

  // Delete
  auto delete_result = store.DeleteObject(object_id);
  EXPECT_TRUE(delete_result.ok());
  EXPECT_FALSE(store.Contains(object_id));
}

TEST_F(RustObjectStorePassThroughTest, MultipleObjects) {
  RustObjectStore store(1024 * 1024);

  const int num_objects = 10;
  std::vector<std::string> object_ids;

  // Create all objects
  for (int i = 0; i < num_objects; i++) {
    std::string obj_id = RandomObjectIdBinary();
    auto result = store.CreateObject(
        obj_id, 100 + i, 0, RustObjectSourceEnum::CreatedByWorker, {});
    EXPECT_TRUE(result.ok());
    object_ids.push_back(obj_id);
  }

  EXPECT_EQ(store.Size(), static_cast<size_t>(num_objects));

  // Seal all objects
  for (const auto &obj_id : object_ids) {
    auto result = store.SealObject(obj_id);
    EXPECT_TRUE(result.ok());
  }

  // Delete all objects
  for (const auto &obj_id : object_ids) {
    auto result = store.DeleteObject(obj_id);
    EXPECT_TRUE(result.ok());
  }

  EXPECT_TRUE(store.IsEmpty());
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
