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

#include "src/ray/ffi/rust_lifecycle.h"

#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "src/ray/ffi/rust_id.h"

using namespace ray;

class RustLifecycleManagerTest : public ::testing::Test {
 protected:
  // Helper to create a random object ID binary
  std::string RandomObjectIdBinary() {
    RustObjectId id = RustObjectId::FromRandom();
    return id.Binary();
  }
};

TEST_F(RustLifecycleManagerTest, CreateAndContains) {
  RustObjectLifecycleManager manager(1024 * 1024);  // 1MB

  EXPECT_TRUE(manager.IsEmpty());
  EXPECT_EQ(manager.Size(), 0);

  std::string id = RandomObjectIdBinary();
  auto result = manager.CreateObject(id, 100, 0, RustLifecycleObjectSource::CreatedByWorker);
  EXPECT_TRUE(result.ok());

  EXPECT_TRUE(manager.Contains(id));
  EXPECT_FALSE(manager.IsEmpty());
  EXPECT_EQ(manager.Size(), 1);
}

TEST_F(RustLifecycleManagerTest, CreateDuplicateFails) {
  RustObjectLifecycleManager manager(1024 * 1024);

  std::string id = RandomObjectIdBinary();
  auto result1 = manager.CreateObject(id, 100, 0, RustLifecycleObjectSource::CreatedByWorker);
  EXPECT_TRUE(result1.ok());

  auto result2 = manager.CreateObject(id, 100, 0, RustLifecycleObjectSource::CreatedByWorker);
  EXPECT_FALSE(result2.ok());
  EXPECT_EQ(result2.error_code(), RustLifecycleErrorCode::ObjectExists);
}

TEST_F(RustLifecycleManagerTest, SealObject) {
  RustObjectLifecycleManager manager(1024 * 1024);

  std::string id = RandomObjectIdBinary();
  manager.CreateObject(id, 100, 0, RustLifecycleObjectSource::CreatedByWorker);

  auto result = manager.SealObject(id);
  EXPECT_TRUE(result.ok());

  // Double seal should fail
  auto result2 = manager.SealObject(id);
  EXPECT_FALSE(result2.ok());
  EXPECT_EQ(result2.error_code(), RustLifecycleErrorCode::ObjectAlreadySealed);
}

TEST_F(RustLifecycleManagerTest, AbortObject) {
  RustObjectLifecycleManager manager(1024 * 1024);

  std::string id = RandomObjectIdBinary();
  manager.CreateObject(id, 100, 0, RustLifecycleObjectSource::CreatedByWorker);

  EXPECT_TRUE(manager.Contains(id));

  auto result = manager.AbortObject(id);
  EXPECT_TRUE(result.ok());

  EXPECT_FALSE(manager.Contains(id));
}

TEST_F(RustLifecycleManagerTest, AbortSealedObjectFails) {
  RustObjectLifecycleManager manager(1024 * 1024);

  std::string id = RandomObjectIdBinary();
  manager.CreateObject(id, 100, 0, RustLifecycleObjectSource::CreatedByWorker);
  manager.SealObject(id);

  auto result = manager.AbortObject(id);
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.error_code(), RustLifecycleErrorCode::ObjectAlreadySealed);
}

TEST_F(RustLifecycleManagerTest, DeleteObject) {
  RustObjectLifecycleManager manager(1024 * 1024);

  std::string id = RandomObjectIdBinary();
  manager.CreateObject(id, 100, 0, RustLifecycleObjectSource::CreatedByWorker);
  manager.SealObject(id);

  auto result = manager.DeleteObject(id);
  EXPECT_TRUE(result.ok());

  EXPECT_FALSE(manager.Contains(id));
}

TEST_F(RustLifecycleManagerTest, DeleteUnsealedFails) {
  RustObjectLifecycleManager manager(1024 * 1024);

  std::string id = RandomObjectIdBinary();
  manager.CreateObject(id, 100, 0, RustLifecycleObjectSource::CreatedByWorker);

  auto result = manager.DeleteObject(id);
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.error_code(), RustLifecycleErrorCode::ObjectNotSealed);
}

TEST_F(RustLifecycleManagerTest, ReferenceCountingBasic) {
  RustObjectLifecycleManager manager(1024 * 1024);

  std::string id = RandomObjectIdBinary();
  manager.CreateObject(id, 100, 0, RustLifecycleObjectSource::CreatedByWorker);
  manager.SealObject(id);

  // Add reference
  EXPECT_TRUE(manager.AddReference(id));

  // Can't delete object with active reference - marks for eager deletion
  auto delete_result = manager.DeleteObject(id);
  EXPECT_FALSE(delete_result.ok());
  EXPECT_EQ(delete_result.error_code(), RustLifecycleErrorCode::InvalidRequest);

  // Remove reference - triggers eager deletion automatically
  EXPECT_TRUE(manager.RemoveReference(id));

  // Object should now be deleted (eager deletion happened)
  EXPECT_FALSE(manager.Contains(id));
}

TEST_F(RustLifecycleManagerTest, MultipleReferences) {
  RustObjectLifecycleManager manager(1024 * 1024);

  std::string id = RandomObjectIdBinary();
  manager.CreateObject(id, 100, 0, RustLifecycleObjectSource::CreatedByWorker);
  manager.SealObject(id);

  // Add multiple references
  EXPECT_TRUE(manager.AddReference(id));
  EXPECT_TRUE(manager.AddReference(id));
  EXPECT_TRUE(manager.AddReference(id));

  // Remove references one by one
  EXPECT_TRUE(manager.RemoveReference(id));
  EXPECT_TRUE(manager.RemoveReference(id));

  // Still can't delete with one reference left - marks for eager deletion
  auto delete_result = manager.DeleteObject(id);
  EXPECT_FALSE(delete_result.ok());

  // Remove last reference - triggers eager deletion automatically
  EXPECT_TRUE(manager.RemoveReference(id));

  // Object should now be deleted (eager deletion happened)
  EXPECT_FALSE(manager.Contains(id));
}

TEST_F(RustLifecycleManagerTest, StatsTrackingBasic) {
  RustObjectLifecycleManager manager(1024 * 1024);

  auto stats = manager.GetStats();
  EXPECT_EQ(stats.num_bytes_created_total(), 0);
  EXPECT_EQ(stats.num_objects_unsealed(), 0);

  // Create object
  std::string id = RandomObjectIdBinary();
  manager.CreateObject(id, 100, 0, RustLifecycleObjectSource::CreatedByWorker);

  stats = manager.GetStats();
  EXPECT_EQ(stats.num_bytes_created_total(), 100);
  EXPECT_EQ(stats.num_objects_unsealed(), 1);
  EXPECT_EQ(stats.num_bytes_unsealed(), 100);
  EXPECT_EQ(stats.num_objects_created_by_worker(), 1);

  // Seal object
  manager.SealObject(id);

  stats = manager.GetStats();
  EXPECT_EQ(stats.num_objects_unsealed(), 0);
  EXPECT_EQ(stats.num_objects_evictable(), 1);  // No references = evictable
}

TEST_F(RustLifecycleManagerTest, StatsTrackingBySource) {
  RustObjectLifecycleManager manager(1024 * 1024);

  std::string id1 = RandomObjectIdBinary();
  std::string id2 = RandomObjectIdBinary();
  std::string id3 = RandomObjectIdBinary();
  std::string id4 = RandomObjectIdBinary();

  manager.CreateObject(id1, 100, 0, RustLifecycleObjectSource::CreatedByWorker);
  manager.CreateObject(id2, 200, 0, RustLifecycleObjectSource::RestoredFromStorage);
  manager.CreateObject(id3, 300, 0, RustLifecycleObjectSource::ReceivedFromRemoteRaylet);
  manager.CreateObject(id4, 400, 0, RustLifecycleObjectSource::ErrorStoredByRaylet);

  auto stats = manager.GetStats();
  EXPECT_EQ(stats.num_objects_created_by_worker(), 1);
  EXPECT_EQ(stats.num_bytes_created_by_worker(), 100);
  EXPECT_EQ(stats.num_objects_restored(), 1);
  EXPECT_EQ(stats.num_bytes_restored(), 200);
  EXPECT_EQ(stats.num_objects_received(), 1);
  EXPECT_EQ(stats.num_bytes_received(), 300);
  EXPECT_EQ(stats.num_objects_errored(), 1);
  EXPECT_EQ(stats.num_bytes_errored(), 400);

  // Total bytes created
  EXPECT_EQ(stats.num_bytes_created_total(), 1000);
}

TEST_F(RustLifecycleManagerTest, StatsInUseTracking) {
  RustObjectLifecycleManager manager(1024 * 1024);

  std::string id = RandomObjectIdBinary();
  manager.CreateObject(id, 100, 0, RustLifecycleObjectSource::CreatedByWorker);
  manager.SealObject(id);

  auto stats = manager.GetStats();
  EXPECT_EQ(stats.num_objects_in_use(), 0);
  EXPECT_EQ(stats.num_bytes_in_use(), 0);

  // Add reference - now in use
  manager.AddReference(id);

  stats = manager.GetStats();
  EXPECT_EQ(stats.num_objects_in_use(), 1);
  EXPECT_EQ(stats.num_bytes_in_use(), 100);
  EXPECT_EQ(stats.num_objects_evictable(), 0);  // In use = not evictable

  // Remove reference - no longer in use
  manager.RemoveReference(id);

  stats = manager.GetStats();
  EXPECT_EQ(stats.num_objects_in_use(), 0);
  EXPECT_EQ(stats.num_bytes_in_use(), 0);
  EXPECT_EQ(stats.num_objects_evictable(), 1);  // No refs = evictable
}

TEST_F(RustLifecycleManagerTest, StatsAfterDelete) {
  RustObjectLifecycleManager manager(1024 * 1024);

  std::string id = RandomObjectIdBinary();
  manager.CreateObject(id, 100, 0, RustLifecycleObjectSource::CreatedByWorker);
  manager.SealObject(id);

  auto stats = manager.GetStats();
  EXPECT_EQ(stats.num_objects_created_by_worker(), 1);
  EXPECT_EQ(stats.num_objects_evictable(), 1);

  manager.DeleteObject(id);

  stats = manager.GetStats();
  EXPECT_EQ(stats.num_objects_created_by_worker(), 0);
  EXPECT_EQ(stats.num_objects_evictable(), 0);
}

TEST_F(RustLifecycleManagerTest, NonexistentObjectOperations) {
  RustObjectLifecycleManager manager(1024 * 1024);

  std::string id = RandomObjectIdBinary();

  // Operations on non-existent object should fail
  auto seal_result = manager.SealObject(id);
  EXPECT_FALSE(seal_result.ok());
  EXPECT_EQ(seal_result.error_code(), RustLifecycleErrorCode::ObjectNotFound);

  auto abort_result = manager.AbortObject(id);
  EXPECT_FALSE(abort_result.ok());
  EXPECT_EQ(abort_result.error_code(), RustLifecycleErrorCode::ObjectNotFound);

  auto delete_result = manager.DeleteObject(id);
  EXPECT_FALSE(delete_result.ok());
  EXPECT_EQ(delete_result.error_code(), RustLifecycleErrorCode::ObjectNotFound);

  EXPECT_FALSE(manager.AddReference(id));
  EXPECT_FALSE(manager.RemoveReference(id));
}

TEST_F(RustLifecycleManagerTest, MultipleObjectsLifecycle) {
  RustObjectLifecycleManager manager(1024 * 1024);

  std::vector<std::string> ids;
  for (int i = 0; i < 10; i++) {
    ids.push_back(RandomObjectIdBinary());
  }

  // Create all objects
  for (const auto &id : ids) {
    auto result =
        manager.CreateObject(id, 100, 0, RustLifecycleObjectSource::CreatedByWorker);
    EXPECT_TRUE(result.ok());
  }
  EXPECT_EQ(manager.Size(), 10);

  // Seal all objects
  for (const auto &id : ids) {
    auto result = manager.SealObject(id);
    EXPECT_TRUE(result.ok());
  }

  // Add references to half
  for (size_t i = 0; i < 5; i++) {
    manager.AddReference(ids[i]);
  }

  auto stats = manager.GetStats();
  EXPECT_EQ(stats.num_objects_in_use(), 5);
  EXPECT_EQ(stats.num_objects_evictable(), 5);

  // Delete the ones without references
  for (size_t i = 5; i < 10; i++) {
    auto result = manager.DeleteObject(ids[i]);
    EXPECT_TRUE(result.ok());
  }
  EXPECT_EQ(manager.Size(), 5);

  // Remove references and delete the rest
  for (size_t i = 0; i < 5; i++) {
    manager.RemoveReference(ids[i]);
    auto result = manager.DeleteObject(ids[i]);
    EXPECT_TRUE(result.ok());
  }
  EXPECT_TRUE(manager.IsEmpty());
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
