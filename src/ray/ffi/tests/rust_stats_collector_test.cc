// Copyright 2025 The Ray Authors.
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

// Rust FFI tests for StatsCollector.
// Matches: src/ray/object_manager/plasma/tests/stats_collector_test.cc

#include "gtest/gtest.h"
#include "src/ray/ffi/rust_lifecycle.h"

#include <cstring>
#include <random>
#include <unordered_set>
#include <vector>

namespace ray {
namespace {

// ObjectSource enum values (must match Rust)
constexpr uint8_t SOURCE_CREATED_BY_WORKER = 0;
constexpr uint8_t SOURCE_RESTORED_FROM_STORAGE = 1;
constexpr uint8_t SOURCE_RECEIVED_FROM_REMOTE = 2;
constexpr uint8_t SOURCE_ERROR_STORED = 3;

// Helper to generate random object IDs (28 bytes for ObjectId)
std::vector<uint8_t> GenerateRandomObjectId() {
  static std::mt19937 gen(std::random_device{}());
  std::vector<uint8_t> id(28);
  for (auto &byte : id) {
    byte = static_cast<uint8_t>(gen() % 256);
  }
  return id;
}

int64_t Random(int64_t max) {
  static std::mt19937 gen(std::random_device{}());
  std::uniform_int_distribution<int64_t> dist(1, max);
  return dist(gen);
}

class RustStatsCollectorTest : public ::testing::Test {
 protected:
  void SetUp() override { Reset(); }

  void Reset() {
    manager_ = RustObjectLifecycleManager(1024 * 1024);  // 1MB capacity
    used_ids_.clear();
    num_bytes_created_total_ = 0;
  }

  std::vector<uint8_t> CreateNewObjectId() {
    auto id = GenerateRandomObjectId();
    while (used_ids_.count(id) > 0) {
      id = GenerateRandomObjectId();
    }
    used_ids_.insert(id);
    return id;
  }

  void ExpectStatsConsistent() {
    auto stats = manager_.GetStats();

    // Verify that num_bytes_created_total matches what we tracked
    EXPECT_EQ(num_bytes_created_total_, stats.NumBytesCreatedTotal());

    // Verify stats counters are non-negative
    EXPECT_GE(stats.NumBytesInUse(), 0);
    EXPECT_GE(stats.NumBytesSpillable(), 0);
    EXPECT_GE(stats.NumBytesEvictable(), 0);
    EXPECT_GE(stats.NumBytesUnsealed(), 0);
  }

  RustObjectLifecycleManager manager_{1024 * 1024};
  std::set<std::vector<uint8_t>> used_ids_;
  int64_t num_bytes_created_total_ = 0;
};

TEST_F(RustStatsCollectorTest, CreateAndAbort) {
  std::vector<uint8_t> sources = {
      SOURCE_CREATED_BY_WORKER,
      SOURCE_RESTORED_FROM_STORAGE,
      SOURCE_RECEIVED_FROM_REMOTE,
      SOURCE_ERROR_STORED,
  };

  std::vector<std::vector<uint8_t>> created_ids;

  for (auto source : sources) {
    int64_t size = Random(100);
    auto id = CreateNewObjectId();
    auto result = manager_.CreateObject(id, size, 0, source, false);
    ASSERT_TRUE(result.Success()) << result.ErrorMessage();
    num_bytes_created_total_ += size;
    created_ids.push_back(id);
    ExpectStatsConsistent();
  }

  // Abort all created objects
  for (const auto &id : created_ids) {
    auto result = manager_.AbortObject(id);
    ASSERT_TRUE(result.Success()) << result.ErrorMessage();
    ExpectStatsConsistent();
  }
}

TEST_F(RustStatsCollectorTest, CreateAndDelete) {
  std::vector<uint8_t> sources = {
      SOURCE_CREATED_BY_WORKER,
      SOURCE_RESTORED_FROM_STORAGE,
      SOURCE_RECEIVED_FROM_REMOTE,
      SOURCE_ERROR_STORED,
  };

  std::vector<std::vector<uint8_t>> created_ids;

  for (auto source : sources) {
    int64_t size = Random(100);
    auto id = CreateNewObjectId();
    auto result = manager_.CreateObject(id, size, 0, source, false);
    ASSERT_TRUE(result.Success()) << result.ErrorMessage();
    num_bytes_created_total_ += size;
    created_ids.push_back(id);
    ExpectStatsConsistent();
  }

  // Add refs, seal, then remove refs and delete all
  for (const auto &id : created_ids) {
    int64_t ref_count = Random(3);
    for (int64_t i = 0; i < ref_count; i++) {
      manager_.AddReference(id);
    }
    // Always seal before delete (required by the implementation)
    manager_.SealObject(id);
    // Remove all added references before delete
    for (int64_t i = 0; i < ref_count; i++) {
      manager_.RemoveReference(id);
    }
    // Remove the initial reference
    manager_.RemoveReference(id);
    auto result = manager_.DeleteObject(id);
    ASSERT_TRUE(result.Success()) << result.ErrorMessage();
    ExpectStatsConsistent();
  }
}

TEST_F(RustStatsCollectorTest, Eviction) {
  std::vector<uint8_t> sources = {
      SOURCE_CREATED_BY_WORKER,
      SOURCE_RESTORED_FROM_STORAGE,
      SOURCE_RECEIVED_FROM_REMOTE,
      SOURCE_ERROR_STORED,
  };

  int64_t size = 100;
  std::vector<std::vector<uint8_t>> created_ids;

  for (auto source : sources) {
    auto id = CreateNewObjectId();
    auto result = manager_.CreateObject(id, size++, 0, source, false);
    ASSERT_TRUE(result.Success()) << result.ErrorMessage();
    num_bytes_created_total_ += (size - 1);
    created_ids.push_back(id);
    ExpectStatsConsistent();
  }

  // Seal and evict all objects
  for (const auto &id : created_ids) {
    manager_.SealObject(id);
    manager_.EvictObject(id);
    ExpectStatsConsistent();
  }
}

TEST_F(RustStatsCollectorTest, RefCountPassThrough) {
  // Create object 1 (CreatedByWorker)
  auto id1 = CreateNewObjectId();
  auto result1 = manager_.CreateObject(id1, 100, 0, SOURCE_CREATED_BY_WORKER, false);
  ASSERT_TRUE(result1.Success());
  num_bytes_created_total_ += 100;

  // Create object 2 (RestoredFromStorage)
  auto id2 = CreateNewObjectId();
  auto result2 = manager_.CreateObject(id2, 200, 0, SOURCE_RESTORED_FROM_STORAGE, false);
  ASSERT_TRUE(result2.Success());
  num_bytes_created_total_ += 200;
  ExpectStatsConsistent();

  // Add reference to object 1
  manager_.AddReference(id1);
  ExpectStatsConsistent();

  // Seal object 1
  manager_.SealObject(id1);
  ExpectStatsConsistent();

  // Add another reference to object 1
  manager_.AddReference(id1);
  ExpectStatsConsistent();

  // Add reference to object 2
  manager_.AddReference(id2);
  ExpectStatsConsistent();

  // Seal object 2
  manager_.SealObject(id2);
  ExpectStatsConsistent();

  // Add another reference to object 2
  manager_.AddReference(id2);
  ExpectStatsConsistent();

  // Remove references from object 2
  manager_.RemoveReference(id2);
  ExpectStatsConsistent();

  manager_.RemoveReference(id2);
  ExpectStatsConsistent();

  // Remove references from object 1
  manager_.RemoveReference(id1);
  ExpectStatsConsistent();

  manager_.RemoveReference(id1);
  ExpectStatsConsistent();

  // Delete both objects
  manager_.DeleteObject(id1);
  ExpectStatsConsistent();

  manager_.DeleteObject(id2);
  ExpectStatsConsistent();
}

TEST_F(RustStatsCollectorTest, SourceTracking) {
  // Create objects with different sources and verify counters
  auto id1 = CreateNewObjectId();
  manager_.CreateObject(id1, 100, 0, SOURCE_CREATED_BY_WORKER, false);
  num_bytes_created_total_ += 100;

  auto id2 = CreateNewObjectId();
  manager_.CreateObject(id2, 200, 0, SOURCE_RESTORED_FROM_STORAGE, false);
  num_bytes_created_total_ += 200;

  auto id3 = CreateNewObjectId();
  manager_.CreateObject(id3, 300, 0, SOURCE_RECEIVED_FROM_REMOTE, false);
  num_bytes_created_total_ += 300;

  auto id4 = CreateNewObjectId();
  manager_.CreateObject(id4, 400, 0, SOURCE_ERROR_STORED, false);
  num_bytes_created_total_ += 400;

  auto stats = manager_.GetStats();

  // Verify source-specific counters
  EXPECT_EQ(1, stats.NumObjectsCreatedByWorker());
  EXPECT_EQ(100, stats.NumBytesCreatedByWorker());

  EXPECT_EQ(1, stats.NumObjectsRestored());
  EXPECT_EQ(200, stats.NumBytesRestored());

  EXPECT_EQ(1, stats.NumObjectsReceived());
  EXPECT_EQ(300, stats.NumBytesReceived());

  EXPECT_EQ(1, stats.NumObjectsErrored());
  EXPECT_EQ(400, stats.NumBytesErrored());

  // All unsealed
  EXPECT_EQ(4, stats.NumObjectsUnsealed());
  EXPECT_EQ(1000, stats.NumBytesUnsealed());
}

}  // namespace
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
