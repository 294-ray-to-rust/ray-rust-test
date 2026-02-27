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

// Rust FFI tests for ID types.
// Matches: src/ray/common/tests/id_test.cc

#include <unordered_set>

#include "gtest/gtest.h"
#include "ray/ffi/rust_id.h"

namespace ray {

class RustJobIdTest : public ::testing::Test {};

TEST_F(RustJobIdTest, FromInt) {
  RustJobId id = RustJobId::FromInt(12345);
  EXPECT_EQ(id.ToInt(), 12345u);
  EXPECT_FALSE(id.IsNil());
}

TEST_F(RustJobIdTest, NilId) {
  RustJobId nil = RustJobId::Nil();
  EXPECT_TRUE(nil.IsNil());
}

TEST_F(RustJobIdTest, HexConversion) {
  RustJobId id = RustJobId::FromInt(0x12345678);
  std::string hex = id.Hex();
  EXPECT_EQ(hex, "12345678");

  RustJobId restored = RustJobId::FromHex(hex);
  EXPECT_EQ(id, restored);
}

TEST_F(RustJobIdTest, BinaryConversion) {
  RustJobId id = RustJobId::FromInt(42);
  std::string binary = id.Binary();
  EXPECT_EQ(binary.size(), 4u);

  RustJobId restored = RustJobId::FromBinary(binary);
  EXPECT_EQ(id, restored);
}

TEST_F(RustJobIdTest, Hashing) {
  RustJobId id1 = RustJobId::FromInt(100);
  RustJobId id2 = RustJobId::FromInt(100);
  RustJobId id3 = RustJobId::FromInt(200);

  std::unordered_set<RustJobId> set;
  set.insert(id1);
  EXPECT_TRUE(set.count(id2) > 0);
  EXPECT_TRUE(set.count(id3) == 0);
}

class RustActorIdTest : public ::testing::Test {};

TEST_F(RustActorIdTest, NilFromJob) {
  RustJobId job_id = RustJobId::FromInt(42);
  RustActorId actor_id = RustActorId::NilFromJob(job_id);

  // The actor ID should contain the job ID
  RustJobId extracted = actor_id.JobId();
  EXPECT_EQ(job_id, extracted);
}

TEST_F(RustActorIdTest, Size) {
  EXPECT_EQ(RustActorId::Size(), 16u);
}

class RustTaskIdTest : public ::testing::Test {};

TEST_F(RustTaskIdTest, ForActorCreationTask) {
  RustJobId job_id = RustJobId::FromInt(1);
  RustActorId actor_id = RustActorId::NilFromJob(job_id);
  RustTaskId task_id = RustTaskId::ForActorCreationTask(actor_id);

  EXPECT_TRUE(task_id.IsForActorCreationTask());
  EXPECT_EQ(task_id.ActorId(), actor_id);
}

TEST_F(RustTaskIdTest, Random) {
  RustJobId job_id = RustJobId::FromInt(1);
  RustTaskId task1 = RustTaskId::FromRandom(job_id);
  RustTaskId task2 = RustTaskId::FromRandom(job_id);

  EXPECT_NE(task1, task2);
}

class RustObjectIdTest : public ::testing::Test {};

TEST_F(RustObjectIdTest, FromIndex) {
  RustJobId job_id = RustJobId::FromInt(1);
  RustTaskId task_id = RustTaskId::FromRandom(job_id);
  RustObjectId object_id = RustObjectId::FromIndex(task_id, 5);

  EXPECT_EQ(object_id.ObjectIndex(), 5u);
  EXPECT_EQ(object_id.TaskId(), task_id);
}

TEST_F(RustObjectIdTest, Random) {
  RustObjectId id1 = RustObjectId::FromRandom();
  RustObjectId id2 = RustObjectId::FromRandom();

  EXPECT_NE(id1, id2);
}

// Tests for ActorId::Of
class RustActorIdOfTest : public ::testing::Test {};

TEST_F(RustActorIdOfTest, CreatesActorIdFromJobAndTask) {
  RustJobId job_id = RustJobId::FromInt(199);
  RustTaskId driver_task_id = RustTaskId::ForDriverTask(job_id);
  RustActorId actor_id = RustActorId::Of(job_id, driver_task_id, 1);

  EXPECT_FALSE(actor_id.IsNil());
  EXPECT_EQ(actor_id.JobId(), job_id);
}

// Tests for TaskId factory methods
class RustTaskIdFactoryTest : public ::testing::Test {};

TEST_F(RustTaskIdFactoryTest, ForDriverTask) {
  RustJobId job_id = RustJobId::FromInt(199);
  RustTaskId driver_task_id = RustTaskId::ForDriverTask(job_id);

  EXPECT_FALSE(driver_task_id.IsNil());
  EXPECT_FALSE(driver_task_id.IsForActorCreationTask());
}

TEST_F(RustTaskIdFactoryTest, ForActorTask) {
  RustJobId job_id = RustJobId::FromInt(199);
  RustTaskId driver_task_id = RustTaskId::ForDriverTask(job_id);
  RustActorId actor_id = RustActorId::Of(job_id, driver_task_id, 1);
  RustTaskId task_id = RustTaskId::ForActorTask(job_id, driver_task_id, 1, actor_id);

  EXPECT_FALSE(task_id.IsNil());
  EXPECT_FALSE(task_id.IsForActorCreationTask());
  EXPECT_EQ(task_id.ActorId(), actor_id);
}

TEST_F(RustTaskIdFactoryTest, ForNormalTask) {
  RustJobId job_id = RustJobId::FromInt(199);
  RustTaskId driver_task_id = RustTaskId::ForDriverTask(job_id);
  RustTaskId task_id = RustTaskId::ForNormalTask(job_id, driver_task_id, 0);

  EXPECT_FALSE(task_id.IsNil());
  EXPECT_FALSE(task_id.IsForActorCreationTask());
}

TEST_F(RustTaskIdFactoryTest, ForExecutionAttempt) {
  RustJobId job_id = RustJobId::FromInt(199);
  RustTaskId task_id = RustTaskId::FromRandom(job_id);

  RustTaskId attempt0 = RustTaskId::ForExecutionAttempt(task_id, 0);
  RustTaskId attempt1 = RustTaskId::ForExecutionAttempt(task_id, 1);

  // Different attempts should produce different task IDs
  EXPECT_NE(task_id, attempt0);
  EXPECT_NE(task_id, attempt1);
  EXPECT_NE(attempt0, attempt1);

  // Same attempt should be equal
  RustTaskId attempt1_again = RustTaskId::ForExecutionAttempt(task_id, 1);
  EXPECT_EQ(attempt1, attempt1_again);

  // Check overflow handling
  EXPECT_NE(RustTaskId::ForExecutionAttempt(task_id, 0),
            RustTaskId::ForExecutionAttempt(task_id, 256));
}

// Tests for PlacementGroupId
class RustPlacementGroupIdTest : public ::testing::Test {};

TEST_F(RustPlacementGroupIdTest, Of) {
  RustJobId job_id = RustJobId::FromInt(1);
  RustPlacementGroupId pg_id = RustPlacementGroupId::Of(job_id);

  EXPECT_FALSE(pg_id.IsNil());
  EXPECT_EQ(pg_id.JobId(), job_id);
}

TEST_F(RustPlacementGroupIdTest, Size) {
  EXPECT_EQ(RustPlacementGroupId::Size(), 18u);
}

TEST_F(RustPlacementGroupIdTest, BinaryRoundtrip) {
  RustJobId job_id = RustJobId::FromInt(1);
  RustPlacementGroupId pg_id1 = RustPlacementGroupId::Of(job_id);
  std::string binary = pg_id1.Binary();

  EXPECT_EQ(binary.size(), 18u);

  RustPlacementGroupId pg_id2 = RustPlacementGroupId::FromBinary(binary);
  EXPECT_EQ(pg_id1, pg_id2);
}

TEST_F(RustPlacementGroupIdTest, HexRoundtrip) {
  RustJobId job_id = RustJobId::FromInt(1);
  RustPlacementGroupId pg_id1 = RustPlacementGroupId::Of(job_id);
  std::string hex = pg_id1.Hex();

  RustPlacementGroupId pg_id2 = RustPlacementGroupId::FromHex(hex);
  EXPECT_EQ(pg_id1, pg_id2);
}

// Tests for LeaseId
class RustLeaseIdTest : public ::testing::Test {};

TEST_F(RustLeaseIdTest, FromWorker) {
  RustUniqueId worker_id = RustUniqueId::FromRandom();
  RustLeaseId lease_id = RustLeaseId::FromWorker(worker_id, 2);

  EXPECT_FALSE(lease_id.IsNil());
  EXPECT_EQ(lease_id.WorkerId(), worker_id);
}

TEST_F(RustLeaseIdTest, Size) {
  EXPECT_EQ(RustLeaseId::Size(), 32u);
}

TEST_F(RustLeaseIdTest, DifferentCounters) {
  RustUniqueId worker_id = RustUniqueId::FromRandom();
  RustLeaseId lease1 = RustLeaseId::FromWorker(worker_id, 1);
  RustLeaseId lease2 = RustLeaseId::FromWorker(worker_id, 2);

  EXPECT_NE(lease1, lease2);
  EXPECT_EQ(lease1.WorkerId(), lease2.WorkerId());
}

TEST_F(RustLeaseIdTest, BinaryRoundtrip) {
  RustUniqueId worker_id = RustUniqueId::FromRandom();
  RustLeaseId lease_id = RustLeaseId::FromWorker(worker_id, 2);
  std::string binary = lease_id.Binary();

  EXPECT_EQ(binary.size(), 32u);

  RustLeaseId from_binary = RustLeaseId::FromBinary(binary);
  EXPECT_EQ(lease_id, from_binary);
  EXPECT_EQ(lease_id.WorkerId(), from_binary.WorkerId());
}

TEST_F(RustLeaseIdTest, HexRoundtrip) {
  RustUniqueId worker_id = RustUniqueId::FromRandom();
  RustLeaseId lease_id = RustLeaseId::FromWorker(worker_id, 2);

  RustLeaseId from_hex = RustLeaseId::FromHex(lease_id.Hex());
  EXPECT_EQ(lease_id, from_hex);
}

TEST_F(RustLeaseIdTest, Random) {
  RustLeaseId random_lease = RustLeaseId::FromRandom();
  EXPECT_FALSE(random_lease.IsNil());
}

// Tests for UniqueId
class RustUniqueIdTest : public ::testing::Test {};

TEST_F(RustUniqueIdTest, FromRandom) {
  RustUniqueId id1 = RustUniqueId::FromRandom();
  RustUniqueId id2 = RustUniqueId::FromRandom();

  EXPECT_FALSE(id1.IsNil());
  EXPECT_FALSE(id2.IsNil());
  EXPECT_NE(id1, id2);
}

TEST_F(RustUniqueIdTest, Size) {
  EXPECT_EQ(RustUniqueId::Size(), 28u);
}

TEST_F(RustUniqueIdTest, HexRoundtrip) {
  RustUniqueId id = RustUniqueId::FromRandom();
  std::string hex = id.Hex();

  RustUniqueId restored = RustUniqueId::FromHex(hex);
  EXPECT_EQ(id, restored);
}

TEST_F(RustUniqueIdTest, Hashing) {
  RustUniqueId id1 = RustUniqueId::FromRandom();
  RustUniqueId id2 = RustUniqueId::FromRandom();

  std::unordered_set<RustUniqueId> set;
  set.insert(id1);
  EXPECT_TRUE(set.count(id1) > 0);
  EXPECT_TRUE(set.count(id2) == 0);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
