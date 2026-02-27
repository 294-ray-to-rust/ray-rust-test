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

// Simple test to verify the Rust FFI infrastructure compiles and works.
// This test uses the Rust implementations of Status and ID types.

#include <unordered_set>
#include <vector>

#include "gtest/gtest.h"
#include "ray/ffi/rust_id.h"
#include "ray/ffi/rust_scheduling.h"
#include "ray/ffi/rust_status.h"

namespace ray {

class RustStatusTest : public ::testing::Test {};

TEST_F(RustStatusTest, OkStatus) {
  RustStatus status = RustStatus::OK();
  EXPECT_TRUE(status.ok());
  EXPECT_EQ(status.ToString(), "OK");
}

TEST_F(RustStatusTest, ErrorStatus) {
  RustStatus status = RustStatus::KeyError("key not found");
  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(status.IsKeyError());
  EXPECT_EQ(status.message(), "key not found");
}

TEST_F(RustStatusTest, AllErrorTypes) {
  EXPECT_TRUE(RustStatus::OutOfMemory("oom").IsOutOfMemory());
  EXPECT_TRUE(RustStatus::KeyError("key").IsKeyError());
  EXPECT_TRUE(RustStatus::TypeError("type").IsTypeError());
  EXPECT_TRUE(RustStatus::Invalid("invalid").IsInvalid());
  EXPECT_TRUE(RustStatus::IOError("io").IsIOError());
  EXPECT_TRUE(RustStatus::NotFound("not found").IsNotFound());
  EXPECT_TRUE(RustStatus::AlreadyExists("exists").IsAlreadyExists());
  EXPECT_TRUE(RustStatus::TimedOut("timeout").IsTimedOut());
  EXPECT_TRUE(RustStatus::InvalidArgument("arg").IsInvalidArgument());
  EXPECT_TRUE(RustStatus::PermissionDenied("denied").IsPermissionDenied());
}

TEST_F(RustStatusTest, RpcError) {
  RustStatus status = RustStatus::RpcError("connection failed", 14);
  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(status.IsRpcError());
  EXPECT_EQ(status.rpc_code(), 14);
}

TEST_F(RustStatusTest, CopyStatus) {
  RustStatus original = RustStatus::KeyError("test error");
  RustStatus copy = original;

  EXPECT_FALSE(copy.ok());
  EXPECT_TRUE(copy.IsKeyError());
  EXPECT_EQ(copy.message(), "test error");
}

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

// ============================================================================
// RustFixedPoint Tests
// ============================================================================

class RustFixedPointTest : public ::testing::Test {};

TEST_F(RustFixedPointTest, FromDouble) {
  RustFixedPoint fp(1.5);
  EXPECT_DOUBLE_EQ(fp.Double(), 1.5);
}

TEST_F(RustFixedPointTest, FromInt) {
  RustFixedPoint fp(3);
  EXPECT_DOUBLE_EQ(fp.Double(), 3.0);
}

TEST_F(RustFixedPointTest, Addition) {
  RustFixedPoint a(1.5);
  RustFixedPoint b(2.5);
  RustFixedPoint result = a + b;
  EXPECT_DOUBLE_EQ(result.Double(), 4.0);
}

TEST_F(RustFixedPointTest, AdditionAssign) {
  RustFixedPoint a(1.5);
  RustFixedPoint b(2.5);
  a += b;
  EXPECT_DOUBLE_EQ(a.Double(), 4.0);
}

TEST_F(RustFixedPointTest, Subtraction) {
  RustFixedPoint a(5.0);
  RustFixedPoint b(2.5);
  RustFixedPoint result = a - b;
  EXPECT_DOUBLE_EQ(result.Double(), 2.5);
}

TEST_F(RustFixedPointTest, Negation) {
  RustFixedPoint fp(3.0);
  RustFixedPoint neg = -fp;
  EXPECT_DOUBLE_EQ(neg.Double(), -3.0);
}

TEST_F(RustFixedPointTest, Comparison) {
  RustFixedPoint a(1.5);
  RustFixedPoint b(2.5);
  RustFixedPoint c(1.5);

  EXPECT_TRUE(a < b);
  EXPECT_TRUE(b > a);
  EXPECT_TRUE(a <= b);
  EXPECT_TRUE(b >= a);
  EXPECT_TRUE(a == c);
  EXPECT_TRUE(a != b);
}

TEST_F(RustFixedPointTest, Sum) {
  std::vector<RustFixedPoint> values = {
      RustFixedPoint(1.0), RustFixedPoint(2.0), RustFixedPoint(3.0)};
  RustFixedPoint sum = RustFixedPoint::Sum(values);
  EXPECT_DOUBLE_EQ(sum.Double(), 6.0);
}

TEST_F(RustFixedPointTest, AddDouble) {
  RustFixedPoint fp(1.0);
  RustFixedPoint result = fp + 2.5;
  EXPECT_DOUBLE_EQ(result.Double(), 3.5);
}

TEST_F(RustFixedPointTest, SubDouble) {
  RustFixedPoint fp(5.0);
  RustFixedPoint result = fp - 2.5;
  EXPECT_DOUBLE_EQ(result.Double(), 2.5);
}

// ============================================================================
// RustResourceId Tests
// ============================================================================

class RustResourceIdTest : public ::testing::Test {};

TEST_F(RustResourceIdTest, PredefinedCpu) {
  RustResourceId cpu = RustResourceId::CPU();
  EXPECT_TRUE(cpu.IsPredefinedResource());
  EXPECT_FALSE(cpu.IsImplicitResource());
  EXPECT_EQ(cpu.Binary(), "CPU");
}

TEST_F(RustResourceIdTest, PredefinedGpu) {
  RustResourceId gpu = RustResourceId::GPU();
  EXPECT_TRUE(gpu.IsPredefinedResource());
  EXPECT_TRUE(gpu.IsUnitInstanceResource());
}

TEST_F(RustResourceIdTest, CustomResource) {
  RustResourceId custom("custom_resource");
  EXPECT_FALSE(custom.IsPredefinedResource());
  EXPECT_FALSE(custom.IsNil());
  EXPECT_EQ(custom.Binary(), "custom_resource");
}

TEST_F(RustResourceIdTest, NilResource) {
  RustResourceId nil = RustResourceId::Nil();
  EXPECT_TRUE(nil.IsNil());
}

TEST_F(RustResourceIdTest, Equality) {
  RustResourceId cpu1 = RustResourceId::CPU();
  RustResourceId cpu2 = RustResourceId::CPU();
  RustResourceId gpu = RustResourceId::GPU();

  EXPECT_EQ(cpu1, cpu2);
  EXPECT_NE(cpu1, gpu);
}

TEST_F(RustResourceIdTest, Hashing) {
  RustResourceId cpu1 = RustResourceId::CPU();
  RustResourceId cpu2 = RustResourceId::CPU();
  RustResourceId gpu = RustResourceId::GPU();

  std::unordered_set<RustResourceId> set;
  set.insert(cpu1);
  EXPECT_TRUE(set.count(cpu2) > 0);
  EXPECT_TRUE(set.count(gpu) == 0);
}

// ============================================================================
// RustResourceSet Tests
// ============================================================================

class RustResourceSetTest : public ::testing::Test {};

TEST_F(RustResourceSetTest, EmptySet) {
  RustResourceSet set;
  EXPECT_TRUE(set.IsEmpty());
  EXPECT_EQ(set.Size(), 0u);
}

TEST_F(RustResourceSetTest, SetAndGet) {
  RustResourceSet set;
  RustResourceId cpu = RustResourceId::CPU();
  set.Set(cpu, RustFixedPoint(4.0));

  EXPECT_FALSE(set.IsEmpty());
  EXPECT_EQ(set.Size(), 1u);
  EXPECT_TRUE(set.Has(cpu));
  EXPECT_DOUBLE_EQ(set.Get(cpu).Double(), 4.0);
}

TEST_F(RustResourceSetTest, SetZeroRemoves) {
  RustResourceSet set;
  RustResourceId cpu = RustResourceId::CPU();
  set.Set(cpu, RustFixedPoint(4.0));
  EXPECT_EQ(set.Size(), 1u);

  set.Set(cpu, RustFixedPoint(0.0));
  EXPECT_TRUE(set.IsEmpty());
}

TEST_F(RustResourceSetTest, Clear) {
  RustResourceSet set;
  set.Set(RustResourceId::CPU(), RustFixedPoint(4.0));
  set.Set(RustResourceId::GPU(), RustFixedPoint(2.0));
  EXPECT_EQ(set.Size(), 2u);

  set.Clear();
  EXPECT_TRUE(set.IsEmpty());
}

TEST_F(RustResourceSetTest, Addition) {
  RustResourceSet set1;
  set1.Set(RustResourceId::CPU(), RustFixedPoint(2.0));

  RustResourceSet set2;
  set2.Set(RustResourceId::CPU(), RustFixedPoint(3.0));
  set2.Set(RustResourceId::GPU(), RustFixedPoint(1.0));

  RustResourceSet result = set1 + set2;
  EXPECT_DOUBLE_EQ(result.Get(RustResourceId::CPU()).Double(), 5.0);
  EXPECT_DOUBLE_EQ(result.Get(RustResourceId::GPU()).Double(), 1.0);
}

TEST_F(RustResourceSetTest, Subtraction) {
  RustResourceSet set1;
  set1.Set(RustResourceId::CPU(), RustFixedPoint(5.0));

  RustResourceSet set2;
  set2.Set(RustResourceId::CPU(), RustFixedPoint(3.0));

  RustResourceSet result = set1 - set2;
  EXPECT_DOUBLE_EQ(result.Get(RustResourceId::CPU()).Double(), 2.0);
}

TEST_F(RustResourceSetTest, Subset) {
  RustResourceSet set1;
  set1.Set(RustResourceId::CPU(), RustFixedPoint(2.0));

  RustResourceSet set2;
  set2.Set(RustResourceId::CPU(), RustFixedPoint(4.0));

  EXPECT_TRUE(set1 <= set2);
  EXPECT_FALSE(set2 <= set1);
  EXPECT_TRUE(set2 >= set1);
}

// ============================================================================
// RustNodeResourceSet Tests
// ============================================================================

class RustNodeResourceSetTest : public ::testing::Test {};

TEST_F(RustNodeResourceSetTest, SetAndGet) {
  RustNodeResourceSet set;
  RustResourceId cpu = RustResourceId::CPU();
  set.Set(cpu, RustFixedPoint(8.0));

  EXPECT_TRUE(set.Has(cpu));
  EXPECT_DOUBLE_EQ(set.Get(cpu).Double(), 8.0);
}

TEST_F(RustNodeResourceSetTest, SupersetCheck) {
  RustNodeResourceSet node_set;
  node_set.Set(RustResourceId::CPU(), RustFixedPoint(8.0));

  RustResourceSet request;
  request.Set(RustResourceId::CPU(), RustFixedPoint(4.0));

  EXPECT_TRUE(node_set >= request);

  request.Set(RustResourceId::CPU(), RustFixedPoint(12.0));
  EXPECT_FALSE(node_set >= request);
}

TEST_F(RustNodeResourceSetTest, Equality) {
  RustNodeResourceSet set1;
  set1.Set(RustResourceId::CPU(), RustFixedPoint(8.0));

  RustNodeResourceSet set2;
  set2.Set(RustResourceId::CPU(), RustFixedPoint(8.0));

  EXPECT_EQ(set1, set2);

  set2.Set(RustResourceId::GPU(), RustFixedPoint(1.0));
  EXPECT_NE(set1, set2);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
