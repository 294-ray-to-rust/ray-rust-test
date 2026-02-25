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

#include "gtest/gtest.h"
#include "ray/ffi/rust_id.h"
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

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
