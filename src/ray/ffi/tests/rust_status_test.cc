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

// Rust FFI tests for Status type.
// Matches: src/ray/common/tests/status_test.cc

#include "gtest/gtest.h"
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

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
