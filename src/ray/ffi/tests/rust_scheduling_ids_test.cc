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

// Rust FFI tests for scheduling ID types (FixedPoint, ResourceId).
// Matches: src/ray/common/scheduling/tests/scheduling_ids_test.cc

#include <unordered_set>
#include <vector>

#include "gtest/gtest.h"
#include "ray/ffi/rust_scheduling.h"

namespace ray {

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

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
