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

// Rust FFI tests for ResourceSet types.
// Matches: src/ray/common/scheduling/tests/resource_set_test.cc

#include "gtest/gtest.h"
#include "ray/ffi/rust_scheduling.h"

namespace ray {

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
