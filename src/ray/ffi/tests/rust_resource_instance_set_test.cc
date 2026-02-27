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

// Rust FFI tests for NodeResourceInstanceSet.
// Matches: src/ray/common/scheduling/tests/resource_instance_set_test.cc

#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "ray/ffi/rust_scheduling.h"

namespace ray {

class RustNodeResourceInstanceSetTest : public ::testing::Test {};

TEST_F(RustNodeResourceInstanceSetTest, TestConstructor) {
  RustNodeResourceSet node_set;
  node_set.Set(RustResourceId::CPU(), RustFixedPoint(2));
  node_set.Set(RustResourceId::GPU(), RustFixedPoint(2));

  RustNodeResourceInstanceSet r1(node_set);

  // CPU is non-unit, stored as single aggregate
  auto cpu_instances = r1.Get(RustResourceId::CPU());
  ASSERT_EQ(cpu_instances.size(), 1u);
  ASSERT_EQ(cpu_instances[0].Double(), 2);

  // GPU is unit resource, split into individual instances
  auto gpu_instances = r1.Get(RustResourceId::GPU());
  ASSERT_EQ(gpu_instances.size(), 2u);
  ASSERT_EQ(gpu_instances[0].Double(), 1);
  ASSERT_EQ(gpu_instances[1].Double(), 1);
}

TEST_F(RustNodeResourceInstanceSetTest, TestHas) {
  RustNodeResourceSet node_set;
  node_set.Set(RustResourceId::CPU(), RustFixedPoint(2));
  node_set.Set(RustResourceId::GPU(), RustFixedPoint(2));

  RustNodeResourceInstanceSet r1(node_set);

  ASSERT_TRUE(r1.Has(RustResourceId::CPU()));
  ASSERT_TRUE(r1.Has(RustResourceId::GPU()));
  ASSERT_FALSE(r1.Has(RustResourceId("non-exist")));
}

TEST_F(RustNodeResourceInstanceSetTest, TestRemove) {
  RustNodeResourceSet node_set;
  node_set.Set(RustResourceId::CPU(), RustFixedPoint(2));
  node_set.Set(RustResourceId::GPU(), RustFixedPoint(2));

  RustNodeResourceInstanceSet r1(node_set);

  ASSERT_TRUE(r1.Has(RustResourceId::GPU()));
  r1.Remove(RustResourceId::GPU());
  ASSERT_FALSE(r1.Has(RustResourceId::GPU()));

  // Removing non-existent resource should be safe
  r1.Remove(RustResourceId("non-exist"));
  ASSERT_FALSE(r1.Has(RustResourceId("non-exist")));
}

TEST_F(RustNodeResourceInstanceSetTest, TestGet) {
  RustNodeResourceSet node_set;
  node_set.Set(RustResourceId::CPU(), RustFixedPoint(2));

  RustNodeResourceInstanceSet r1(node_set);

  auto cpu_instances = r1.Get(RustResourceId::CPU());
  ASSERT_EQ(cpu_instances.size(), 1u);
  ASSERT_EQ(cpu_instances[0].Double(), 2);

  // Non-existent resource returns empty
  auto non_exist = r1.Get(RustResourceId("non-exist"));
  ASSERT_TRUE(non_exist.empty());
}

TEST_F(RustNodeResourceInstanceSetTest, TestSet) {
  RustNodeResourceInstanceSet r1;

  r1.Set(RustResourceId::CPU(), FixedPointVectorFromDouble({1}));
  ASSERT_TRUE(r1.Has(RustResourceId::CPU()));

  auto cpu_instances = r1.Get(RustResourceId::CPU());
  ASSERT_EQ(cpu_instances.size(), 1u);
  ASSERT_EQ(cpu_instances[0].Double(), 1);

  // Set different value
  r1.Set(RustResourceId::CPU(), FixedPointVectorFromDouble({3}));
  cpu_instances = r1.Get(RustResourceId::CPU());
  ASSERT_EQ(cpu_instances[0].Double(), 3);
}

TEST_F(RustNodeResourceInstanceSetTest, TestSum) {
  RustNodeResourceInstanceSet r1;

  r1.Set(RustResourceId::GPU(), FixedPointVectorFromDouble({1, 0.3, 0.5}));
  ASSERT_DOUBLE_EQ(r1.Sum(RustResourceId::GPU()), 1.8);

  // Non-existent resource returns 0
  ASSERT_DOUBLE_EQ(r1.Sum(RustResourceId("non-exist")), 0);
}

TEST_F(RustNodeResourceInstanceSetTest, TestOperator) {
  RustNodeResourceSet node_set1;
  node_set1.Set(RustResourceId::CPU(), RustFixedPoint(2));
  node_set1.Set(RustResourceId::GPU(), RustFixedPoint(2));

  RustNodeResourceSet node_set2;
  node_set2.Set(RustResourceId::CPU(), RustFixedPoint(2));
  node_set2.Set(RustResourceId::GPU(), RustFixedPoint(2));

  RustNodeResourceSet node_set3;
  node_set3.Set(RustResourceId::CPU(), RustFixedPoint(2));
  node_set3.Set(RustResourceId::GPU(), RustFixedPoint(1));

  RustNodeResourceInstanceSet r1(node_set1);
  RustNodeResourceInstanceSet r2(node_set2);
  RustNodeResourceInstanceSet r3(node_set3);

  ASSERT_TRUE(r1 == r2);
  ASSERT_FALSE(r1 == r3);
}

TEST_F(RustNodeResourceInstanceSetTest, TestTryAllocateNonUnitResource) {
  RustNodeResourceSet node_set;
  node_set.Set(RustResourceId::CPU(), RustFixedPoint(2));

  RustNodeResourceInstanceSet r1(node_set);

  // Allocation succeeds when demand is smaller than available
  RustResourceSet success_request;
  success_request.Set(RustResourceId::CPU(), RustFixedPoint(1));
  ASSERT_TRUE(r1.TryAllocate(success_request));

  // After allocation, remaining should be 1
  auto cpu_instances = r1.Get(RustResourceId::CPU());
  ASSERT_EQ(cpu_instances[0].Double(), 1);

  // Allocation fails when demand is larger than available
  RustResourceSet fail_request;
  fail_request.Set(RustResourceId::CPU(), RustFixedPoint(2));
  ASSERT_FALSE(r1.TryAllocate(fail_request));

  // Make sure nothing changed on failed allocation
  cpu_instances = r1.Get(RustResourceId::CPU());
  ASSERT_EQ(cpu_instances[0].Double(), 1);
}

TEST_F(RustNodeResourceInstanceSetTest, TestTryAllocateUnitResource) {
  RustNodeResourceSet node_set;
  node_set.Set(RustResourceId::GPU(), RustFixedPoint(4));

  RustNodeResourceInstanceSet r1(node_set);

  // Should have 4 GPU instances of 1 each
  auto gpu_instances = r1.Get(RustResourceId::GPU());
  ASSERT_EQ(gpu_instances.size(), 4u);

  // Allocate 2 GPUs
  RustResourceSet success_request;
  success_request.Set(RustResourceId::GPU(), RustFixedPoint(2));
  ASSERT_TRUE(r1.TryAllocate(success_request));

  // Should have 2 GPUs allocated (value 0) and 2 remaining (value 1)
  gpu_instances = r1.Get(RustResourceId::GPU());
  int remaining = 0;
  for (const auto &inst : gpu_instances) {
    if (inst.Double() > 0) remaining++;
  }
  ASSERT_EQ(remaining, 2);

  // Try to allocate 3 more - should fail
  RustResourceSet fail_request;
  fail_request.Set(RustResourceId::GPU(), RustFixedPoint(3));
  ASSERT_FALSE(r1.TryAllocate(fail_request));
}

TEST_F(RustNodeResourceInstanceSetTest, TestAdd) {
  RustNodeResourceInstanceSet r1;

  r1.Set(RustResourceId::GPU(), FixedPointVectorFromDouble({1, 0.3}));
  r1.Add(RustResourceId::GPU(), FixedPointVectorFromDouble({0, 0.3}));

  auto gpu_instances = r1.Get(RustResourceId::GPU());
  ASSERT_EQ(gpu_instances.size(), 2u);
  ASSERT_DOUBLE_EQ(gpu_instances[0].Double(), 1);
  ASSERT_DOUBLE_EQ(gpu_instances[1].Double(), 0.6);

  // Add new resource
  r1.Add(RustResourceId("new"), FixedPointVectorFromDouble({2}));
  auto new_instances = r1.Get(RustResourceId("new"));
  ASSERT_EQ(new_instances.size(), 1u);
  ASSERT_DOUBLE_EQ(new_instances[0].Double(), 2);
}

TEST_F(RustNodeResourceInstanceSetTest, TestSubtract) {
  RustNodeResourceInstanceSet r1;

  r1.Set(RustResourceId::GPU(), FixedPointVectorFromDouble({1, 1}));
  r1.Subtract(RustResourceId::GPU(), FixedPointVectorFromDouble({0.5, 0}), true);

  auto gpu_instances = r1.Get(RustResourceId::GPU());
  ASSERT_EQ(gpu_instances.size(), 2u);
  ASSERT_DOUBLE_EQ(gpu_instances[0].Double(), 0.5);
  ASSERT_DOUBLE_EQ(gpu_instances[1].Double(), 1);

  // Subtract more with allow_negative=true
  r1.Subtract(RustResourceId::GPU(), FixedPointVectorFromDouble({1, 0}), true);
  gpu_instances = r1.Get(RustResourceId::GPU());
  ASSERT_DOUBLE_EQ(gpu_instances[0].Double(), -0.5);
  ASSERT_DOUBLE_EQ(gpu_instances[1].Double(), 1);
}

TEST_F(RustNodeResourceInstanceSetTest, TestFree) {
  RustNodeResourceInstanceSet r1;

  r1.Set(RustResourceId::GPU(), FixedPointVectorFromDouble({1, 0.3}));
  r1.Free(RustResourceId::GPU(), FixedPointVectorFromDouble({0, 0.7}));

  auto gpu_instances = r1.Get(RustResourceId::GPU());
  ASSERT_EQ(gpu_instances.size(), 2u);
  ASSERT_DOUBLE_EQ(gpu_instances[0].Double(), 1);
  ASSERT_DOUBLE_EQ(gpu_instances[1].Double(), 1);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
