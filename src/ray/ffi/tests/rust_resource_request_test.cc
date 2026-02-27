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

// Rust FFI tests for ResourceRequest and TaskResourceInstances.
// Matches: src/ray/common/scheduling/tests/resource_request_test.cc

#include <string>
#include <unordered_set>
#include <vector>

#include "gtest/gtest.h"
#include "ray/ffi/rust_scheduling.h"

namespace ray {

class RustResourceRequestTest : public ::testing::Test {};

TEST_F(RustResourceRequestTest, TestBasic) {
  auto cpu_id = RustResourceId::CPU();
  auto gpu_id = RustResourceId::GPU();
  auto custom_id1 = RustResourceId("custom1");
  auto custom_id2 = RustResourceId("custom2");

  RustResourceRequest resource_request;
  resource_request.Set(cpu_id, 1);
  resource_request.Set(custom_id1, 2);

  // Test Has
  ASSERT_TRUE(resource_request.Has(cpu_id));
  ASSERT_TRUE(resource_request.Has(custom_id1));
  ASSERT_FALSE(resource_request.Has(gpu_id));
  ASSERT_FALSE(resource_request.Has(custom_id2));

  // Test Get
  ASSERT_EQ(resource_request.Get(cpu_id).Double(), 1);
  ASSERT_EQ(resource_request.Get(custom_id1).Double(), 2);
  ASSERT_EQ(resource_request.Get(gpu_id).Double(), 0);
  ASSERT_EQ(resource_request.Get(custom_id2).Double(), 0);

  // Test Size and IsEmpty
  ASSERT_EQ(resource_request.Size(), 2u);
  ASSERT_FALSE(resource_request.IsEmpty());

  // Test ResourceIds
  auto resource_ids = resource_request.ResourceIds();
  std::unordered_set<int64_t> id_set;
  for (const auto &id : resource_ids) {
    id_set.insert(id.ToInt());
  }
  ASSERT_EQ(id_set.size(), 2u);
  ASSERT_TRUE(id_set.count(cpu_id.ToInt()) > 0);
  ASSERT_TRUE(id_set.count(custom_id1.ToInt()) > 0);

  // Test Set
  resource_request.Set(gpu_id, 1);
  resource_request.Set(custom_id2, 2);
  ASSERT_TRUE(resource_request.Has(gpu_id));
  ASSERT_TRUE(resource_request.Has(custom_id2));
  ASSERT_EQ(resource_request.Get(gpu_id).Double(), 1);
  ASSERT_EQ(resource_request.Get(custom_id2).Double(), 2);

  // Set 0 will remove the resource
  resource_request.Set(cpu_id, 0);
  resource_request.Set(custom_id1, 0);
  ASSERT_FALSE(resource_request.Has(cpu_id));
  ASSERT_FALSE(resource_request.Has(custom_id1));

  // Test Clear
  resource_request.Clear();
  ASSERT_EQ(resource_request.Size(), 0u);
  ASSERT_TRUE(resource_request.IsEmpty());
}

TEST_F(RustResourceRequestTest, TestComparisonOperators) {
  auto cpu_id = RustResourceId::CPU();
  auto custom_id1 = RustResourceId("custom1");
  auto custom_id2 = RustResourceId("custom2");

  RustResourceRequest r1;
  r1.Set(cpu_id, 1);
  r1.Set(custom_id1, 2);

  RustResourceRequest r2 = r1;

  // r1 = {CPU: 1, custom1: 2}, r2 = {CPU: 1, custom1: 2}
  ASSERT_TRUE(r1 == r2);
  ASSERT_TRUE(r1 <= r2);
  ASSERT_TRUE(r2 <= r1);
  ASSERT_TRUE(r1 >= r2);
  ASSERT_TRUE(r2 >= r1);

  // r1 = {CPU: 1, custom1: 2}, r2 = {CPU: 2, custom1: 2}
  r2.Set(cpu_id, 2);
  ASSERT_TRUE(r1 != r2);
  ASSERT_TRUE(r1 <= r2);
  ASSERT_TRUE(r2 >= r1);

  // r1 = {CPU: 1, custom1: 2}, r2 = {CPU: 2, custom1: 2, custom2: 2}
  r2.Set(custom_id2, 2);
  ASSERT_TRUE(r1 != r2);
  ASSERT_TRUE(r1 <= r2);
  ASSERT_TRUE(r2 >= r1);

  // r1 = {CPU: 1, custom1: 2}, r2 = {CPU: 2, custom1: 1, custom2: 2}
  r2.Set(custom_id1, 1);
  ASSERT_TRUE(r1 != r2);
  ASSERT_FALSE(r1 <= r2);
  ASSERT_FALSE(r2 >= r1);
}

TEST_F(RustResourceRequestTest, TestNegativeValues) {
  auto custom_id1 = RustResourceId("custom1");

  // r1 = {custom1: -2}, r2 = {}
  RustResourceRequest r1;
  r1.Set(custom_id1, -2);
  RustResourceRequest r2;
  ASSERT_TRUE(r1 != r2);
  ASSERT_TRUE(r1 <= r2);
  ASSERT_TRUE(r2 >= r1);

  // r1 = {}, r2 = {custom1: -2}
  r1.Clear();
  r2.Set(custom_id1, -2);
  ASSERT_TRUE(r1 != r2);
  ASSERT_FALSE(r1 <= r2);
  ASSERT_FALSE(r2 >= r1);
}

TEST_F(RustResourceRequestTest, TestAlgebraOperators) {
  auto cpu_id = RustResourceId::CPU();
  auto custom_id1 = RustResourceId("custom1");
  auto custom_id2 = RustResourceId("custom2");

  // r1 = {CPU: 1, custom1: 2}, r2 = {CPU: -1, custom2: 2}
  RustResourceRequest r1;
  r1.Set(cpu_id, 1);
  r1.Set(custom_id1, 2);

  RustResourceRequest r2;
  r2.Set(cpu_id, -1);
  r2.Set(custom_id2, 2);

  // r3 = r1 + r2 = {custom1: 2, custom2: 2} (CPU becomes 0, removed)
  RustResourceRequest r3 = r1 + r2;
  ASSERT_FALSE(r3.Has(cpu_id));
  ASSERT_EQ(r3.Get(custom_id1).Double(), 2);
  ASSERT_EQ(r3.Get(custom_id2).Double(), 2);

  // Test += operator
  RustResourceRequest r3b = r1;
  r3b += r2;
  ASSERT_FALSE(r3b.Has(cpu_id));
  ASSERT_EQ(r3b.Get(custom_id1).Double(), 2);
  ASSERT_EQ(r3b.Get(custom_id2).Double(), 2);

  // r4 = r1 - r2 = {cpu: 2, custom1: 2, custom2: -2}
  RustResourceRequest r4 = r1 - r2;
  ASSERT_EQ(r4.Get(cpu_id).Double(), 2);
  ASSERT_EQ(r4.Get(custom_id1).Double(), 2);
  ASSERT_EQ(r4.Get(custom_id2).Double(), -2);

  // Test -= operator
  RustResourceRequest r4b = r1;
  r4b -= r2;
  ASSERT_EQ(r4b.Get(cpu_id).Double(), 2);
  ASSERT_EQ(r4b.Get(custom_id1).Double(), 2);
  ASSERT_EQ(r4b.Get(custom_id2).Double(), -2);
}

class RustTaskResourceInstancesTest : public ::testing::Test {};

TEST_F(RustTaskResourceInstancesTest, TestBasic) {
  auto cpu_id = RustResourceId::CPU();
  auto gpu_id = RustResourceId::GPU();
  auto custom_id1 = RustResourceId("custom1");

  RustResourceSet resource_set;
  resource_set.Set(cpu_id, 5);
  resource_set.Set(gpu_id, 5);

  RustTaskResourceInstances task_resource_instances(resource_set);

  // Test Has
  ASSERT_TRUE(task_resource_instances.Has(cpu_id));
  ASSERT_TRUE(task_resource_instances.Has(gpu_id));
  ASSERT_FALSE(task_resource_instances.Has(custom_id1));

  // Test Get
  // GPU is a unit resource, while CPU is not.
  auto cpu_instances = task_resource_instances.Get(cpu_id);
  auto gpu_instances = task_resource_instances.Get(gpu_id);
  EXPECT_EQ(cpu_instances, FixedPointVectorFromDouble({5}));
  EXPECT_EQ(gpu_instances, FixedPointVectorFromDouble({1, 1, 1, 1, 1}));

  // Test Set
  task_resource_instances.Set(custom_id1, FixedPointVectorFromDouble({1}));
  ASSERT_TRUE(task_resource_instances.Has(custom_id1));
  ASSERT_EQ(task_resource_instances.Get(custom_id1), FixedPointVectorFromDouble({1}));
  task_resource_instances.Set(custom_id1, FixedPointVectorFromDouble({2}));
  ASSERT_TRUE(task_resource_instances.Has(custom_id1));
  ASSERT_EQ(task_resource_instances.Get(custom_id1), FixedPointVectorFromDouble({2}));

  // Test Remove
  task_resource_instances.Remove(custom_id1);
  ASSERT_FALSE(task_resource_instances.Has(custom_id1));

  // Test ResourceIds
  auto resource_ids = task_resource_instances.ResourceIds();
  std::unordered_set<int64_t> id_set;
  for (const auto &id : resource_ids) {
    id_set.insert(id.ToInt());
  }
  ASSERT_EQ(id_set.size(), 2u);
  ASSERT_TRUE(id_set.count(cpu_id.ToInt()) > 0);
  ASSERT_TRUE(id_set.count(gpu_id.ToInt()) > 0);

  // Test Size and IsEmpty
  ASSERT_EQ(task_resource_instances.Size(), 2u);
  ASSERT_FALSE(task_resource_instances.IsEmpty());

  // Test Sum
  ASSERT_EQ(task_resource_instances.Sum(cpu_id).Double(), 5);
  ASSERT_EQ(task_resource_instances.Sum(gpu_id).Double(), 5);

  // Test ToResourceSet
  auto result_set = task_resource_instances.ToResourceSet();
  ASSERT_EQ(result_set.Get(cpu_id).Double(), 5);
  ASSERT_EQ(result_set.Get(gpu_id).Double(), 5);
}

TEST_F(RustTaskResourceInstancesTest, TestEmpty) {
  RustTaskResourceInstances instances;
  ASSERT_TRUE(instances.IsEmpty());
  ASSERT_EQ(instances.Size(), 0u);
}

TEST_F(RustTaskResourceInstancesTest, TestCopyConstructor) {
  auto cpu_id = RustResourceId::CPU();

  RustResourceSet resource_set;
  resource_set.Set(cpu_id, 4);

  RustTaskResourceInstances original(resource_set);
  RustTaskResourceInstances copy(original);

  ASSERT_EQ(copy.Sum(cpu_id).Double(), 4);
  ASSERT_TRUE(copy.Has(cpu_id));
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
