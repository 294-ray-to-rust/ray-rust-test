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

// Rust FFI tests for cgroup manager.
// Matches: src/ray/common/cgroup2/tests/cgroup_manager_test.cc

#include "gtest/gtest.h"
#include "src/ray/ffi/rust_cgroup_manager.h"

#include <memory>
#include <string>

namespace ray {

TEST(RustCgroupManagerTest, CreateReturnsInvalidIfCgroupv2NotAvailable) {
  RustFakeCgroupDriverState driver_state;
  driver_state.AddCgroup("/sys/fs/cgroup");
  driver_state.SetCheckEnabledStatus(RustCgroupManagerStatus::Invalid);

  auto [result, manager] =
      RustCgroupManager::Create("/sys/fs/cgroup/ray",
                                "node_id_123",
                                100,
                                1000000,
                                0,
                                0,
                                0,
                                driver_state);

  EXPECT_TRUE(result.IsInvalid()) << result.ToString();
  EXPECT_EQ(manager, nullptr);
  // No visible side-effects
  EXPECT_EQ(driver_state.GetCgroupCount(), 1);
}

TEST(RustCgroupManagerTest, CreateReturnsNotFoundIfBaseCgroupDoesNotExist) {
  RustFakeCgroupDriverState driver_state;
  driver_state.SetCheckCgroupStatus(RustCgroupManagerStatus::NotFound);

  auto [result, manager] =
      RustCgroupManager::Create("/sys/fs/cgroup/ray",
                                "node_id_123",
                                100,
                                1000000,
                                1000000,
                                10000000,
                                10000000,
                                driver_state);

  EXPECT_TRUE(result.IsNotFound()) << result.ToString();
  EXPECT_EQ(manager, nullptr);
  // No visible side-effects
  EXPECT_EQ(driver_state.GetCgroupCount(), 0);
}

TEST(RustCgroupManagerTest,
     CreateReturnsPermissionDeniedIfProcessDoesNotHavePermissionsForBaseCgroup) {
  RustFakeCgroupDriverState driver_state;
  driver_state.AddCgroup("/sys/fs/cgroup");
  driver_state.SetCheckCgroupStatus(RustCgroupManagerStatus::PermissionDenied);

  auto [result, manager] =
      RustCgroupManager::Create("/sys/fs/cgroup/ray",
                                "node_id_123",
                                100,
                                1000000,
                                1000000,
                                10000000,
                                100000000,
                                driver_state);

  EXPECT_TRUE(result.IsPermissionDenied()) << result.ToString();
  EXPECT_EQ(manager, nullptr);
  // No visible side-effects
  EXPECT_EQ(driver_state.GetCgroupCount(), 1);
}

TEST(RustCgroupManagerTest, CreateReturnsInvalidIfSupportedControllersAreNotAvailable) {
  RustFakeCgroupDriverState driver_state;
  // Add cgroup without cpu and memory controllers
  driver_state.AddCgroup("/sys/fs/cgroup");

  auto [result, manager] =
      RustCgroupManager::Create("/sys/fs/cgroup",
                                "node_id_123",
                                100,
                                1000000,
                                1000000,
                                10000000,
                                100000000,
                                driver_state);

  EXPECT_TRUE(result.IsInvalid()) << result.ToString();
  EXPECT_EQ(manager, nullptr);
  // No visible side-effects
  EXPECT_EQ(driver_state.GetCgroupCount(), 1);
}

TEST(RustCgroupManagerTest, CreateReturnsInvalidArgumentIfConstraintValuesOutOfBounds) {
  RustFakeCgroupDriverState driver_state;
  driver_state.AddCgroup("/sys/fs/cgroup", {}, {"cpu", "memory"});

  // cpu_weight -1 is out of bounds [1, 10000]
  auto [result, manager] =
      RustCgroupManager::Create("/sys/fs/cgroup",
                                "node_id_123",
                                -1,  // Invalid cpu_weight
                                1000000,
                                1000000,
                                10000000,
                                100000000,
                                driver_state);

  EXPECT_TRUE(result.IsInvalidArgument()) << result.ToString();
  EXPECT_EQ(manager, nullptr);
  // No visible side-effects
  EXPECT_EQ(driver_state.GetCgroupCount(), 1);
}

TEST(RustCgroupManagerTest, CreateSucceedsWithCleanupInOrder) {
  RustFakeCgroupDriverState driver_state;
  driver_state.AddCgroup("/sys/fs/cgroup", {5}, {"cpu", "memory"});

  std::string node_id = "id_123";
  std::string base_cgroup_path = "/sys/fs/cgroup";
  std::string node_cgroup_path = "/sys/fs/cgroup/ray-node_id_123";
  std::string system_cgroup_path = "/sys/fs/cgroup/ray-node_id_123/system";
  std::string system_leaf_cgroup_path = "/sys/fs/cgroup/ray-node_id_123/system/leaf";
  std::string user_cgroup_path = "/sys/fs/cgroup/ray-node_id_123/user";
  std::string workers_cgroup_path = "/sys/fs/cgroup/ray-node_id_123/user/workers";
  std::string non_ray_cgroup_path = "/sys/fs/cgroup/ray-node_id_123/user/non-ray";

  int64_t system_reserved_cpu_weight = 1000;
  int64_t system_memory_bytes_min = 1024 * 1024 * 1024;
  int64_t system_memory_bytes_low = 1024 * 1024 * 1024;
  int64_t user_memory_high_bytes = 10 * 1024 * static_cast<int64_t>(1024 * 1024);
  int64_t user_memory_max_bytes = 10 * 1024 * static_cast<int64_t>(1024 * 1024);

  auto [result, manager] =
      RustCgroupManager::Create(base_cgroup_path,
                                node_id,
                                system_reserved_cpu_weight,
                                system_memory_bytes_min,
                                system_memory_bytes_low,
                                user_memory_high_bytes,
                                user_memory_max_bytes,
                                driver_state);

  ASSERT_TRUE(result.ok()) << result.ToString();
  ASSERT_NE(manager, nullptr);

  // The cgroup hierarchy was created correctly.
  EXPECT_EQ(driver_state.GetCgroupCount(), 7);
  EXPECT_TRUE(driver_state.HasCgroup(base_cgroup_path));
  EXPECT_TRUE(driver_state.HasCgroup(node_cgroup_path));
  EXPECT_TRUE(driver_state.HasCgroup(system_cgroup_path));
  EXPECT_TRUE(driver_state.HasCgroup(system_leaf_cgroup_path));
  EXPECT_TRUE(driver_state.HasCgroup(user_cgroup_path));
  EXPECT_TRUE(driver_state.HasCgroup(workers_cgroup_path));
  EXPECT_TRUE(driver_state.HasCgroup(non_ray_cgroup_path));

  // Controller counts
  EXPECT_EQ(driver_state.GetEnabledControllersCount(base_cgroup_path), 2);
  EXPECT_EQ(driver_state.GetEnabledControllersCount(node_cgroup_path), 2);
  EXPECT_EQ(driver_state.GetEnabledControllersCount(system_cgroup_path), 1);
  EXPECT_EQ(driver_state.GetEnabledControllersCount(user_cgroup_path), 1);

  // CPU controllers on base and node
  EXPECT_TRUE(driver_state.HasEnabledController(base_cgroup_path, "cpu"));
  EXPECT_TRUE(driver_state.HasEnabledController(node_cgroup_path, "cpu"));

  // Memory controllers on base, node, system, and user
  EXPECT_TRUE(driver_state.HasEnabledController(base_cgroup_path, "memory"));
  EXPECT_TRUE(driver_state.HasEnabledController(node_cgroup_path, "memory"));
  EXPECT_TRUE(driver_state.HasEnabledController(system_cgroup_path, "memory"));
  EXPECT_TRUE(driver_state.HasEnabledController(user_cgroup_path, "memory"));

  // Processes were moved from base to non-ray
  EXPECT_EQ(driver_state.GetProcessesCount(base_cgroup_path), 0);
  EXPECT_EQ(driver_state.GetProcessesCount(non_ray_cgroup_path), 1);

  // System cgroup constraints
  EXPECT_EQ(driver_state.GetConstraint(system_cgroup_path, "cpu.weight"),
            std::to_string(system_reserved_cpu_weight));
  EXPECT_EQ(driver_state.GetConstraint(system_cgroup_path, "memory.min"),
            std::to_string(system_memory_bytes_min));
  EXPECT_EQ(driver_state.GetConstraint(system_cgroup_path, "memory.low"),
            std::to_string(system_memory_bytes_low));

  // User cgroup constraints
  EXPECT_EQ(driver_state.GetConstraint(user_cgroup_path, "cpu.weight"), "9000");
  EXPECT_EQ(driver_state.GetConstraint(user_cgroup_path, "memory.high"),
            std::to_string(user_memory_high_bytes));
  EXPECT_EQ(driver_state.GetConstraint(user_cgroup_path, "memory.max"),
            std::to_string(user_memory_max_bytes));

  // Enable cleanup mode to record operations
  driver_state.SetCleanupMode(true);

  // Destroy the manager to trigger cleanup
  manager.reset();

  // Only the base cgroup is left
  EXPECT_EQ(driver_state.GetCgroupCount(), 1);
  EXPECT_TRUE(driver_state.HasCgroup(base_cgroup_path));

  // Constraints were disabled first (6 total)
  EXPECT_EQ(driver_state.GetConstraintsDisabledCount(), 6);
  EXPECT_TRUE(
      driver_state.WasConstraintDisabled(system_cgroup_path, "cpu.weight"));
  EXPECT_TRUE(
      driver_state.WasConstraintDisabled(system_cgroup_path, "memory.min"));
  EXPECT_TRUE(
      driver_state.WasConstraintDisabled(system_cgroup_path, "memory.low"));
  EXPECT_TRUE(
      driver_state.WasConstraintDisabled(user_cgroup_path, "cpu.weight"));
  EXPECT_TRUE(
      driver_state.WasConstraintDisabled(user_cgroup_path, "memory.high"));
  EXPECT_TRUE(
      driver_state.WasConstraintDisabled(user_cgroup_path, "memory.max"));

  // Controllers were disabled second (6 total)
  EXPECT_EQ(driver_state.GetControllersDisabledCount(), 6);
  EXPECT_LT(driver_state.GetLastConstraintOrder(),
            driver_state.GetFirstControllerOrder());

  // Check controller disable order
  EXPECT_EQ(driver_state.GetControllerDisabledCgroup(0), user_cgroup_path);
  EXPECT_EQ(driver_state.GetControllerDisabledCgroup(1), system_cgroup_path);
  EXPECT_EQ(driver_state.GetControllerDisabledCgroup(2), node_cgroup_path);
  EXPECT_EQ(driver_state.GetControllerDisabledCgroup(3), base_cgroup_path);
  EXPECT_EQ(driver_state.GetControllerDisabledCgroup(4), node_cgroup_path);
  EXPECT_EQ(driver_state.GetControllerDisabledCgroup(5), base_cgroup_path);

  // Processes were moved third (3 total)
  EXPECT_EQ(driver_state.GetProcessesMovedCount(), 3);
  EXPECT_LT(driver_state.GetLastConstraintOrder(),
            driver_state.GetLastProcessMovedOrder());

  EXPECT_EQ(driver_state.GetProcessMovedTo(0), base_cgroup_path);
  EXPECT_EQ(driver_state.CountProcessesMovedFrom(system_leaf_cgroup_path, base_cgroup_path), 1);
  EXPECT_EQ(driver_state.CountProcessesMovedFrom(non_ray_cgroup_path, base_cgroup_path), 1);
  EXPECT_EQ(driver_state.CountProcessesMovedFrom(workers_cgroup_path, base_cgroup_path), 1);

  // Cgroups were deleted last (6 total, in reverse order)
  EXPECT_EQ(driver_state.GetDeletedCgroupsCount(), 6);
  EXPECT_LT(driver_state.GetLastProcessMovedOrder(),
            driver_state.GetFirstDeletedOrder());

  EXPECT_EQ(driver_state.GetDeletedCgroup(0), non_ray_cgroup_path);
  EXPECT_EQ(driver_state.GetDeletedCgroup(1), workers_cgroup_path);
  EXPECT_EQ(driver_state.GetDeletedCgroup(2), user_cgroup_path);
  EXPECT_EQ(driver_state.GetDeletedCgroup(3), system_leaf_cgroup_path);
  EXPECT_EQ(driver_state.GetDeletedCgroup(4), system_cgroup_path);
  EXPECT_EQ(driver_state.GetDeletedCgroup(5), node_cgroup_path);
}

TEST(RustCgroupManagerTest, AddProcessToSystemCgroupFailsIfInvalidProcess) {
  RustFakeCgroupDriverState driver_state;
  driver_state.AddCgroup("/sys/fs/cgroup", {5}, {"cpu", "memory"});

  auto [result, manager] =
      RustCgroupManager::Create("/sys/fs/cgroup",
                                "node_id_123",
                                100,
                                1000000,
                                1000000,
                                10000000,
                                100000000,
                                driver_state);

  ASSERT_TRUE(result.ok()) << result.ToString();
  ASSERT_NE(manager, nullptr);

  driver_state.SetAddProcessStatus(RustCgroupManagerStatus::InvalidArgument);

  auto s = manager->AddProcessToSystemCgroup("-1");
  EXPECT_TRUE(s.IsInvalidArgument()) << s.ToString();
}

// Note: The EXPECT_DEATH tests from the original C++ tests are replaced with
// status checks since the Rust FFI doesn't use RAY_CHECK fatals the same way.

TEST(RustCgroupManagerTest, AddProcessToSystemCgroupReturnsNotFoundIfCgroupDoesNotExist) {
  RustFakeCgroupDriverState driver_state;
  driver_state.AddCgroup("/sys/fs/cgroup", {5}, {"cpu", "memory"});

  auto [result, manager] =
      RustCgroupManager::Create("/sys/fs/cgroup",
                                "node_id_123",
                                100,
                                1000000,
                                1000000,
                                10000000,
                                100000000,
                                driver_state);

  ASSERT_TRUE(result.ok()) << result.ToString();
  ASSERT_NE(manager, nullptr);

  driver_state.SetAddProcessStatus(RustCgroupManagerStatus::NotFound);

  auto s = manager->AddProcessToSystemCgroup("-1");
  EXPECT_TRUE(s.IsNotFound()) << s.ToString();
}

TEST(RustCgroupManagerTest,
     AddProcessToSystemCgroupReturnsPermissionDeniedIfNoPermissions) {
  RustFakeCgroupDriverState driver_state;
  driver_state.AddCgroup("/sys/fs/cgroup", {5}, {"cpu", "memory"});

  auto [result, manager] =
      RustCgroupManager::Create("/sys/fs/cgroup",
                                "node_id_123",
                                100,
                                1000000,
                                1000000,
                                10000000,
                                100000000,
                                driver_state);

  ASSERT_TRUE(result.ok()) << result.ToString();
  ASSERT_NE(manager, nullptr);

  driver_state.SetAddProcessStatus(RustCgroupManagerStatus::PermissionDenied);

  auto s = manager->AddProcessToSystemCgroup("-1");
  EXPECT_TRUE(s.IsPermissionDenied()) << s.ToString();
}

TEST(RustCgroupManagerTest,
     AddProcessToSystemCgroupSucceedsWithValidProcessAndPermissions) {
  RustFakeCgroupDriverState driver_state;
  driver_state.AddCgroup("/sys/fs/cgroup", {5}, {"cpu", "memory"});

  auto [result, manager] =
      RustCgroupManager::Create("/sys/fs/cgroup",
                                "node_id_123",
                                100,
                                1000000,
                                1000000,
                                10000000,
                                100000000,
                                driver_state);

  ASSERT_TRUE(result.ok()) << result.ToString();
  ASSERT_NE(manager, nullptr);

  auto s = manager->AddProcessToSystemCgroup("5");
  EXPECT_TRUE(s.ok()) << s.ToString();
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
