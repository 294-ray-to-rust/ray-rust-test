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

// Rust FFI tests for cgroup utilities.
// Matches: src/ray/common/cgroup2/tests/sysfs_cgroup_driver_test.cc

#include "gtest/gtest.h"
#include "src/ray/ffi/rust_cgroup.h"

#include <filesystem>
#include <memory>
#include <string>

namespace ray {

TEST(RustSysFsCgroupDriverTest, CheckCgroupv2EnabledFailsIfEmptyMountFile) {
  RustTempFile temp_mount_file;
  RustSysFsCgroupDriver driver(temp_mount_file.GetPath());
  auto s = driver.CheckCgroupv2Enabled();
  EXPECT_TRUE(s.IsInvalid()) << s.ToString();
}

TEST(RustSysFsCgroupDriverTest, CheckCgroupv2EnabledFailsIfMalformedMountFile) {
  RustTempFile temp_mount_file;
  temp_mount_file.AppendLine("cgroup /sys/fs/cgroup rw 0 0\n");
  temp_mount_file.AppendLine("cgroup2 /sys/fs/cgroup/unified/ rw 0 0\n");
  temp_mount_file.AppendLine("oopsie");
  RustSysFsCgroupDriver driver(temp_mount_file.GetPath());
  auto s = driver.CheckCgroupv2Enabled();
  EXPECT_TRUE(s.IsInvalid()) << s.ToString();
}

TEST(RustSysFsCgroupDriverTest,
     CheckCgroupv2EnabledFailsIfCgroupv1MountedAndCgroupv2NotMounted) {
  RustTempFile temp_mount_file;
  temp_mount_file.AppendLine("cgroup /sys/fs/cgroup rw 0 0\n");
  RustSysFsCgroupDriver driver(temp_mount_file.GetPath());
  auto s = driver.CheckCgroupv2Enabled();
  ASSERT_TRUE(s.IsInvalid()) << s.ToString();
}

TEST(RustSysFsCgroupDriverTest,
     CheckCgroupv2EnabledFailsIfCgroupv1MountedAndCgroupv2Mounted) {
  RustTempFile temp_mount_file;
  temp_mount_file.AppendLine("cgroup /sys/fs/cgroup rw 0 0\n");
  temp_mount_file.AppendLine("cgroup2 /sys/fs/cgroup/unified/ rw 0 0\n");
  RustSysFsCgroupDriver driver(temp_mount_file.GetPath());
  auto s = driver.CheckCgroupv2Enabled();
  ASSERT_TRUE(s.IsInvalid()) << s.ToString();
}

TEST(RustSysFsCgroupDriverTest,
     CheckCgroupv2EnabledSucceedsIfMountFileNotFoundButFallbackFileIsCorrect) {
  RustTempFile temp_fallback_mount_file;
  temp_fallback_mount_file.AppendLine("cgroup2 /sys/fs/cgroup cgroup2 rw 0 0\n");
  RustSysFsCgroupDriver driver("/does/not/exist", temp_fallback_mount_file.GetPath());
  auto s = driver.CheckCgroupv2Enabled();
  EXPECT_TRUE(s.ok()) << s.ToString();
}

TEST(RustSysFsCgroupDriverTest, CheckCgroupv2EnabledSucceedsIfOnlyCgroupv2Mounted) {
  RustTempFile temp_mount_file;
  temp_mount_file.AppendLine("cgroup2 /sys/fs/cgroup cgroup2 rw 0 0\n");
  RustSysFsCgroupDriver driver(temp_mount_file.GetPath());
  auto s = driver.CheckCgroupv2Enabled();
  EXPECT_TRUE(s.ok()) << s.ToString();
}

TEST(RustSysFsCgroupDriver, CheckCgroupFailsIfNotCgroupv2Path) {
  auto [ok, temp_dir] = RustTempDirectory::Create();
  ASSERT_TRUE(ok) << "Failed to create temp directory";
  RustSysFsCgroupDriver driver;
  auto s = driver.CheckCgroup(temp_dir->GetPath());
  EXPECT_TRUE(s.IsInvalidArgument()) << s.ToString();
}

TEST(RustSysFsCgroupDriver, CheckCgroupFailsIfCgroupDoesNotExist) {
  RustSysFsCgroupDriver driver;
  auto s = driver.CheckCgroup("/some/path/that/doesnt/exist");
  EXPECT_TRUE(s.IsNotFound()) << s.ToString();
}

TEST(RustSysFsCgroupDriver, DeleteCgroupFailsIfNotCgroup2Path) {
  auto [ok, temp_dir] = RustTempDirectory::Create();
  ASSERT_TRUE(ok) << "Failed to create temp directory";
  RustSysFsCgroupDriver driver;
  auto s = driver.DeleteCgroup(temp_dir->GetPath());
  EXPECT_TRUE(s.IsInvalidArgument()) << s.ToString();
}

TEST(RustSysFsCgroupDriver, DeleteCgroupFailsIfCgroupDoesNotExist) {
  RustSysFsCgroupDriver driver;
  auto s = driver.DeleteCgroup("/some/path/that/doesnt/exist");
  EXPECT_TRUE(s.IsNotFound()) << s.ToString();
}

TEST(RustSysFsCgroupDriver, GetAvailableControllersFailsIfNotCgroup2Path) {
  auto [ok, temp_dir] = RustTempDirectory::Create();
  ASSERT_TRUE(ok) << "Failed to create temp directory";

  // Create controllers file
  std::filesystem::path controller_file_path =
      std::filesystem::path(temp_dir->GetPath()) /
      std::filesystem::path("cgroup.controllers");
  RustTempFile controller_file(controller_file_path.string());
  controller_file.AppendLine("cpuset cpu io memory hugetlb pids rdma misc");

  RustSysFsCgroupDriver driver;
  auto [s, controllers] = driver.GetAvailableControllers(temp_dir->GetPath());
  EXPECT_TRUE(s.IsInvalidArgument()) << s.ToString();
}

TEST(RustSysFsCgroupDriver, EnableControllerFailsIfNotCgroupv2Path) {
  auto [ok, temp_dir] = RustTempDirectory::Create();
  ASSERT_TRUE(ok) << "Failed to create temp directory";
  RustSysFsCgroupDriver driver;
  auto s = driver.EnableController(temp_dir->GetPath(), "cpu");
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
}

TEST(RustSysFsCgroupDriver, DisableControllerFailsIfNotCgroupv2Path) {
  auto [ok, temp_dir] = RustTempDirectory::Create();
  ASSERT_TRUE(ok) << "Failed to create temp directory";
  RustSysFsCgroupDriver driver;
  auto s = driver.DisableController(temp_dir->GetPath(), "cpu");
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
}

TEST(RustSysFsCgroupDriver, AddConstraintFailsIfNotCgroupv2Path) {
  auto [ok, temp_dir] = RustTempDirectory::Create();
  ASSERT_TRUE(ok) << "Failed to create temp directory";
  RustSysFsCgroupDriver driver;
  auto s = driver.AddConstraint(temp_dir->GetPath(), "memory.min", "1");
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
