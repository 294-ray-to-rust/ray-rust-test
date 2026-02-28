// Copyright 2024 The Ray Authors.
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

// Rust FFI tests for MemoryMonitorUtils.
// Matches: src/ray/common/tests/memory_monitor_utils_test.cc

#include "gtest/gtest.h"
#include "ray/ffi/rust_memory_monitor.h"

#include <cstdlib>
#include <fstream>
#include <string>

namespace ray {

class RustMemoryMonitorUtilsTest : public ::testing::Test {
 protected:
  std::string CreateTempFile(const std::string& content) {
    char tmpl[] = "/tmp/rust_mem_test_XXXXXX";
    int fd = mkstemp(tmpl);
    if (fd == -1) return "";
    close(fd);
    std::ofstream file(tmpl);
    file << content;
    file.close();
    return std::string(tmpl);
  }

  std::string CreateTempDir() {
    char tmpl[] = "/tmp/rust_mem_test_XXXXXX";
    char* dir = mkdtemp(tmpl);
    if (dir == nullptr) return "";
    return std::string(dir);
  }
};

TEST_F(RustMemoryMonitorUtilsTest, TestGetMemoryThresholdTakeGreaterOfTheTwoValues) {
  auto kNull = RustMemoryMonitorKNull();

  ASSERT_EQ(RustMemoryMonitorUtils::GetMemoryThreshold(100, 0.5, 0), 100);
  ASSERT_EQ(RustMemoryMonitorUtils::GetMemoryThreshold(100, 0.5, 60), 50);

  ASSERT_EQ(RustMemoryMonitorUtils::GetMemoryThreshold(100, 1, 10), 100);
  ASSERT_EQ(RustMemoryMonitorUtils::GetMemoryThreshold(100, 1, 100), 100);

  ASSERT_EQ(RustMemoryMonitorUtils::GetMemoryThreshold(100, 0.1, 100), 10);
  ASSERT_EQ(RustMemoryMonitorUtils::GetMemoryThreshold(100, 0, 10), 90);
  ASSERT_EQ(RustMemoryMonitorUtils::GetMemoryThreshold(100, 0, 100), 0);

  ASSERT_EQ(RustMemoryMonitorUtils::GetMemoryThreshold(100, 0, kNull), 0);
  ASSERT_EQ(RustMemoryMonitorUtils::GetMemoryThreshold(100, 0.5, kNull), 50);
  ASSERT_EQ(RustMemoryMonitorUtils::GetMemoryThreshold(100, 1, kNull), 100);
}

TEST_F(RustMemoryMonitorUtilsTest, TestCgroupFilesValidReturnsWorkingSet) {
  std::string stat_content =
      "random_key random_value\n"
      "inactive_file 123\n"
      "active_file 88\n"
      "another_random_key some_value\n";
  std::string stat_file = CreateTempFile(stat_content);

  std::string curr_content = "300\n";
  std::string curr_file = CreateTempFile(curr_content);

  int64_t used_bytes = RustMemoryMonitorUtils::GetCGroupMemoryUsedBytes(
      stat_file, curr_file, "inactive_file", "active_file");

  std::remove(stat_file.c_str());
  std::remove(curr_file.c_str());

  ASSERT_EQ(used_bytes, 300 - 123 - 88);
}

TEST_F(RustMemoryMonitorUtilsTest, TestCgroupFilesValidNegativeWorkingSet) {
  std::string stat_content =
      "random_key random_value\n"
      "inactive_file 300\n"
      "active_file 100\n";
  std::string stat_file = CreateTempFile(stat_content);

  std::string curr_content = "123\n";
  std::string curr_file = CreateTempFile(curr_content);

  int64_t used_bytes = RustMemoryMonitorUtils::GetCGroupMemoryUsedBytes(
      stat_file, curr_file, "inactive_file", "active_file");

  std::remove(stat_file.c_str());
  std::remove(curr_file.c_str());

  ASSERT_EQ(used_bytes, 123 - 300 - 100);
}

TEST_F(RustMemoryMonitorUtilsTest, TestCgroupFilesValidMissingFieldReturnskNull) {
  auto kNull = RustMemoryMonitorKNull();

  std::string stat_content =
      "random_key random_value\n"
      "another_random_key 123\n";
  std::string stat_file = CreateTempFile(stat_content);

  std::string curr_content = "300\n";
  std::string curr_file = CreateTempFile(curr_content);

  int64_t used_bytes = RustMemoryMonitorUtils::GetCGroupMemoryUsedBytes(
      stat_file, curr_file, "inactive_file", "active_file");

  std::remove(stat_file.c_str());
  std::remove(curr_file.c_str());

  ASSERT_EQ(used_bytes, kNull);
}

TEST_F(RustMemoryMonitorUtilsTest, TestCgroupNonexistentStatFileReturnskNull) {
  auto kNull = RustMemoryMonitorKNull();

  std::string curr_content = "300\n";
  std::string curr_file = CreateTempFile(curr_content);

  int64_t used_bytes = RustMemoryMonitorUtils::GetCGroupMemoryUsedBytes(
      "/nonexistent/stat/file", curr_file, "inactive_file", "active_file");

  std::remove(curr_file.c_str());

  ASSERT_EQ(used_bytes, kNull);
}

TEST_F(RustMemoryMonitorUtilsTest, TestCgroupNonexistentUsageFileReturnskNull) {
  auto kNull = RustMemoryMonitorKNull();

  std::string stat_content =
      "random_key random_value\n"
      "inactive_file 300\n"
      "active_file 88\n";
  std::string stat_file = CreateTempFile(stat_content);

  int64_t used_bytes = RustMemoryMonitorUtils::GetCGroupMemoryUsedBytes(
      stat_file, "/nonexistent/usage/file", "inactive_file", "active_file");

  std::remove(stat_file.c_str());

  ASSERT_EQ(used_bytes, kNull);
}

TEST_F(RustMemoryMonitorUtilsTest, TestGetPidsFromDirOnlyReturnsNumericFilenames) {
  std::string proc_dir = CreateTempDir();

  std::string num_filename = proc_dir + "/123";
  std::string non_num_filename = proc_dir + "/123b";

  std::ofstream num_file(num_filename);
  num_file << "content";
  num_file.close();

  std::ofstream non_num_file(non_num_filename);
  non_num_file << "content";
  non_num_file.close();

  auto pids = RustMemoryMonitorUtils::GetPidsFromDir(proc_dir);

  std::remove(num_filename.c_str());
  std::remove(non_num_filename.c_str());
  rmdir(proc_dir.c_str());

  ASSERT_EQ(pids.size(), 1);
  ASSERT_EQ(pids[0], 123);
}

TEST_F(RustMemoryMonitorUtilsTest, TestGetPidsFromNonExistentDirReturnsEmpty) {
  auto pids = RustMemoryMonitorUtils::GetPidsFromDir("/nonexistent/dir");
  ASSERT_EQ(pids.size(), 0);
}

TEST_F(RustMemoryMonitorUtilsTest, TestGetCommandLinePidExistReturnsValid) {
  std::string proc_dir = CreateTempDir();
  std::string pid_dir = proc_dir + "/123";
  mkdir(pid_dir.c_str(), 0755);

  std::string cmdline_filename = pid_dir + "/" + RustMemoryMonitorUtils::kCommandlinePath;

  std::ofstream cmdline_file(cmdline_filename);
  cmdline_file << "/my/very/custom/command --test passes!     ";
  cmdline_file.close();

  std::string commandline = RustMemoryMonitorUtils::GetCommandLineForPid(123, proc_dir);

  std::remove(cmdline_filename.c_str());
  rmdir(pid_dir.c_str());
  rmdir(proc_dir.c_str());

  ASSERT_EQ(commandline, "/my/very/custom/command --test passes!");
}

TEST_F(RustMemoryMonitorUtilsTest, TestGetCommandLineMissingFileReturnsEmpty) {
  std::string proc_dir = CreateTempDir();
  std::string commandline = RustMemoryMonitorUtils::GetCommandLineForPid(123, proc_dir);
  rmdir(proc_dir.c_str());
  ASSERT_EQ(commandline, "");
}

TEST_F(RustMemoryMonitorUtilsTest, TestShortStringNotTruncated) {
  std::string out = RustMemoryMonitorUtils::TruncateString("im short", 20);
  ASSERT_EQ(out, "im short");
}

TEST_F(RustMemoryMonitorUtilsTest, TestLongStringTruncated) {
  std::string out = RustMemoryMonitorUtils::TruncateString(std::string(7, 'k'), 5);
  ASSERT_EQ(out, "kkkkk...");
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
