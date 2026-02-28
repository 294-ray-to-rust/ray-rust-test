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

// Rust FFI tests for filesystem utilities.
// Matches: src/ray/util/tests/filesystem_test.cc

#include "gtest/gtest.h"
#include "ray/ffi/rust_filesystem.h"

namespace ray {

TEST(RustFileSystemTest, PathParseTest) {
  ASSERT_EQ(RustGetFileName("."), ".");
  ASSERT_EQ(RustGetFileName(".."), "..");
  ASSERT_EQ(RustGetFileName("foo/bar"), "bar");
  ASSERT_EQ(RustGetFileName("///bar"), "bar");
  ASSERT_EQ(RustGetFileName("///bar/"), "");
}

TEST(RustFileSystemTest, JoinPathTest) {
  // Basic path joining
  auto result = RustJoinPaths(RustGetUserTempDir(), "hello");
  ASSERT_FALSE(result.empty());

  // Join with subdir starting with separator
  result = RustJoinPaths(RustGetUserTempDir(), "hello", "subdir", "more", "last");
  ASSERT_FALSE(result.empty());

  // Verify components are included
  ASSERT_NE(result.find("hello"), std::string::npos);
  ASSERT_NE(result.find("subdir"), std::string::npos);
  ASSERT_NE(result.find("more"), std::string::npos);
  ASSERT_NE(result.find("last"), std::string::npos);
}

TEST(RustFileSystemTest, IsDirSepTest) {
  ASSERT_TRUE(RustIsDirSep('/'));
  ASSERT_TRUE(RustIsDirSep('\\'));
  ASSERT_FALSE(RustIsDirSep('a'));
  ASSERT_FALSE(RustIsDirSep('.'));
  ASSERT_FALSE(RustIsDirSep(':'));
}

TEST(RustFileSystemTest, GetUserTempDirTest) {
  auto temp_dir = RustGetUserTempDir();
  ASSERT_FALSE(temp_dir.empty());
  // Should be a valid path starting with /
  ASSERT_TRUE(temp_dir[0] == '/' || temp_dir[0] == '\\' ||
              (temp_dir.size() >= 2 && temp_dir[1] == ':'));
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
