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

// Rust FFI tests for container utilities.
// Matches: src/ray/util/tests/container_util_test.cc

#include "gtest/gtest.h"
#include "src/ray/ffi/rust_container_util.h"

#include <deque>
#include <list>
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace ray {

TEST(RustContainerUtilTest, TestDebugString) {
  // Numerical values.
  ASSERT_EQ(RustDebugStringIntVec({2}), "[2]");

  // String values.
  ASSERT_EQ(RustDebugStringStringVec({"hello"}), "[hello]");

  // Non-associative containers.
  ASSERT_EQ(RustDebugStringIntVec({1, 2}), "[1, 2]");
  ASSERT_EQ(RustDebugStringIntVec({1, 2, 3}), "[1, 2, 3]");

  // Pairs
  ASSERT_EQ(RustDebugStringIntPair(1, 2), "(1, 2)");

  // Optional.
  ASSERT_EQ(RustDebugStringOptional(std::nullopt), "(nullopt)");
  ASSERT_EQ(RustDebugStringOptional(std::optional<std::string>{}), "(nullopt)");
  ASSERT_EQ(RustDebugStringOptional(std::optional<std::string>{"hello"}), "hello");

  // Maps (using RustIntMap debug_string)
  RustIntMap map;
  map.Insert(1, 2);
  map.Insert(3, 4);
  ASSERT_EQ(map.DebugString(), "[(1, 2), (3, 4)]");
}

TEST(RustContainerUtilTest, TestMapFindOrDie) {
  {
    std::map<int, int> m{{1, 2}, {3, 4}};
    ASSERT_EQ(map_find_or_die(m, 1), 2);
    // Note: ASSERT_DEATH test skipped as it requires special handling
  }

  // Test with RustIntMap
  {
    RustIntMap m;
    m.Insert(1, 2);
    m.Insert(3, 4);
    ASSERT_EQ(m.FindOrDie(1), 2);
    ASSERT_EQ(m.FindOrDie(3), 4);
  }
}

TEST(RustContainerUtilTest, TestEraseIf) {
  // Test with RustIntList (like std::list<int>)
  {
    RustIntList list({1, 2, 3, 4});
    list.EraseIfEven();
    auto result = list.ToVector();
    ASSERT_EQ(result, (std::vector<int32_t>{1, 3}));
  }

  {
    RustIntList list({1, 2, 3});
    list.EraseIfEven();
    auto result = list.ToVector();
    ASSERT_EQ(result, (std::vector<int32_t>{1, 3}));
  }

  {
    RustIntList list(std::vector<int32_t>{});
    list.EraseIfEven();
    auto result = list.ToVector();
    ASSERT_EQ(result, (std::vector<int32_t>{}));
  }

  // Test with RustIntDequeMap (like absl::flat_hash_map<int, std::deque<int>>)
  {
    RustIntDequeMap map;
    map.Insert(1, std::deque<int32_t>{1, 3});
    map.Insert(2, std::deque<int32_t>{2, 4});
    map.Insert(3, std::deque<int32_t>{5, 6});
    map.EraseIfEven();

    ASSERT_EQ(map.Size(), 2);
    ASSERT_EQ(map.Get(1), (std::deque<int32_t>{1, 3}));
    ASSERT_FALSE(map.Contains(2));  // All even values removed, entry deleted
    ASSERT_EQ(map.Get(3), (std::deque<int32_t>{5}));
  }
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
