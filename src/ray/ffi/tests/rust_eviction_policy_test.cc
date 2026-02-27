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

// Rust FFI tests for eviction policy types (LRUCache).
// Matches: src/ray/object_manager/plasma/tests/eviction_policy_test.cc

#include <algorithm>
#include <vector>

#include "gtest/gtest.h"
#include "ray/ffi/rust_id.h"
#include "ray/ffi/rust_plasma.h"

namespace ray {

class RustLRUCacheTest : public ::testing::Test {};

TEST_F(RustLRUCacheTest, BasicCapacity) {
  RustLRUCache cache("cache", 1024);
  EXPECT_EQ(1024, cache.Capacity());
  EXPECT_EQ(1024, cache.OriginalCapacity());
  EXPECT_EQ(1024, cache.RemainingCapacity());
}

TEST_F(RustLRUCacheTest, AddAndRemove) {
  RustLRUCache cache("cache", 1024);

  RustObjectId key1 = RustObjectId::FromRandom();
  int64_t size1 = 32;
  cache.Add(key1.Binary(), size1);
  EXPECT_EQ(1024 - size1, cache.RemainingCapacity());

  RustObjectId key2 = RustObjectId::FromRandom();
  int64_t size2 = 64;
  cache.Add(key2.Binary(), size2);
  EXPECT_EQ(1024 - size1 - size2, cache.RemainingCapacity());

  cache.Remove(key1.Binary());
  EXPECT_EQ(1024 - size2, cache.RemainingCapacity());

  cache.Remove(key2.Binary());
  EXPECT_EQ(1024, cache.RemainingCapacity());
}

TEST_F(RustLRUCacheTest, ChooseObjectsToEvict) {
  RustLRUCache cache("cache", 1024);

  RustObjectId key1 = RustObjectId::FromRandom();
  int64_t size1 = 10;
  RustObjectId key2 = RustObjectId::FromRandom();
  int64_t size2 = 10;

  cache.Add(key1.Binary(), size1);
  cache.Add(key2.Binary(), size2);

  {
    std::vector<std::string> objects_to_evict;
    EXPECT_EQ(20, cache.ChooseObjectsToEvict(15, objects_to_evict));
    EXPECT_EQ(2u, objects_to_evict.size());
  }

  {
    std::vector<std::string> objects_to_evict;
    EXPECT_EQ(20, cache.ChooseObjectsToEvict(30, objects_to_evict));
    EXPECT_EQ(2u, objects_to_evict.size());
  }
}

TEST_F(RustLRUCacheTest, Foreach) {
  RustLRUCache cache("cache", 1024);

  RustObjectId key1 = RustObjectId::FromRandom();
  RustObjectId key2 = RustObjectId::FromRandom();
  std::vector<std::string> keys{key1.Binary(), key2.Binary()};

  cache.Add(key1.Binary(), 10);
  cache.Add(key2.Binary(), 10);

  std::vector<std::string> foreach_keys;
  cache.Foreach([&foreach_keys](const std::string &key) {
    foreach_keys.push_back(key);
  });

  // Foreach returns in LRU order (oldest first), which is the same
  // as insertion order
  EXPECT_EQ(foreach_keys, keys);
}

TEST_F(RustLRUCacheTest, AdjustCapacity) {
  RustLRUCache cache("cache", 1024);

  cache.AdjustCapacity(1024);
  EXPECT_EQ(2048, cache.Capacity());
  EXPECT_EQ(1024, cache.OriginalCapacity());
}

TEST_F(RustLRUCacheTest, Exists) {
  RustLRUCache cache("cache", 1024);

  RustObjectId key1 = RustObjectId::FromRandom();
  cache.Add(key1.Binary(), 10);

  EXPECT_TRUE(cache.Exists(key1.Binary()));

  RustObjectId key2 = RustObjectId::FromRandom();
  EXPECT_FALSE(cache.Exists(key2.Binary()));
}

TEST_F(RustLRUCacheTest, SizeAndEmpty) {
  RustLRUCache cache("cache", 1024);

  EXPECT_TRUE(cache.IsEmpty());
  EXPECT_EQ(0u, cache.Size());

  RustObjectId key1 = RustObjectId::FromRandom();
  cache.Add(key1.Binary(), 10);

  EXPECT_FALSE(cache.IsEmpty());
  EXPECT_EQ(1u, cache.Size());

  RustObjectId key2 = RustObjectId::FromRandom();
  cache.Add(key2.Binary(), 10);

  EXPECT_EQ(2u, cache.Size());
}

TEST_F(RustLRUCacheTest, LRUOrder) {
  RustLRUCache cache("cache", 1024);

  RustObjectId key1 = RustObjectId::FromRandom();
  RustObjectId key2 = RustObjectId::FromRandom();
  RustObjectId key3 = RustObjectId::FromRandom();

  cache.Add(key1.Binary(), 10);
  cache.Add(key2.Binary(), 20);
  cache.Add(key3.Binary(), 30);

  // Evict should return oldest first (key1)
  std::vector<std::string> to_evict;
  cache.ChooseObjectsToEvict(10, to_evict);

  EXPECT_EQ(1u, to_evict.size());
  EXPECT_EQ(key1.Binary(), to_evict[0]);
}

TEST_F(RustLRUCacheTest, RemoveReturnsSize) {
  RustLRUCache cache("cache", 1024);

  RustObjectId key1 = RustObjectId::FromRandom();
  cache.Add(key1.Binary(), 42);

  int64_t removed_size = cache.Remove(key1.Binary());
  EXPECT_EQ(42, removed_size);

  // Removing non-existent key returns 0
  int64_t removed_again = cache.Remove(key1.Binary());
  EXPECT_EQ(0, removed_again);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
