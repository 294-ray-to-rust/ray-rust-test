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

// Rust FFI tests for SharedLruCache.
// Matches: src/ray/util/tests/shared_lru_test.cc

#include "gtest/gtest.h"
#include "ray/ffi/rust_shared_lru.h"

namespace ray {

constexpr size_t kTestCacheSz = 1;

TEST(RustSharedLruCache, PutAndGet) {
  RustSharedLruCache cache{kTestCacheSz};

  // No value initially.
  auto val = cache.Get("1");
  EXPECT_EQ(val, nullptr);

  // Check put and get.
  cache.Put("1", "1");
  val = cache.Get("1");
  EXPECT_NE(val, nullptr);
  EXPECT_EQ(*val, "1");

  // Check key eviction.
  cache.Put("2", "2");
  val = cache.Get("1");
  EXPECT_EQ(val, nullptr);
  val = cache.Get("2");
  EXPECT_NE(val, nullptr);
  EXPECT_EQ(*val, "2");

  // Check deletion.
  EXPECT_FALSE(cache.Delete("1"));
  val = cache.Get("1");
  EXPECT_EQ(val, nullptr);
}

// Testing scenario: push multiple same keys into the cache.
TEST(RustSharedLruCache, SameKeyTest) {
  RustIntLruCache cache{2};

  cache.Put(1, 1);
  auto val = cache.Get(1);
  EXPECT_NE(val, nullptr);
  EXPECT_EQ(1, *val);

  cache.Put(1, 2);
  val = cache.Get(1);
  EXPECT_NE(val, nullptr);
  EXPECT_EQ(2, *val);
}

TEST(RustSharedLruCache, MaxEntriesTest) {
  RustSharedLruCache cache1{10};
  EXPECT_EQ(cache1.max_entries(), 10);

  RustSharedLruCache cache2{0};
  EXPECT_EQ(cache2.max_entries(), 0);
}

TEST(RustSharedLruCache, ClearTest) {
  RustSharedLruCache cache{10};

  cache.Put("a", "1");
  cache.Put("b", "2");
  cache.Put("c", "3");

  EXPECT_EQ(cache.size(), 3);
  EXPECT_NE(cache.Get("a"), nullptr);

  cache.Clear();

  EXPECT_EQ(cache.size(), 0);
  EXPECT_EQ(cache.Get("a"), nullptr);
  EXPECT_EQ(cache.Get("b"), nullptr);
  EXPECT_EQ(cache.Get("c"), nullptr);
}

TEST(RustSharedLruCache, EvictionOrderTest) {
  // Cache with capacity 3
  RustSharedLruCache cache{3};

  cache.Put("a", "1");
  cache.Put("b", "2");
  cache.Put("c", "3");

  // Access "a" to make it most recently used
  auto val = cache.Get("a");
  EXPECT_NE(val, nullptr);

  // Add a new entry, should evict "b" (least recently used)
  cache.Put("d", "4");

  // "a" and "c" should still be there, "b" should be evicted
  EXPECT_NE(cache.Get("a"), nullptr);
  EXPECT_EQ(cache.Get("b"), nullptr);  // evicted
  EXPECT_NE(cache.Get("c"), nullptr);
  EXPECT_NE(cache.Get("d"), nullptr);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
