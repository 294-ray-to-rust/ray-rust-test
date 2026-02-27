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

#include "src/ray/ffi/rust_allocator.h"

#include <filesystem>
#include <string>
#include <vector>

#include "gtest/gtest.h"

using namespace ray;
using std::filesystem::create_directories;
using std::filesystem::path;

namespace {
const int64_t kKB = 1024;
const int64_t kMB = 1024 * 1024;

std::string CreateTestDir() {
  path directory = std::filesystem::temp_directory_path() / "rust_allocator_test_XXXXXX";
  // Create a unique directory
  std::string dir_str = directory.string();
  create_directories(directory);
  return dir_str;
}
}  // namespace

class RustPlasmaAllocatorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    plasma_directory_ = CreateTestDir();
    fallback_directory_ = CreateTestDir();
  }

  void TearDown() override {
    // Clean up test directories
    std::filesystem::remove_all(plasma_directory_);
    std::filesystem::remove_all(fallback_directory_);
  }

  std::string plasma_directory_;
  std::string fallback_directory_;
};

TEST_F(RustPlasmaAllocatorTest, Creation) {
  RustPlasmaAllocator allocator(
      plasma_directory_, fallback_directory_, false, 2 * kMB);

  EXPECT_EQ(2 * kMB, allocator.GetFootprintLimit());
  EXPECT_EQ(0, allocator.Allocated());
  EXPECT_EQ(0, allocator.FallbackAllocated());
}

TEST_F(RustPlasmaAllocatorTest, PrimaryAllocation) {
  RustPlasmaAllocator allocator(
      plasma_directory_, fallback_directory_, false, 2 * kMB);

  auto alloc = allocator.Allocate(100 * kKB);
  ASSERT_TRUE(alloc.has_value());
  EXPECT_FALSE(alloc->is_fallback_allocated());
  EXPECT_EQ(100 * kKB, alloc->size());
  EXPECT_EQ(100 * kKB, allocator.Allocated());
  EXPECT_EQ(0, allocator.FallbackAllocated());

  allocator.Free(std::move(*alloc));
  EXPECT_EQ(0, allocator.Allocated());
}

TEST_F(RustPlasmaAllocatorTest, FallbackAllocation) {
  RustPlasmaAllocator allocator(
      plasma_directory_, fallback_directory_, false, 2 * kMB);

  auto alloc = allocator.FallbackAllocate(100 * kKB);
  ASSERT_TRUE(alloc.has_value());
  EXPECT_TRUE(alloc->is_fallback_allocated());
  EXPECT_EQ(100 * kKB, alloc->size());
  EXPECT_EQ(100 * kKB, allocator.Allocated());
  EXPECT_EQ(100 * kKB, allocator.FallbackAllocated());

  allocator.Free(std::move(*alloc));
  EXPECT_EQ(0, allocator.Allocated());
  EXPECT_EQ(0, allocator.FallbackAllocated());
}

TEST_F(RustPlasmaAllocatorTest, MultipleAllocations) {
  RustPlasmaAllocator allocator(
      plasma_directory_, fallback_directory_, false, 2 * kMB);

  std::vector<RustAllocation> allocations;

  // Allocate multiple blocks
  for (int i = 0; i < 5; i++) {
    auto alloc = allocator.Allocate(100 * kKB);
    ASSERT_TRUE(alloc.has_value());
    allocations.push_back(std::move(*alloc));
  }

  EXPECT_EQ(500 * kKB, allocator.Allocated());

  // Free them all
  for (auto &alloc : allocations) {
    allocator.Free(std::move(alloc));
  }

  EXPECT_EQ(0, allocator.Allocated());
}

TEST_F(RustPlasmaAllocatorTest, FallbackPassThrough) {
  // Matches the original fallback_allocator_test.cc behavior
  // Account for reserved bytes (256 * sizeof(size_t)) used by allocator
  int64_t limit = 256 * sizeof(size_t) + 2 * kMB;
  int64_t object_size = 900 * kKB;

  RustPlasmaAllocator allocator(
      plasma_directory_, fallback_directory_, false, limit);

  EXPECT_EQ(limit, allocator.GetFootprintLimit());

  // First round of allocations
  {
    auto allocation_1 = allocator.Allocate(object_size);
    ASSERT_TRUE(allocation_1.has_value());
    EXPECT_FALSE(allocation_1->is_fallback_allocated());

    auto allocation_2 = allocator.Allocate(object_size);
    ASSERT_TRUE(allocation_2.has_value());
    EXPECT_FALSE(allocation_2->is_fallback_allocated());

    EXPECT_EQ(2 * object_size, allocator.Allocated());

    allocator.Free(std::move(*allocation_1));
    auto allocation_3 = allocator.Allocate(object_size);
    ASSERT_TRUE(allocation_3.has_value());
    EXPECT_EQ(0, allocator.FallbackAllocated());
    EXPECT_EQ(2 * object_size, allocator.Allocated());

    allocator.Free(std::move(*allocation_2));
    allocator.Free(std::move(*allocation_3));
    EXPECT_EQ(0, allocator.Allocated());
  }

  // Second round - test capacity limits
  int64_t expect_allocated = 0;
  int64_t expect_fallback_allocated = 0;
  std::vector<RustAllocation> allocations;
  std::vector<RustAllocation> fallback_allocations;

  // Fill up primary
  for (int i = 0; i < 2; i++) {
    auto allocation = allocator.Allocate(kMB);
    expect_allocated += kMB;
    ASSERT_TRUE(allocation.has_value());
    EXPECT_FALSE(allocation->is_fallback_allocated());
    EXPECT_EQ(expect_allocated, allocator.Allocated());
    EXPECT_EQ(0, allocator.FallbackAllocated());
    allocations.push_back(std::move(*allocation));
  }

  // Over allocation should fail
  {
    auto allocation = allocator.Allocate(kMB);
    EXPECT_FALSE(allocation.has_value());
    EXPECT_EQ(0, allocator.FallbackAllocated());
    EXPECT_EQ(expect_allocated, allocator.Allocated());
  }

  // Fallback allocation should succeed
  {
    for (int i = 0; i < 2; i++) {
      auto allocation = allocator.FallbackAllocate(kMB);
      expect_allocated += kMB;
      expect_fallback_allocated += kMB;
      ASSERT_TRUE(allocation.has_value());
      EXPECT_TRUE(allocation->is_fallback_allocated());
      EXPECT_EQ(expect_allocated, allocator.Allocated());
      EXPECT_EQ(expect_fallback_allocated, allocator.FallbackAllocated());
      fallback_allocations.push_back(std::move(*allocation));
    }
  }

  // Free fallback allocation
  {
    auto allocation = std::move(fallback_allocations.back());
    fallback_allocations.pop_back();
    allocator.Free(std::move(allocation));
    EXPECT_EQ(3 * kMB, allocator.Allocated());
    EXPECT_EQ(1 * kMB, allocator.FallbackAllocated());
  }

  // Free primary allocation
  {
    auto allocation = std::move(allocations.back());
    allocations.pop_back();
    allocator.Free(std::move(allocation));
    EXPECT_EQ(2 * kMB, allocator.Allocated());
    EXPECT_EQ(1 * kMB, allocator.FallbackAllocated());

    // Now we can allocate from primary again
    auto new_allocation = allocator.Allocate(kMB);
    ASSERT_TRUE(new_allocation.has_value());
    EXPECT_EQ(3 * kMB, allocator.Allocated());
    EXPECT_EQ(1 * kMB, allocator.FallbackAllocated());

    allocator.Free(std::move(*new_allocation));
  }

  // Cleanup remaining allocations
  for (auto &alloc : allocations) {
    allocator.Free(std::move(alloc));
  }
  for (auto &alloc : fallback_allocations) {
    allocator.Free(std::move(alloc));
  }

  EXPECT_EQ(0, allocator.Allocated());
  EXPECT_EQ(0, allocator.FallbackAllocated());
}

TEST_F(RustPlasmaAllocatorTest, WriteAndRead) {
  RustPlasmaAllocator allocator(
      plasma_directory_, fallback_directory_, false, 2 * kMB);

  auto alloc = allocator.Allocate(1000);
  ASSERT_TRUE(alloc.has_value());

  // Write data
  uint8_t *ptr = reinterpret_cast<uint8_t *>(alloc->address());
  for (size_t i = 0; i < alloc->size(); i++) {
    ptr[i] = static_cast<uint8_t>(i % 256);
  }

  // Read and verify
  for (size_t i = 0; i < alloc->size(); i++) {
    EXPECT_EQ(static_cast<uint8_t>(i % 256), ptr[i]);
  }

  allocator.Free(std::move(*alloc));
}

TEST_F(RustPlasmaAllocatorTest, FallbackWriteAndRead) {
  RustPlasmaAllocator allocator(
      plasma_directory_, fallback_directory_, false, 2 * kMB);

  auto alloc = allocator.FallbackAllocate(1000);
  ASSERT_TRUE(alloc.has_value());
  EXPECT_TRUE(alloc->is_fallback_allocated());

  // Write data
  uint8_t *ptr = reinterpret_cast<uint8_t *>(alloc->address());
  for (size_t i = 0; i < alloc->size(); i++) {
    ptr[i] = static_cast<uint8_t>(i % 256);
  }

  // Read and verify
  for (size_t i = 0; i < alloc->size(); i++) {
    EXPECT_EQ(static_cast<uint8_t>(i % 256), ptr[i]);
  }

  allocator.Free(std::move(*alloc));
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
