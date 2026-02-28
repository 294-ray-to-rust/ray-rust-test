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

// Rust FFI tests for utility types.
// Matches: src/ray/util/tests/exponential_backoff_test.cc
//          src/ray/util/tests/string_utils_test.cc
//          src/ray/util/tests/size_literals_test.cc

#include "gtest/gtest.h"
#include "ray/ffi/rust_util.h"

namespace ray {

// =============================================================================
// ExponentialBackoff Tests (matches exponential_backoff_test.cc)
// =============================================================================

class RustExponentialBackoffTest : public ::testing::Test {};

TEST_F(RustExponentialBackoffTest, TestExponentialIncrease) {
  ASSERT_EQ(RustExponentialBackoff::GetBackoffMs(0, 157), 157 * 1);
  ASSERT_EQ(RustExponentialBackoff::GetBackoffMs(1, 157), 157 * 2);
  ASSERT_EQ(RustExponentialBackoff::GetBackoffMs(2, 157), 157 * 4);
  ASSERT_EQ(RustExponentialBackoff::GetBackoffMs(3, 157), 157 * 8);

  ASSERT_EQ(RustExponentialBackoff::GetBackoffMs(10, 0), 0);
  ASSERT_EQ(RustExponentialBackoff::GetBackoffMs(11, 0), 0);
}

TEST_F(RustExponentialBackoffTest, TestExceedMaxBackoffReturnsMaxBackoff) {
  auto backoff = RustExponentialBackoff::GetBackoffMs(
      /*attempt*/ 10, /*base_ms*/ 1, /*max_backoff_ms*/ 5);
  ASSERT_EQ(backoff, 5);
}

TEST_F(RustExponentialBackoffTest, TestOverflowReturnsMaxBackoff) {
  // 2^64+ will overflow.
  for (int i = 64; i < 10000; i++) {
    auto backoff = RustExponentialBackoff::GetBackoffMs(
        /*attempt*/ i,
        /*base_ms*/ 1,
        /*max_backoff_ms*/ 1234);
    ASSERT_EQ(backoff, 1234);
  }
}

TEST_F(RustExponentialBackoffTest, GetNext) {
  auto exp = RustExponentialBackoff{1, 2, 9};
  ASSERT_EQ(1, exp.Next());
  ASSERT_EQ(2, exp.Next());
  ASSERT_EQ(4, exp.Next());
  ASSERT_EQ(8, exp.Next());
  ASSERT_EQ(9, exp.Next());
  ASSERT_EQ(9, exp.Next());
  exp.Reset();
  ASSERT_EQ(1, exp.Next());
  ASSERT_EQ(2, exp.Next());
  ASSERT_EQ(4, exp.Next());
  ASSERT_EQ(8, exp.Next());
  ASSERT_EQ(9, exp.Next());
  ASSERT_EQ(9, exp.Next());
}

// =============================================================================
// StringToInt Tests (matches string_utils_test.cc)
// =============================================================================

class RustStringUtilsTest : public ::testing::Test {};

TEST_F(RustStringUtilsTest, StringToIntFailsWhenNonNumberInputWithInvalidArgument) {
  std::string input = "imanumber";
  auto parsed = StringToInt<int>(input);
  ASSERT_TRUE(parsed.IsInvalidArgument()) << parsed.ToString();
}

TEST_F(RustStringUtilsTest, StringToIntFailsWhenEmptyStringWithInvalidArgument) {
  std::string input = "";
  auto parsed = StringToInt<int>(input);
  ASSERT_TRUE(parsed.IsInvalidArgument()) << parsed.ToString();
}

TEST_F(RustStringUtilsTest, StringToIntFailsWhenNumberWithSpacesWithInvalidArgument) {
  std::string leading_space = " 1";
  auto parsed = StringToInt<int>(leading_space);
  ASSERT_TRUE(parsed.IsInvalidArgument()) << parsed.ToString();

  std::string trailing_space = "1 ";
  parsed = StringToInt<int>(trailing_space);
  ASSERT_TRUE(parsed.IsInvalidArgument()) << parsed.ToString();

  std::string space_separated = "1 2";
  parsed = StringToInt<int>(space_separated);
  ASSERT_TRUE(parsed.IsInvalidArgument()) << parsed.ToString();
}

TEST_F(RustStringUtilsTest, StringToIntFailsWhenNonIntegerAndIntegerCharsWithInvalidArgument) {
  std::string input = "123hellodarknessmyoldfriend";
  auto parsed = StringToInt<int>(input);
  ASSERT_TRUE(parsed.IsInvalidArgument()) << parsed.ToString();
}

TEST_F(RustStringUtilsTest, StringToIntFailWhenIntegerTooOverflowsTypeWithInvalidArgument) {
  std::string input = "4294967296";
  auto parsed = StringToInt<int8_t>(input);
  ASSERT_TRUE(parsed.IsInvalidArgument()) << parsed.ToString();
}

TEST_F(RustStringUtilsTest, StringToIntSucceedsWithNegativeIntegers) {
  std::string input = "-4294967296";
  auto parsed = StringToInt<int64_t>(input);
  ASSERT_TRUE(parsed.ok()) << parsed.ToString();
}

TEST_F(RustStringUtilsTest, StringToIntSucceedsWithPositiveIntegers) {
  std::string input = "4294967296";
  auto parsed = StringToInt<int64_t>(input);
  ASSERT_TRUE(parsed.ok()) << parsed.ToString();
}

// =============================================================================
// Size Literals Tests (matches size_literals_test.cc)
// =============================================================================

class RustSizeLiteralsTest : public ::testing::Test {};

TEST_F(RustSizeLiteralsTest, BasicTest) {
  // Test compile-time C++ versions
  static_assert(MiB(2) == 2 * 1024 * 1024);
  static_assert(KB(2) == 2000);  // Note: KB uses 1000, not 1024
  static_assert(GB(4) == 4'000'000'000ULL);

  // Test Rust implementations match
  ASSERT_EQ(RustMiB(2), 2 * 1024 * 1024);
  ASSERT_EQ(RustKB(2), 2000);
  ASSERT_EQ(RustGB(4), 4'000'000'000ULL);

  // Verify Rust matches C++
  ASSERT_EQ(MiB(2), RustMiB(2));
  ASSERT_EQ(KB(2), RustKB(2));
  ASSERT_EQ(GB(4), RustGB(4));
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
