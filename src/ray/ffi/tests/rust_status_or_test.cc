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

// Rust FFI tests for StatusOr type.
// Matches: src/ray/common/tests/status_or_test.cc

#include "gtest/gtest.h"
#include "ray/ffi/rust_status_or.h"

namespace ray {

class RustStatusOrTest : public ::testing::Test {};

TEST_F(RustStatusOrTest, AssignTest) {
  // Assign with status (error).
  {
    RustStatusOrInt status_or_val = RustStatusOrInt::InvalidArgument("msg");
    EXPECT_FALSE(status_or_val.ok());
    EXPECT_EQ(status_or_val.code(), ffi::StatusCode::InvalidArgument);
  }

  // Assign with value.
  {
    int val = 20;
    RustStatusOrInt status_or_val(val);
    EXPECT_TRUE(status_or_val.ok());
    EXPECT_EQ(status_or_val.value(), val);
    EXPECT_EQ(*status_or_val, val);
  }
}

TEST_F(RustStatusOrTest, CopyTest) {
  // Copy with status (error).
  {
    RustStatusOrInt status_or_val = RustStatusOrInt::InvalidArgument("msg");
    RustStatusOrInt copied_status_or = status_or_val;
    EXPECT_FALSE(copied_status_or.ok());
    EXPECT_EQ(copied_status_or.code(), ffi::StatusCode::InvalidArgument);
  }

  // Copy with value.
  {
    int val = 20;
    RustStatusOrInt status_or_val(val);
    RustStatusOrInt copied_status_or = status_or_val;
    EXPECT_TRUE(copied_status_or.ok());
    EXPECT_EQ(copied_status_or.value(), val);
    EXPECT_EQ(*copied_status_or, val);
  }
}

TEST_F(RustStatusOrTest, MoveTest) {
  // Move with status (error).
  {
    RustStatusOrInt status_or_val = RustStatusOrInt::InvalidArgument("msg");
    RustStatusOrInt moved_status_or = std::move(status_or_val);
    EXPECT_FALSE(moved_status_or.ok());
    EXPECT_EQ(moved_status_or.code(), ffi::StatusCode::InvalidArgument);
  }

  // Move with value.
  {
    int val = 20;
    RustStatusOrInt status_or_val(val);
    RustStatusOrInt moved_status_or = std::move(status_or_val);
    EXPECT_TRUE(moved_status_or.ok());
    EXPECT_EQ(moved_status_or.value(), val);
    EXPECT_EQ(*moved_status_or, val);
  }
}

TEST_F(RustStatusOrTest, EqualityTest) {
  // Error vs value - not equal
  {
    RustStatusOrInt val1 = RustStatusOrInt::InvalidArgument("msg");
    RustStatusOrInt val2(20);
    EXPECT_NE(val1, val2);
  }

  // Same value - equal
  {
    RustStatusOrInt val1(20);
    RustStatusOrInt val2(20);
    EXPECT_EQ(val1, val2);
  }

  // Different values - not equal
  {
    RustStatusOrInt val1(40);
    RustStatusOrInt val2(20);
    EXPECT_NE(val1, val2);
  }
}

TEST_F(RustStatusOrTest, ValueOrTest) {
  // OK status - return value
  {
    RustStatusOrInt statusor(10);
    EXPECT_EQ(statusor.value_or(100), 10);
  }
  // Error status - return default
  {
    RustStatusOrInt statusor = RustStatusOrInt::InvalidArgument("msg");
    EXPECT_EQ(statusor.value_or(100), 100);
  }
}

TEST_F(RustStatusOrTest, ValueOrDefaultTest) {
  RustStatusOrInt status_or_val = RustStatusOrInt::InvalidArgument("msg");
  EXPECT_EQ(status_or_val.value_or_default(), 0);
}

TEST_F(RustStatusOrTest, AndThen) {
  auto f = [](const RustStatusOrInt &statusor) -> RustStatusOrInt {
    EXPECT_TRUE(statusor.ok());
    return RustStatusOrInt(statusor.value() + 10);
  };

  // Error status - return error
  {
    RustStatusOrInt s = RustStatusOrInt::InvalidArgument("msg");
    EXPECT_EQ(s.and_then(f).code(), ffi::StatusCode::InvalidArgument);
  }
  // OK status - apply function
  {
    RustStatusOrInt s(10);
    EXPECT_EQ(s.and_then(f).value(), 20);
  }
}

TEST_F(RustStatusOrTest, OrElse) {
  auto f = [](const RustStatusOrInt &statusor) -> RustStatusOrInt {
    EXPECT_FALSE(statusor.ok());
    return RustStatusOrInt(static_cast<int>(statusor.code()));
  };

  // Error status - apply function
  {
    RustStatusOrInt s = RustStatusOrInt::InvalidArgument("msg");
    EXPECT_EQ(s.or_else(f).value(), static_cast<int>(ffi::StatusCode::InvalidArgument));
  }
  // OK status - return value
  {
    RustStatusOrInt s(10);
    EXPECT_EQ(s.or_else(f).value(), 10);
  }
}

TEST_F(RustStatusOrTest, CopyAssignment) {
  // Copy value.
  {
    RustStatusOrInt val(10);
    RustStatusOrInt copy = val;
    EXPECT_EQ(val.value(), 10);
    EXPECT_EQ(copy.value(), 10);
  }

  // Copy error status.
  {
    RustStatusOrInt val = RustStatusOrInt::InvalidArgument("error");
    RustStatusOrInt copy = val;
    EXPECT_EQ(val.code(), ffi::StatusCode::InvalidArgument);
    EXPECT_EQ(copy.code(), ffi::StatusCode::InvalidArgument);
  }
}

TEST_F(RustStatusOrTest, MoveAssignment) {
  // Move value.
  {
    RustStatusOrInt val(10);
    RustStatusOrInt moved = std::move(val);
    EXPECT_EQ(moved.value(), 10);
  }

  // Move status.
  {
    RustStatusOrInt val = RustStatusOrInt::InvalidArgument("error");
    RustStatusOrInt moved = std::move(val);
    EXPECT_EQ(moved.code(), ffi::StatusCode::InvalidArgument);
  }
}

// Test helper functions that return StatusOr
TEST_F(RustStatusOrTest, GetValueOrError) {
  // Get error status
  {
    auto result = GetValueOrError(true);
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.code(), ffi::StatusCode::Invalid);
  }
  // Get value
  {
    auto result = GetValueOrError(false);
    EXPECT_TRUE(result.ok());
    EXPECT_EQ(result.value(), 1);
  }
}

TEST_F(RustStatusOrTest, DivideFunction) {
  // Successful division
  {
    auto result = Divide(10, 2);
    EXPECT_TRUE(result.ok());
    EXPECT_EQ(result.value(), 5);
  }
  // Division by zero
  {
    auto result = Divide(10, 0);
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.code(), ffi::StatusCode::Invalid);
  }
}

TEST_F(RustStatusOrTest, ParseIntFunction) {
  // Successful parse
  {
    auto result = ParseInt("123");
    EXPECT_TRUE(result.ok());
    EXPECT_EQ(result.value(), 123);
  }
  // Failed parse
  {
    auto result = ParseInt("not a number");
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.code(), ffi::StatusCode::Invalid);
  }
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
