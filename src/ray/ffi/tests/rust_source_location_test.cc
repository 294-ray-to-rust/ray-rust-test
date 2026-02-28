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

// Rust FFI tests for SourceLocation.
// Matches: src/ray/common/tests/source_location_test.cc

#include "gtest/gtest.h"
#include "ray/ffi/rust_source_location.h"

#include <sstream>

namespace ray {

TEST(RustSourceLocationTest, DefaultLocation) {
  // Default source location should be empty/invalid.
  RustSourceLocation loc;
  EXPECT_FALSE(loc.IsValid());
  EXPECT_EQ(loc.ToString(), "");
}

TEST(RustSourceLocationTest, StringifyTest) {
  // Default source location.
  {
    std::stringstream ss{};
    ss << RustSourceLocation();
    EXPECT_EQ(ss.str(), "");
  }

  // Initialized source location.
  {
    auto loc = RUST_LOC();
    std::stringstream ss{};
    ss << loc;
    // Should contain filename and line number
    EXPECT_NE(ss.str().find("rust_source_location_test.cc"), std::string::npos);
    EXPECT_NE(ss.str().find(":"), std::string::npos);
  }
}

TEST(RustSourceLocationTest, ValidLocation) {
  RustSourceLocation loc("test_file.cc", 42);
  EXPECT_TRUE(loc.IsValid());
  EXPECT_EQ(loc.filename(), "test_file.cc");
  EXPECT_EQ(loc.line_no(), 42);
  EXPECT_EQ(loc.ToString(), "test_file.cc:42");
}

TEST(RustSourceLocationTest, InvalidLocations) {
  // Empty filename
  {
    RustSourceLocation loc("", 10);
    EXPECT_FALSE(loc.IsValid());
  }

  // Zero line number
  {
    RustSourceLocation loc("file.cc", 0);
    EXPECT_FALSE(loc.IsValid());
  }

  // Negative line number
  {
    RustSourceLocation loc("file.cc", -1);
    EXPECT_FALSE(loc.IsValid());
  }
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
