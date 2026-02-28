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

// Rust FFI tests for RayConfig utilities.
// Matches: src/ray/common/tests/ray_config_test.cc

#include "gtest/gtest.h"
#include "ray/ffi/rust_ray_config.h"

#include <string>
#include <vector>

namespace ray {

class RustRayConfigTest : public ::testing::Test {};

TEST_F(RustRayConfigTest, ConvertValueTrimsVectorElements) {
  const std::string type_string = "std::vector";
  const std::string input = "no_spaces, with spaces ";
  const std::vector<std::string> expected_output{"no_spaces", "with spaces"};
  auto output = RustConvertValue<std::vector<std::string>>(type_string, input);
  ASSERT_EQ(output, expected_output);
}

TEST_F(RustRayConfigTest, ConvertValueBool) {
  ASSERT_TRUE(RustConvertValue<bool>("bool", "true"));
  ASSERT_TRUE(RustConvertValue<bool>("bool", "True"));
  ASSERT_TRUE(RustConvertValue<bool>("bool", "TRUE"));
  ASSERT_TRUE(RustConvertValue<bool>("bool", "1"));
  ASSERT_FALSE(RustConvertValue<bool>("bool", "false"));
  ASSERT_FALSE(RustConvertValue<bool>("bool", "0"));
}

TEST_F(RustRayConfigTest, ConvertValueInt) {
  ASSERT_EQ(RustConvertValue<int64_t>("int64", "42"), 42);
  ASSERT_EQ(RustConvertValue<int64_t>("int64", "-10"), -10);
  ASSERT_EQ(RustConvertValue<int64_t>("int64", "0"), 0);
}

TEST_F(RustRayConfigTest, ConvertValueFloat) {
  ASSERT_DOUBLE_EQ(RustConvertValue<double>("double", "3.14"), 3.14);
  ASSERT_DOUBLE_EQ(RustConvertValue<double>("double", "-2.5"), -2.5);
}

TEST_F(RustRayConfigTest, ConvertValueString) {
  ASSERT_EQ(RustConvertValue<std::string>("string", "hello world"), "hello world");
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
