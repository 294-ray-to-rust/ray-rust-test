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

// Rust FFI tests for gRPC utilities.
// Matches: src/ray/common/tests/grpc_util_test.cc

#include "gtest/gtest.h"
#include "src/ray/ffi/rust_grpc_util.h"

namespace ray {

class RustGrpcUtilTest : public ::testing::Test {};

TEST_F(RustGrpcUtilTest, TestMapEqualMapSizeNotEqual) {
  RustDoubleMap map1;
  RustDoubleMap map2;
  map1["key1"] = 1.0;
  ASSERT_FALSE(MapEqual(map1, map2));
}

TEST_F(RustGrpcUtilTest, TestMapEqualMissingKey) {
  RustDoubleMap map1;
  RustDoubleMap map2;
  map1["key1"] = 1.0;
  map2["key2"] = 1.0;
  ASSERT_FALSE(MapEqual(map1, map2));
}

TEST_F(RustGrpcUtilTest, TestMapEqualSimpleTypeValueNotEqual) {
  RustDoubleMap map1;
  RustDoubleMap map2;
  map1["key1"] = 1.0;
  map2["key1"] = 2.0;
  ASSERT_FALSE(MapEqual(map1, map2));
}

TEST_F(RustGrpcUtilTest, TestMapEqualSimpleTypeEqual) {
  RustDoubleMap map1;
  RustDoubleMap map2;
  map1["key1"] = 1.0;
  map2["key1"] = 1.0;
  ASSERT_TRUE(MapEqual(map1, map2));
}

TEST_F(RustGrpcUtilTest, TestMapEqualProtoMessageTypeNotEqual) {
  RustLabelInMap map1;
  RustLabelInMap map2;
  RustLabelIn label_in_1;
  RustLabelIn label_in_2;
  label_in_1.AddValue("value1");
  label_in_2.AddValue("value2");
  map1["key1"] = label_in_1;
  map2["key1"] = label_in_2;
  ASSERT_FALSE(MapEqual(map1, map2));
}

TEST_F(RustGrpcUtilTest, TestMapEqualProtoMessageTypeEqual) {
  RustLabelInMap map1;
  RustLabelInMap map2;
  RustLabelIn label_in;
  label_in.AddValue("value1");
  map1["key1"] = label_in;
  map2["key1"] = label_in;
  ASSERT_TRUE(MapEqual(map1, map2));
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
