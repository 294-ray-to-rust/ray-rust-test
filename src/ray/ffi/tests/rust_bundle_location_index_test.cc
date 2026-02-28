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

// Rust FFI tests for BundleLocationIndex.
// Matches: src/ray/common/tests/bundle_location_index_test.cc

#include "gtest/gtest.h"
#include "src/ray/ffi/rust_bundle_location_index.h"

namespace ray {

class RustBundleLocationIndexTest : public ::testing::Test {
 public:
  RustPlacementGroupId pg_1 = RustPlacementGroupId::Of(RustJobId::FromInt(1));
  RustPlacementGroupId pg_2 = RustPlacementGroupId::Of(RustJobId::FromInt(2));
  RustBundleId bundle_0 = std::make_pair(pg_1, 0);
  RustBundleId bundle_1 = std::make_pair(pg_1, 2);
  RustBundleId bundle_2 = std::make_pair(pg_1, 3);

  RustBundleId pg_2_bundle_0 = std::make_pair(pg_2, 0);
  RustBundleId pg_2_bundle_1 = std::make_pair(pg_2, 1);

  RustNodeId node_0 = RustNodeId::FromRandom();
  RustNodeId node_1 = RustNodeId::FromRandom();
  RustNodeId node_2 = RustNodeId::FromRandom();
};

TEST_F(RustBundleLocationIndexTest, BasicTest) {
  RustBundleLocationIndex pg_location_index;

  ASSERT_FALSE(pg_location_index.GetBundleLocations(pg_1));
  ASSERT_FALSE(pg_location_index.GetBundleLocation(bundle_1));

  // test add and get
  auto bundle_locations = std::make_shared<RustBundleLocations>();
  (*bundle_locations)[bundle_0] = std::make_pair(node_0, nullptr);
  (*bundle_locations)[bundle_1] = std::make_pair(node_1, nullptr);
  pg_location_index.AddOrUpdateBundleLocations(bundle_locations);
  auto pg_bundles_location = pg_location_index.GetBundleLocations(pg_1);
  ASSERT_TRUE(pg_bundles_location);
  ASSERT_EQ(*(pg_location_index.GetBundleLocation(bundle_0)), node_0);
  ASSERT_EQ(*(pg_location_index.GetBundleLocation(bundle_1)), node_1);
  ASSERT_FALSE(pg_location_index.GetBundleLocation(bundle_2));

  bundle_locations = std::make_shared<RustBundleLocations>();
  (*bundle_locations)[bundle_2] = std::make_pair(node_2, nullptr);
  pg_location_index.AddOrUpdateBundleLocations(bundle_locations);
  bundle_locations = std::make_shared<RustBundleLocations>();
  (*bundle_locations)[pg_2_bundle_0] = std::make_pair(node_0, nullptr);
  (*bundle_locations)[pg_2_bundle_1] = std::make_pair(node_1, nullptr);

  pg_location_index.AddOrUpdateBundleLocations(bundle_locations);
  ASSERT_EQ(*(pg_location_index.GetBundleLocation(bundle_0)), node_0);
  ASSERT_EQ(*(pg_location_index.GetBundleLocation(bundle_1)), node_1);
  ASSERT_EQ(*(pg_location_index.GetBundleLocation(bundle_2)), node_2);
  ASSERT_EQ(*(pg_location_index.GetBundleLocation(pg_2_bundle_0)), node_0);
  ASSERT_EQ(*(pg_location_index.GetBundleLocation(pg_2_bundle_1)), node_1);
  ASSERT_EQ(pg_location_index.GetBundleCount(pg_1), 3);

  // test erase by node
  pg_location_index.Erase(node_0);
  ASSERT_FALSE(pg_location_index.GetBundleLocation(bundle_0));
  ASSERT_FALSE(pg_location_index.GetBundleLocation(pg_2_bundle_0));
  ASSERT_EQ(*(pg_location_index.GetBundleLocation(bundle_1)), node_1);

  // test erase by placement group
  pg_location_index.Erase(pg_1);
  ASSERT_FALSE(pg_location_index.GetBundleLocations(pg_1));
  ASSERT_FALSE(pg_location_index.GetBundleLocation(bundle_1));
  ASSERT_FALSE(pg_location_index.GetBundleLocation(bundle_2));
  ASSERT_EQ(*(pg_location_index.GetBundleLocation(pg_2_bundle_1)), node_1);

  // test add again
  bundle_locations = std::make_shared<RustBundleLocations>();
  (*bundle_locations)[bundle_0] = std::make_pair(node_0, nullptr);
  (*bundle_locations)[bundle_1] = std::make_pair(node_1, nullptr);
  pg_location_index.AddOrUpdateBundleLocations(bundle_locations);
  ASSERT_EQ(*(pg_location_index.GetBundleLocation(bundle_0)), node_0);
  ASSERT_EQ(*(pg_location_index.GetBundleLocation(bundle_1)), node_1);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
