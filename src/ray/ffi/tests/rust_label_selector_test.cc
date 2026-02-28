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

// Rust FFI tests for LabelSelector.
// Matches: src/ray/common/scheduling/tests/label_selector_test.cc

#include "gtest/gtest.h"
#include "ray/ffi/rust_label_selector.h"

#include <algorithm>
#include <map>
#include <set>
#include <string>
#include <vector>

namespace ray {

class RustLabelSelectorTest : public ::testing::Test {};

TEST_F(RustLabelSelectorTest, BasicConstruction) {
  std::map<std::string, std::string> label_selector_dict;
  label_selector_dict["market-type"] = "spot";
  label_selector_dict["region"] = "us-east";

  RustLabelSelector selector(label_selector_dict);
  auto constraints = selector.GetConstraints();

  ASSERT_EQ(constraints.size(), 2);

  for (const auto &constraint : constraints) {
    EXPECT_TRUE(label_selector_dict.count(constraint.GetLabelKey()));
    EXPECT_EQ(constraint.GetOperator(), RustLabelSelectorOperator::LABEL_IN);
    auto values = constraint.GetLabelValues();
    EXPECT_EQ(values.size(), 1);
    EXPECT_EQ(*values.begin(), label_selector_dict[constraint.GetLabelKey()]);
  }
}

TEST_F(RustLabelSelectorTest, InOperatorParsing) {
  RustLabelSelector selector;
  selector.AddConstraint("region", "in(us-west,us-east,me-central)");

  auto constraints = selector.GetConstraints();
  ASSERT_EQ(constraints.size(), 1);
  const auto &constraint = constraints[0];

  EXPECT_EQ(constraint.GetOperator(), RustLabelSelectorOperator::LABEL_IN);
  auto values = constraint.GetLabelValues();
  EXPECT_EQ(values.size(), 3);
  EXPECT_TRUE(values.count("us-west"));
  EXPECT_TRUE(values.count("us-east"));
  EXPECT_TRUE(values.count("me-central"));
}

TEST_F(RustLabelSelectorTest, NotInOperatorParsing) {
  RustLabelSelector selector;
  selector.AddConstraint("tier", "!in(premium,free)");

  auto constraints = selector.GetConstraints();
  ASSERT_EQ(constraints.size(), 1);
  const auto &constraint = constraints[0];

  EXPECT_EQ(constraint.GetOperator(), RustLabelSelectorOperator::LABEL_NOT_IN);
  auto values = constraint.GetLabelValues();
  EXPECT_EQ(values.size(), 2);
  EXPECT_TRUE(values.count("premium"));
  EXPECT_TRUE(values.count("free"));
}

TEST_F(RustLabelSelectorTest, SingleValueNotInParsing) {
  RustLabelSelector selector;
  selector.AddConstraint("env", "!dev");

  auto constraints = selector.GetConstraints();
  ASSERT_EQ(constraints.size(), 1);
  const auto &constraint = constraints[0];

  EXPECT_EQ(constraint.GetOperator(), RustLabelSelectorOperator::LABEL_NOT_IN);
  auto values = constraint.GetLabelValues();
  EXPECT_EQ(values.size(), 1);
  EXPECT_TRUE(values.count("dev"));
}

TEST_F(RustLabelSelectorTest, ToStringMap) {
  // Unpopulated label selector.
  RustLabelSelector empty_selector;
  auto empty_map = empty_selector.ToStringMap();
  EXPECT_TRUE(empty_map.empty());

  // Test label selector with all supported constraints.
  RustLabelSelector selector;

  selector.AddConstraint(
      RustLabelConstraint("region", RustLabelSelectorOperator::LABEL_IN, {"us-west"}));

  selector.AddConstraint(RustLabelConstraint(
      "tier", RustLabelSelectorOperator::LABEL_IN, {"prod", "dev", "staging"}));

  selector.AddConstraint(
      RustLabelConstraint("env", RustLabelSelectorOperator::LABEL_NOT_IN, {"dev"}));

  selector.AddConstraint(
      RustLabelConstraint("team", RustLabelSelectorOperator::LABEL_NOT_IN, {"A100", "B200"}));

  // Validate LabelSelector is correctly converted back to a string map.
  auto string_map = selector.ToStringMap();

  ASSERT_EQ(string_map.size(), 4);
  EXPECT_EQ(string_map.at("region"), "us-west");
  EXPECT_EQ(string_map.at("env"), "!dev");
  EXPECT_EQ(string_map.at("tier"), "in(dev,prod,staging)");
  EXPECT_EQ(string_map.at("team"), "!in(A100,B200)");
}

TEST_F(RustLabelSelectorTest, Deduplication) {
  RustLabelSelector selector;

  selector.AddConstraint("region", "us-west");
  ASSERT_EQ(selector.Size(), 1);

  // Add the exact same constraint again.
  selector.AddConstraint("region", "us-west");
  ASSERT_EQ(selector.Size(), 1);

  // Add a constraint with the same key but different value.
  selector.AddConstraint("region", "us-east");
  ASSERT_EQ(selector.Size(), 2);

  // Add a constraint with a different key but same value.
  selector.AddConstraint("location", "us-east");
  ASSERT_EQ(selector.Size(), 3);

  // Add a constraint with a different key and value.
  selector.AddConstraint("instance", "spot");
  ASSERT_EQ(selector.Size(), 4);

  // Add a duplicate using the RustLabelConstraint object directly.
  RustLabelConstraint duplicate_constraint("instance", RustLabelSelectorOperator::LABEL_IN,
                                           {"spot"});
  selector.AddConstraint(duplicate_constraint);
  ASSERT_EQ(selector.Size(), 4);
}

TEST_F(RustLabelSelectorTest, Equality) {
  RustLabelSelector selector1;
  selector1.AddConstraint("region", "us-west");
  selector1.AddConstraint("tier", "prod");

  RustLabelSelector selector2;
  selector2.AddConstraint("region", "us-west");
  selector2.AddConstraint("tier", "prod");

  EXPECT_EQ(selector1, selector2);

  RustLabelSelector selector3;
  selector3.AddConstraint("region", "us-east");

  EXPECT_NE(selector1, selector3);
}

TEST_F(RustLabelSelectorTest, DebugString) {
  RustLabelSelector selector;
  selector.AddConstraint("region", "us-west");

  std::string debug = selector.DebugString();
  EXPECT_FALSE(debug.empty());
  EXPECT_TRUE(debug.find("region") != std::string::npos);
  EXPECT_TRUE(debug.find("us-west") != std::string::npos);
}

// Test that mimics the ToProto test - validates constraint data is correct
// without requiring actual protobuf types.
TEST_F(RustLabelSelectorTest, ConstraintDataValidation) {
  RustLabelSelector selector;
  selector.AddConstraint("region", "us-west");
  selector.AddConstraint("tier", "in(prod,dev)");
  selector.AddConstraint("env", "!dev");
  selector.AddConstraint("team", "!in(A100,B200)");

  auto constraints = selector.GetConstraints();
  ASSERT_EQ(constraints.size(), 4);

  // Build expected data
  std::map<std::string,
           std::pair<RustLabelSelectorOperator, std::vector<std::string>>>
      expected;
  expected["region"] = {RustLabelSelectorOperator::LABEL_IN, {"us-west"}};
  expected["tier"] = {RustLabelSelectorOperator::LABEL_IN, {"dev", "prod"}};
  expected["env"] = {RustLabelSelectorOperator::LABEL_NOT_IN, {"dev"}};
  expected["team"] = {RustLabelSelectorOperator::LABEL_NOT_IN, {"A100", "B200"}};

  // Verify each constraint
  for (const auto &constraint : constraints) {
    const std::string &key = constraint.GetLabelKey();

    ASSERT_TRUE(expected.count(key)) << "Unexpected key: " << key;
    const auto &[expected_op, expected_values] = expected[key];

    EXPECT_EQ(constraint.GetOperator(), expected_op)
        << "Operator mismatch for key: " << key;

    std::vector<std::string> actual_values(constraint.GetLabelValues().begin(),
                                           constraint.GetLabelValues().end());
    std::sort(actual_values.begin(), actual_values.end());

    EXPECT_EQ(actual_values.size(), expected_values.size())
        << "Value count mismatch for key: " << key;
    EXPECT_EQ(actual_values, expected_values) << "Values mismatch for key: " << key;

    expected.erase(key);
  }

  EXPECT_TRUE(expected.empty()) << "Not all expected constraints were found.";
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
