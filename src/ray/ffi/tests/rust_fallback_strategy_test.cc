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

// Rust FFI tests for FallbackStrategy.
// Matches: src/ray/common/scheduling/tests/fallback_strategy_test.cc

#include "gtest/gtest.h"
#include "ray/ffi/rust_fallback_strategy.h"

#include <map>
#include <string>
#include <unordered_set>

namespace ray {

class RustFallbackStrategyTest : public ::testing::Test {};

TEST_F(RustFallbackStrategyTest, OptionsConstructionAndEquality) {
  std::map<std::string, std::string> selector_a_map{{"region", "us-east-1"}};
  std::map<std::string, std::string> selector_b_map{{"region", "us-east-1"}};
  std::map<std::string, std::string> selector_c_map{{"region", "us-west-2"}};

  RustFallbackOption options_a(selector_a_map);
  RustFallbackOption options_b(selector_b_map);
  RustFallbackOption options_c(selector_c_map);

  // Test FallbackOption equality
  EXPECT_EQ(options_a, options_b);
  EXPECT_NE(options_a, options_c);

  // Test copy construction
  RustFallbackOption options_from_copy(options_a);
  EXPECT_EQ(options_a, options_from_copy);
}

TEST_F(RustFallbackStrategyTest, OptionsGetSelectorMap) {
  std::map<std::string, std::string> selector_map{{"accelerator-type", "A100"}};
  RustFallbackOption options(selector_map);

  auto retrieved_map = options.GetSelectorMap();
  ASSERT_EQ(retrieved_map.size(), 1);
  EXPECT_EQ(retrieved_map.at("accelerator-type"), "A100");
}

TEST_F(RustFallbackStrategyTest, OptionsHashing) {
  std::map<std::string, std::string> selector_a_map{{"key1", "val1"}};
  std::map<std::string, std::string> selector_b_map{{"key1", "val1"}};
  std::map<std::string, std::string> selector_c_map{{"key2", "val2"}};

  RustFallbackOption options_a(selector_a_map);
  RustFallbackOption options_b(selector_b_map);
  RustFallbackOption options_c(selector_c_map);

  // Using std::hash specialization
  std::hash<RustFallbackOption> hasher;
  EXPECT_EQ(hasher(options_a), hasher(options_b));
  EXPECT_NE(hasher(options_a), hasher(options_c));

  // Can be used in unordered_set
  std::unordered_set<RustFallbackOption> option_set;
  option_set.insert(options_a);
  option_set.insert(options_b);  // Duplicate, should not increase size
  EXPECT_EQ(option_set.size(), 1);
  option_set.insert(options_c);
  EXPECT_EQ(option_set.size(), 2);
}

TEST_F(RustFallbackStrategyTest, ParseAndSerializeStrategy) {
  std::map<std::string, std::string> selector1{{"region", "us-east-1"},
                                                {"market-type", "spot"}};
  std::map<std::string, std::string> selector2{{"cpu-family", "intel"}};

  RustFallbackStrategy strategy;
  strategy.AddOption(selector1);
  strategy.AddOption(selector2);

  ASSERT_EQ(strategy.Size(), 2);

  // Serialize
  auto serialized = strategy.Serialize();
  ASSERT_EQ(serialized.size(), 2);

  // Parse back
  auto parsed = RustFallbackStrategy::Parse(serialized);

  // Validate equality
  EXPECT_EQ(strategy, parsed);
  ASSERT_EQ(parsed.Size(), 2);

  // Validate option contents
  auto map1 = parsed.GetOption(0).GetSelectorMap();
  EXPECT_EQ(map1.at("region"), "us-east-1");
  EXPECT_EQ(map1.at("market-type"), "spot");

  auto map2 = parsed.GetOption(1).GetSelectorMap();
  EXPECT_EQ(map2.at("cpu-family"), "intel");
}

TEST_F(RustFallbackStrategyTest, EmptyFallbackStrategy) {
  RustFallbackStrategy strategy;

  EXPECT_TRUE(strategy.Empty());
  EXPECT_EQ(strategy.Size(), 0);

  auto serialized = strategy.Serialize();
  EXPECT_TRUE(serialized.empty());

  auto parsed = RustFallbackStrategy::Parse(serialized);
  EXPECT_TRUE(parsed.Empty());
  EXPECT_EQ(parsed.Size(), 0);
}

TEST_F(RustFallbackStrategyTest, AddOptionFromFallbackOption) {
  std::map<std::string, std::string> selector_map{{"zone", "us-west-2a"}};
  RustFallbackOption option(selector_map);

  RustFallbackStrategy strategy;
  strategy.AddOption(option);

  EXPECT_EQ(strategy.Size(), 1);

  auto retrieved = strategy.GetOption(0);
  auto map = retrieved.GetSelectorMap();
  EXPECT_EQ(map.at("zone"), "us-west-2a");
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
