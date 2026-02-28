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

// Rust FFI tests for Asio Chaos testing utilities.
// Matches: src/ray/common/tests/asio_defer_test.cc

#include "gtest/gtest.h"
#include "src/ray/ffi/rust_asio_chaos.h"

#include <cstdint>
#include <string>

namespace ray {

bool EnsureBelow(const std::string &method_name, int64_t min_val, int64_t max_val) {
  for (int i = 0; i < 1000; ++i) {
    auto delay = asio::testing::GetDelayUs(method_name);
    if (delay > max_val || delay < min_val) {
      return false;
    }
  }
  return true;
}

TEST(RustAsioChaosTest, Basic) {
  ASSERT_TRUE(asio::testing::Init("method1=10:100,method2=20:30"));
  ASSERT_TRUE(EnsureBelow("method1", 10, 100));
  ASSERT_TRUE(EnsureBelow("method2", 20, 30));
  asio::testing::Clear();
}

TEST(RustAsioChaosTest, WithGlobal) {
  ASSERT_TRUE(asio::testing::Init("method1=10:10,method2=20:30,*=100:200"));
  ASSERT_TRUE(EnsureBelow("method1", 10, 10));
  ASSERT_TRUE(EnsureBelow("method2", 20, 30));
  ASSERT_TRUE(EnsureBelow("others", 100, 200));
  asio::testing::Clear();
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
