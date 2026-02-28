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

// Rust FFI tests for ScopedEnvSetter.
// Matches: src/ray/util/tests/scoped_env_setter_test.cc

#include "gtest/gtest.h"
#include "src/ray/ffi/rust_scoped_env_setter.h"

namespace ray {

namespace {

constexpr const char *kEnvKey = "key";
constexpr const char *kEnvVal = "val";

TEST(RustScopedEnvSetter, BasicTest) {
  EXPECT_FALSE(EnvVarExists(kEnvKey));

  {
    RustScopedEnvSetter env_setter{kEnvKey, kEnvVal};
    EXPECT_TRUE(EnvVarExists(kEnvKey));
    EXPECT_EQ(GetEnvVar(kEnvKey), kEnvVal);
  }

  EXPECT_FALSE(EnvVarExists(kEnvKey));
}

}  // namespace

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
