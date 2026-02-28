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

// Rust FFI tests for SpilledObject utilities.
// Matches: src/ray/object_manager/tests/spilled_object_test.cc (partial)

#include "gtest/gtest.h"
#include "src/ray/ffi/rust_spilled_object.h"

#include <cstdint>
#include <limits>
#include <string>

namespace ray {

TEST(RustSpilledObjectTest, ParseObjectURL) {
  auto assert_parse_success = [](const std::string &object_url,
                                 const std::string &expected_file_path,
                                 uint64_t expected_object_offset,
                                 uint64_t expected_object_size) {
    auto result = RustParseObjectURL(object_url);
    ASSERT_TRUE(result.success()) << "Failed to parse: " << object_url;
    ASSERT_EQ(expected_file_path, result.file_path());
    ASSERT_EQ(expected_object_offset, result.offset());
    ASSERT_EQ(expected_object_size, result.size());
  };

  auto assert_parse_fail = [](const std::string &object_url) {
    auto result = RustParseObjectURL(object_url);
    ASSERT_FALSE(result.success()) << "Should have failed to parse: " << object_url;
  };

  // Success cases
  assert_parse_success(
      "file://path/to/file?offset=123&size=456", "file://path/to/file", 123, 456);
  assert_parse_success("http://123?offset=123&size=456", "http://123", 123, 456);
  assert_parse_success("file:///C:/Users/file.txt?offset=123&size=456",
                       "file:///C:/Users/file.txt",
                       123,
                       456);
  assert_parse_success("/tmp/file.txt?offset=123&size=456", "/tmp/file.txt", 123, 456);
  assert_parse_success("C:\\file.txt?offset=123&size=456", "C:\\file.txt", 123, 456);
  assert_parse_success(
      "/tmp/ray/session_2021-07-19_09-50-58_115365_119/ray_spillled_objects/"
      "2f81e7cfcc578f4effffffffffffffffffffffff0200000001000000-multi-1?offset=0&size="
      "2199437144",
      "/tmp/ray/session_2021-07-19_09-50-58_115365_119/ray_spillled_objects/"
      "2f81e7cfcc578f4effffffffffffffffffffffff0200000001000000-multi-1",
      0,
      2199437144);
  assert_parse_success(
      "/tmp/123?offset=0&size=9223372036854775807", "/tmp/123", 0, 9223372036854775807);

  // Failure cases
  assert_parse_fail("/tmp/123?offset=-1&size=1");
  assert_parse_fail("/tmp/123?offset=0&size=9223372036854775808");
  assert_parse_fail("file://path/to/file?offset=a&size=456");
  assert_parse_fail("file://path/to/file?offset=0&size=bb");
  assert_parse_fail("file://path/to/file?offset=123");
  assert_parse_fail("file://path/to/file?offset=a&size=456&extra");
}

TEST(RustSpilledObjectTest, ToUINT64) {
  ASSERT_EQ(0, RustBytesToUint64(
                   std::string{'\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00'}));
  ASSERT_EQ(1, RustBytesToUint64(
                   std::string{'\x01', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00'}));
  ASSERT_EQ(std::numeric_limits<uint64_t>::max(),
            RustBytesToUint64(std::string{'\xff', '\xff', '\xff', '\xff', '\xff', '\xff',
                                          '\xff', '\xff'}));
}

TEST(RustSpilledObjectTest, GetNumChunks) {
  ASSERT_EQ(11, RustCalculateNumChunks(11, 1));
  ASSERT_EQ(1, RustCalculateNumChunks(1, 11));
  ASSERT_EQ(0, RustCalculateNumChunks(0, 11));
  ASSERT_EQ(5, RustCalculateNumChunks(9, 2));
  ASSERT_EQ(5, RustCalculateNumChunks(10, 2));
  ASSERT_EQ(6, RustCalculateNumChunks(11, 2));
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
