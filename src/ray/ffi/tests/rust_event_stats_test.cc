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

// Rust FFI tests for EventTracker.
// Matches: src/ray/common/tests/event_stats_test.cc

#include "gtest/gtest.h"
#include "ray/ffi/rust_event_stats.h"

#include <chrono>
#include <thread>

namespace ray {

TEST(RustEventStatsTest, TestRecordEnd) {
  auto event_tracker = std::make_shared<RustEventTracker>();
  std::shared_ptr<RustStatsHandle> handle = event_tracker->RecordStart("method");

  auto event_stats = event_tracker->get_event_stats("method").value();
  ASSERT_EQ(event_stats.cum_count, 1);
  ASSERT_EQ(event_stats.curr_count, 1);

  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  event_tracker->RecordEnd(std::move(handle));

  event_stats = event_tracker->get_event_stats("method").value();
  ASSERT_EQ(event_stats.cum_count, 1);
  ASSERT_EQ(event_stats.curr_count, 0);
  ASSERT_GE(event_stats.cum_execution_time, 100000000);  // At least 100ms in nanoseconds
}

TEST(RustEventStatsTest, TestRecordExecution) {
  auto event_tracker = std::make_shared<RustEventTracker>();
  std::shared_ptr<RustStatsHandle> handle = event_tracker->RecordStart("method");

  auto event_stats = event_tracker->get_event_stats("method").value();
  ASSERT_EQ(event_stats.cum_count, 1);
  ASSERT_EQ(event_stats.curr_count, 1);

  // Queueing time
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  event_tracker->RecordExecution(
      [&] {
        // Execution time
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        event_stats = event_tracker->get_event_stats("method").value();
        ASSERT_EQ(event_stats.running_count, 1);
      },
      std::move(handle));

  event_stats = event_tracker->get_event_stats("method").value();
  ASSERT_EQ(event_stats.cum_count, 1);
  ASSERT_EQ(event_stats.curr_count, 0);
  ASSERT_EQ(event_stats.running_count, 0);
  ASSERT_GE(event_stats.cum_execution_time, 200000000);  // At least 200ms
  ASSERT_GE(event_stats.cum_queue_time, 100000000);      // At least 100ms
}

TEST(RustEventStatsTest, TestNoStats) {
  auto event_tracker = std::make_shared<RustEventTracker>();
  auto stats = event_tracker->get_event_stats("nonexistent");
  ASSERT_FALSE(stats.has_value());
}

TEST(RustEventStatsTest, TestMultipleEvents) {
  auto event_tracker = std::make_shared<RustEventTracker>();

  auto handle1 = event_tracker->RecordStart("event1");
  auto handle2 = event_tracker->RecordStart("event2");
  auto handle3 = event_tracker->RecordStart("event1");

  auto stats1 = event_tracker->get_event_stats("event1").value();
  auto stats2 = event_tracker->get_event_stats("event2").value();

  ASSERT_EQ(stats1.cum_count, 2);
  ASSERT_EQ(stats1.curr_count, 2);
  ASSERT_EQ(stats2.cum_count, 1);
  ASSERT_EQ(stats2.curr_count, 1);

  event_tracker->RecordEnd(handle1);
  event_tracker->RecordEnd(handle2);
  event_tracker->RecordEnd(handle3);

  stats1 = event_tracker->get_event_stats("event1").value();
  stats2 = event_tracker->get_event_stats("event2").value();

  ASSERT_EQ(stats1.curr_count, 0);
  ASSERT_EQ(stats2.curr_count, 0);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
