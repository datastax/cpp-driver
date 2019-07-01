/*
  Copyright (c) DataStax, Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

#include <gtest/gtest.h>

#include "stream_manager.hpp"

using datastax::internal::core::StreamManager;

TEST(StreamManagerUnitTest, MaxStreams) { ASSERT_EQ(StreamManager<int>().max_streams(), 32768u); }

TEST(StreamManagerUnitTest, Simple) {
  StreamManager<int> streams;

  for (size_t i = 0; i < streams.max_streams(); ++i) {
    int stream = streams.acquire(i);
    ASSERT_GE(stream, 0);
  }

  // Verify there are no more streams left
  EXPECT_LT(streams.acquire(streams.max_streams()), 0);

  for (size_t i = 0; i < streams.max_streams(); ++i) {
    int item = -1;
    EXPECT_TRUE(streams.get(i, item));
    streams.release(i);
    EXPECT_GE(item, 0);
  }

  for (size_t i = 0; i < streams.max_streams(); ++i) {
    int stream = streams.acquire(i);
    ASSERT_GE(stream, 0);
  }

  // Verify there are no more streams left
  EXPECT_LT(streams.acquire(streams.max_streams()), 0);
}

TEST(StreamManagerUnitTest, Release) {
  StreamManager<int> streams;

  for (size_t i = 0; i < streams.max_streams(); ++i) {
    int stream = streams.acquire(i);
    ASSERT_GE(stream, 0);
  }

  // Verify there are no more streams left
  EXPECT_LT(streams.acquire(streams.max_streams()), 0);

  // Verify that the stream the was previously release is re-acquired
  for (size_t i = 0; i < streams.max_streams(); ++i) {
    streams.release(i);
    int stream = streams.acquire(i);
    ASSERT_EQ(static_cast<size_t>(stream), i);
  }

  // Verify there are no more streams left
  ASSERT_LT(streams.acquire(streams.max_streams()), 0);
}
