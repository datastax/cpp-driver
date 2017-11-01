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

#include "timer.hpp"

void on_timer_once(cass::Timer* timer) {
  bool* was_timer_called = static_cast<bool*>(timer->data());
  *was_timer_called = true;
  EXPECT_FALSE(timer->is_running());
}

struct RepeatData {
  uv_loop_t* loop;
  int count;
};

void on_timer_repeat(cass::Timer* timer) {
  RepeatData* data = static_cast<RepeatData*>(timer->data());
  EXPECT_FALSE(timer->is_running());
  data->count++;
  if (data->count == 1) {
    timer->start(data->loop, 1, data, on_timer_repeat);
  }
}

TEST(TimerUnitTest, Once)
{
  uv_loop_t* loop;

#if UV_VERSION_MAJOR == 0
  loop = uv_loop_new();
#else
  uv_loop_t loop_storage__;
  loop = &loop_storage__;
  uv_loop_init(loop);
#endif

  cass::Timer timer;

  bool was_timer_called = false;

  timer.start(loop, 1, &was_timer_called, on_timer_once);

  EXPECT_TRUE(timer.is_running());

  uv_run(loop, UV_RUN_DEFAULT);

  EXPECT_FALSE(timer.is_running());
  EXPECT_TRUE(was_timer_called);

#if UV_VERSION_MAJOR == 0
  uv_loop_delete(loop);
#else
  uv_loop_close(loop);
#endif

}

TEST(TimerUnitTest, Repeat)
{
  uv_loop_t* loop;

#if UV_VERSION_MAJOR == 0
  loop = uv_loop_new();
#else
  uv_loop_t loop_storage__;
  loop = &loop_storage__;
  uv_loop_init(loop);
#endif

  cass::Timer timer;

  RepeatData data;
  data.loop = loop;
  data.count = 0;

  timer.start(loop, 1, &data, on_timer_repeat);

  EXPECT_TRUE(timer.is_running());

  uv_run(loop, UV_RUN_DEFAULT);

  EXPECT_FALSE(timer.is_running());
  EXPECT_EQ(data.count, 2);

#if UV_VERSION_MAJOR == 0
  uv_loop_delete(loop);
#else
  uv_loop_close(loop);
#endif

}
