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

static bool was_timer_called = false;

void on_timer_once(cass::Timer* timer) {
  was_timer_called = true;
  EXPECT_FALSE(timer->is_running());
}

struct RepeatData {
  uv_loop_t* loop;
  int count;
};

RepeatData repeat_data;

void on_timer_repeat(cass::Timer* timer) {
  EXPECT_FALSE(timer->is_running());
  repeat_data.count++;
  if (repeat_data.count == 1) {
    timer->start(repeat_data.loop, 1,
                 cass::bind_func(on_timer_repeat));
  }
}

TEST(TimerUnitTest, Once)
{
  uv_loop_t* loop;
  uv_loop_t loop_storage__;
  loop = &loop_storage__;
  uv_loop_init(loop);

  cass::Timer timer;

  timer.start(loop, 1,
              cass::bind_func(on_timer_once));

  EXPECT_TRUE(timer.is_running());

  uv_run(loop, UV_RUN_DEFAULT);

  EXPECT_FALSE(timer.is_running());
  EXPECT_TRUE(was_timer_called);

  uv_loop_close(loop);
}

TEST(TimerUnitTest, Repeat)
{
  uv_loop_t* loop;
  uv_loop_t loop_storage__;
  loop = &loop_storage__;
  uv_loop_init(loop);

  cass::Timer timer;

  repeat_data.loop = loop;
  repeat_data.count = 0;

  timer.start(loop, 1,
              cass::bind_func(on_timer_repeat));

  EXPECT_TRUE(timer.is_running());

  uv_run(loop, UV_RUN_DEFAULT);

  EXPECT_FALSE(timer.is_running());
  EXPECT_EQ(repeat_data.count, 2);

  uv_loop_close(loop);
}
