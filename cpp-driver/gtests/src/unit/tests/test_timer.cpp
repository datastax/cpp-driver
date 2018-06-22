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

#include "timer.hpp"

#include "loop_test.hpp"


class TimerUnitTest : public LoopTest {
public:

  TimerUnitTest()
    : count_(0) { }

  void test_once(uint64_t timeout) {
    cass::Timer timer;

    timer.start(loop(), timeout,
                cass::bind_callback(&TimerUnitTest::on_timer_once, this));

    EXPECT_TRUE(timer.is_running());

    uv_run(loop(), UV_RUN_DEFAULT);

    EXPECT_FALSE(timer.is_running());
    EXPECT_EQ(count_, 1);
  }

  void test_repeat(uint64_t timeout) {
    cass::Timer timer;

    timer.start(loop(), timeout,
                cass::bind_callback(&TimerUnitTest::on_timer_repeat, this));

    EXPECT_TRUE(timer.is_running());

    uv_run(loop(), UV_RUN_DEFAULT);

    EXPECT_FALSE(timer.is_running());
    EXPECT_EQ(count_, 2);
  }

private:
  void on_timer_once(cass::Timer* timer) {
    count_++;
    EXPECT_FALSE(timer->is_running());
  }

  void on_timer_repeat(cass::Timer* timer) {
    EXPECT_FALSE(timer->is_running());
    count_++;
    if (count_ == 1) {
      timer->start(loop(), 1,
                   cass::bind_callback(&TimerUnitTest::on_timer_repeat, this));
    }
  }

  int count_;
};

TEST_F(TimerUnitTest, Once)
{
  test_once(1);
}

TEST_F(TimerUnitTest, Repeat)
{
  test_repeat(1);
}
