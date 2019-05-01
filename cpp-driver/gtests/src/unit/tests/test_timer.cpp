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

#include "loop_test.hpp"

#include "timer.hpp"

using datastax::internal::bind_callback;
using datastax::internal::core::Timer;

class TimerUnitTest : public LoopTest {
public:
  TimerUnitTest()
      : count_(0)
      , repeat_timeout_(0)
      , restart_count_(0) {}

  void test_once(uint64_t timeout) {
    Timer timer;

    timer.start(loop(), timeout, bind_callback(&TimerUnitTest::on_timer_once, this));

    EXPECT_TRUE(timer.is_running());

    uv_run(loop(), UV_RUN_DEFAULT);

    EXPECT_FALSE(timer.is_running());
    EXPECT_EQ(count_, 1);
  }

  void test_repeat(uint64_t timeout) {
    Timer timer;

    repeat_timeout_ = timeout;

    timer.start(loop(), timeout, bind_callback(&TimerUnitTest::on_timer_repeat, this));

    EXPECT_TRUE(timer.is_running());

    uv_run(loop(), UV_RUN_DEFAULT);

    EXPECT_FALSE(timer.is_running());
    EXPECT_EQ(count_, 2);
  }

  void test_stop() {
    Timer timer;

    timer.start(loop(), 1, bind_callback(&TimerUnitTest::on_timer_once, this));

    EXPECT_TRUE(timer.is_running());

    timer.stop();

    EXPECT_FALSE(timer.is_running());

    timer.start(loop(), 1, bind_callback(&TimerUnitTest::on_timer_once, this));

    EXPECT_TRUE(timer.is_running());

    uv_run(loop(), UV_RUN_DEFAULT);

    EXPECT_FALSE(timer.is_running());
    EXPECT_EQ(count_, 1);
  }

  void test_restart() {
    Timer timer;

    restart_timer_.start(loop(), 10, bind_callback(&TimerUnitTest::on_timer_once, this));

    timer.start(loop(), 1, bind_callback(&TimerUnitTest::on_timer_restart, this));

    EXPECT_TRUE(restart_timer_.is_running());
    EXPECT_TRUE(timer.is_running());

    uv_run(loop(), UV_RUN_DEFAULT);

    EXPECT_FALSE(restart_timer_.is_running());
    EXPECT_FALSE(timer.is_running());

    EXPECT_EQ(restart_count_, 10);
    EXPECT_EQ(count_, 0); // Make sure the timer was never triggered
  }

private:
  void on_timer_once(Timer* timer) {
    count_++;
    EXPECT_FALSE(timer->is_running());
  }

  void on_timer_repeat(Timer* timer) {
    EXPECT_FALSE(timer->is_running());
    count_++;
    if (count_ == 1) {
      timer->start(loop(), repeat_timeout_, bind_callback(&TimerUnitTest::on_timer_repeat, this));
    }
  }

  void on_timer_restart(Timer* timer) {
    restart_count_++;
    if (restart_count_ == 10) {
      restart_timer_.stop();
    } else {
      restart_timer_.start(loop(), 10, bind_callback(&TimerUnitTest::on_timer_once, this));

      timer->start(loop(), 1, bind_callback(&TimerUnitTest::on_timer_restart, this));
    }
  }

  int count_;
  uint64_t repeat_timeout_;

  int restart_count_;
  Timer restart_timer_;
};

TEST_F(TimerUnitTest, Once) { test_once(1); }

TEST_F(TimerUnitTest, OnceZero) { test_once(0); }

TEST_F(TimerUnitTest, Repeat) { test_repeat(1); }

TEST_F(TimerUnitTest, RepeatZero) { test_repeat(0); }

TEST_F(TimerUnitTest, Stop) { test_stop(); }

TEST_F(TimerUnitTest, Restart) { test_restart(); }
