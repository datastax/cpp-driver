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

#include "micro_timer.hpp"

#include "loop_test.hpp"

using namespace datastax::internal;
using namespace datastax::internal::core;

class MicroTimerUnitTest : public LoopTest {
public:
  MicroTimerUnitTest()
      : count_(0)
      , repeat_timeout_us_(0) {}

  void test_once(uint64_t timeout_us) {
    MicroTimer timer;

    timer.start(loop(), timeout_us, bind_callback(&MicroTimerUnitTest::on_timer_once, this));

    EXPECT_TRUE(timer.is_running());

    uv_run(loop(), UV_RUN_DEFAULT);

    EXPECT_FALSE(timer.is_running());
    EXPECT_EQ(count_, 1);
  }

  void test_repeat(uint64_t timeout_us) {
    MicroTimer timer;

    repeat_timeout_us_ = timeout_us;

    timer.start(loop(), timeout_us, bind_callback(&MicroTimerUnitTest::on_timer_repeat, this));

    EXPECT_TRUE(timer.is_running());

    uv_run(loop(), UV_RUN_DEFAULT);

    EXPECT_FALSE(timer.is_running());
    EXPECT_EQ(count_, 2);
  }

  void test_stop() {
    MicroTimer timer;

    timer.start(loop(), 1, bind_callback(&MicroTimerUnitTest::on_timer_once, this));

    EXPECT_TRUE(timer.is_running());

    timer.stop();

    EXPECT_FALSE(timer.is_running());

    timer.start(loop(), 1, bind_callback(&MicroTimerUnitTest::on_timer_once, this));

    EXPECT_TRUE(timer.is_running());

    uv_run(loop(), UV_RUN_DEFAULT);

    EXPECT_FALSE(timer.is_running());
    EXPECT_EQ(count_, 1);
  }

private:
  void on_timer_once(MicroTimer* timer) {
    count_++;
    EXPECT_FALSE(timer->is_running());
  }

  void on_timer_repeat(MicroTimer* timer) {
    EXPECT_FALSE(timer->is_running());
    count_++;
    if (count_ == 1) {
      timer->start(loop(), repeat_timeout_us_,
                   bind_callback(&MicroTimerUnitTest::on_timer_repeat, this));
    }
  }

  int count_;
  uint64_t repeat_timeout_us_;
};

TEST_F(MicroTimerUnitTest, Once) { test_once(2000); }

TEST_F(MicroTimerUnitTest, OnceZero) { test_once(0); }

TEST_F(MicroTimerUnitTest, OnceMilliAndMicroSec) { test_once(1200); }

TEST_F(MicroTimerUnitTest, OnceNearThreshold) {
  test_once((1000 * CASS_PERCENT_OF_MILLSECOND_THRESHOLD) / 100);
}

TEST_F(MicroTimerUnitTest, OnceMilliAndNearThreshold) {
  test_once(1000 + (1000 * CASS_PERCENT_OF_MILLSECOND_THRESHOLD) / 100);
}

TEST_F(MicroTimerUnitTest, OnceMicroSec) { test_once(1); }

TEST_F(MicroTimerUnitTest, Repeat) { test_repeat(2000); }

TEST_F(MicroTimerUnitTest, RepeatZero) { test_repeat(0); }

TEST_F(MicroTimerUnitTest, RepeatMilliAndMicroSec) { test_repeat(1200); }

TEST_F(MicroTimerUnitTest, RepeatNearThreshold) {
  test_repeat((1000 * CASS_PERCENT_OF_MILLSECOND_THRESHOLD) / 100);
}

TEST_F(MicroTimerUnitTest, RepeatMilliAndNearThreshold) {
  test_repeat(1000 + (1000 * CASS_PERCENT_OF_MILLSECOND_THRESHOLD) / 100);
}

TEST_F(MicroTimerUnitTest, RepeatMicroSec) { test_repeat(1); }

TEST_F(MicroTimerUnitTest, Stop) { test_stop(); }
