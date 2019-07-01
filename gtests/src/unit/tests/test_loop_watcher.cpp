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

#include "loop_watcher.hpp"

using namespace datastax::internal;
using namespace datastax::internal::core;

class LoopWatcherUnitTest : public LoopTest {
public:
  LoopWatcherUnitTest()
      : is_check_callback_called_(false)
      , is_prepare_callback_called_(false) {}

  bool is_check_callback_called() { return is_check_callback_called_; }
  bool is_prepare_callback_called() { return is_prepare_callback_called_; }

protected:
  void start(Check* check) {
    ASSERT_EQ(0, check->start(loop(), bind_callback(&LoopWatcherUnitTest::on_check, this)));
  }
  void start(Prepare* prepare) {
    ASSERT_EQ(0, prepare->start(loop(), bind_callback(&LoopWatcherUnitTest::on_prepare, this)));
  }

private:
  void on_check(Check* check) {
    is_check_callback_called_ = true;
    check->close_handle();
  }
  void on_prepare(Prepare* prepare) {
    is_prepare_callback_called_ = true;
    prepare->close_handle();
  }

private:
  bool is_check_callback_called_;
  bool is_prepare_callback_called_;
};

TEST_F(LoopWatcherUnitTest, Check) {
  Check check;
  ASSERT_FALSE(check.is_running());

  start(&check);
  ASSERT_FALSE(is_check_callback_called());
  ASSERT_TRUE(check.is_running());

  uv_run(loop(), UV_RUN_NOWAIT);

  ASSERT_TRUE(is_check_callback_called());
  ASSERT_FALSE(check.is_running());
}

TEST_F(LoopWatcherUnitTest, CheckNotStarted) {
  Prepare check;
  ASSERT_FALSE(check.is_running());
  ASSERT_FALSE(is_check_callback_called());

  uv_run(loop(), UV_RUN_NOWAIT);

  ASSERT_FALSE(is_check_callback_called());
  ASSERT_FALSE(check.is_running());
}

TEST_F(LoopWatcherUnitTest, Prepare) {
  Prepare prepare;
  ASSERT_FALSE(prepare.is_running());

  start(&prepare);
  ASSERT_FALSE(is_prepare_callback_called());
  ASSERT_TRUE(prepare.is_running());

  uv_run(loop(), UV_RUN_NOWAIT);

  ASSERT_TRUE(is_prepare_callback_called());
  ASSERT_FALSE(prepare.is_running());
}

TEST_F(LoopWatcherUnitTest, PrepareNotStarted) {
  Prepare prepare;
  ASSERT_FALSE(prepare.is_running());
  ASSERT_FALSE(is_prepare_callback_called());

  uv_run(loop(), UV_RUN_NOWAIT);

  ASSERT_FALSE(is_prepare_callback_called());
  ASSERT_FALSE(prepare.is_running());
}
