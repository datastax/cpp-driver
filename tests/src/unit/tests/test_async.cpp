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

#include "async.hpp"
#include "callback.hpp"

using datastax::internal::bind_callback;
using datastax::internal::core::Async;

class AsyncUnitTest : public LoopTest {
public:
  AsyncUnitTest()
      : is_callback_called_(false) {}

  bool is_callback_called() { return is_callback_called_; }

protected:
  void start(Async* async) {
    ASSERT_EQ(0, async->start(loop(), bind_callback(&AsyncUnitTest::on_async, this)));
  }

private:
  void on_async(Async* async) {
    is_callback_called_ = true;
    async->close_handle();
  }

private:
  bool is_callback_called_;
};

TEST_F(AsyncUnitTest, Simple) {
  Async async;
  ASSERT_FALSE(async.is_running());

  start(&async);
  ASSERT_FALSE(is_callback_called());
  ASSERT_TRUE(async.is_running());

  async.send();
  ASSERT_FALSE(is_callback_called());
  ASSERT_TRUE(async.is_running());

  uv_run(loop(), UV_RUN_DEFAULT);

  ASSERT_TRUE(is_callback_called());
  ASSERT_FALSE(async.is_running());
}

TEST_F(AsyncUnitTest, NotStarted) {
  Async async;
  ASSERT_FALSE(async.is_running());
  ASSERT_FALSE(is_callback_called());

  async.send();
  ASSERT_FALSE(is_callback_called());

  uv_run(loop(), UV_RUN_DEFAULT);

  ASSERT_FALSE(is_callback_called());
  ASSERT_FALSE(async.is_running());
}
