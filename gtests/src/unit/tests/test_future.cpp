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

#include "future.hpp"
#include "test_utils.hpp"

#include <uv.h>

#define DELAY_MS 500 // 500 milliseconds

void on_timeout_set_future(uv_timer_t* handle) {
  cass::Future* future = static_cast<cass::Future*>(handle->data);
  future->set();
  uv_close(reinterpret_cast<uv_handle_t*>(handle), NULL);
}

void on_future_callback(CassFuture* future, void* data) {
  bool* is_future_callback_called = static_cast<bool*>(data);
  *is_future_callback_called = true;
}

void start_timer(void* arg) {
  cass::Future* future = static_cast<cass::Future*>(arg);
  test::Utils::msleep(DELAY_MS);
  future->set();
}

TEST(FutureUnitTest, Types) {
  cass::Future generic(cass::Future::FUTURE_TYPE_GENERIC);
  cass::Future session(cass::Future::FUTURE_TYPE_SESSION);
  cass::Future response(cass::Future::FUTURE_TYPE_RESPONSE);

  ASSERT_EQ(cass::Future::FUTURE_TYPE_GENERIC, generic.type());
  ASSERT_EQ(cass::Future::FUTURE_TYPE_SESSION, session.type());
  ASSERT_EQ(cass::Future::FUTURE_TYPE_RESPONSE, response.type());
}

TEST(FutureUnitTest, Ready) {
  cass::Future future(cass::Future::FUTURE_TYPE_GENERIC);
  ASSERT_FALSE(future.ready());
  future.set();
  ASSERT_TRUE(future.ready());
}

TEST(FutureUnitTest, Wait) {
  cass::Future future(cass::Future::FUTURE_TYPE_GENERIC);
  uv_thread_t thread;
  ASSERT_EQ(0, uv_thread_create(&thread, start_timer, &future));

  future.wait();
  ASSERT_TRUE(future.ready());

  ASSERT_EQ(0, uv_thread_join(&thread));
}

TEST(FutureUnitTest, WaitFor) {
  cass::Future future(cass::Future::FUTURE_TYPE_GENERIC);
  uv_thread_t thread;
  ASSERT_EQ(0, uv_thread_create(&thread, start_timer, &future));

  uint64_t start = uv_hrtime();
  ASSERT_TRUE(future.wait_for(1e+9)); // 1 second
  ASSERT_TRUE(future.ready());
  uint64_t elasped = uv_hrtime() - start;
  EXPECT_NEAR(static_cast<double>(5e+8), // Convert to nanoseconds
              static_cast<double>(elasped),
              static_cast<double>(1e+8)); // 10 millisecond error

  ASSERT_EQ(0, uv_thread_join(&thread));
}

TEST(FutureUnitTest, Error) {
  cass::Future future(cass::Future::FUTURE_TYPE_GENERIC);
  future.set_error(CASS_ERROR_LIB_BAD_PARAMS, "FutureUnitTest error message");
  ASSERT_TRUE(future.ready());
  ASSERT_TRUE(future.error());
  ASSERT_EQ(CASS_ERROR_LIB_BAD_PARAMS, future.error()->code);
  ASSERT_STREQ("FutureUnitTest error message", future.error()->message.c_str());
}

TEST(FutureUnitTest, Callback) {
  bool is_future_callback_called = false;
  cass::Future future(cass::Future::FUTURE_TYPE_GENERIC);
  ASSERT_TRUE(future.set_callback(&on_future_callback, &is_future_callback_called));

  ASSERT_FALSE(is_future_callback_called);
  future.set();
  ASSERT_TRUE(is_future_callback_called);
  ASSERT_TRUE(future.ready());
}

TEST(FutureUnitTest, CallbackAlreadyAssigned) {
  cass::Future future(cass::Future::FUTURE_TYPE_GENERIC);
  ASSERT_TRUE(future.set_callback(&on_future_callback, NULL));
  ASSERT_FALSE(future.set_callback(&on_future_callback, NULL));
}

TEST(FutureUnitTest, CallbackAfterFutureIsSet) {
  bool is_future_callback_called = false;
  cass::Future future(cass::Future::FUTURE_TYPE_GENERIC);

  ASSERT_FALSE(is_future_callback_called);
  future.set();
  ASSERT_TRUE(future.ready());
  ASSERT_FALSE(is_future_callback_called);

  ASSERT_TRUE(future.set_callback(&on_future_callback, &is_future_callback_called));
  ASSERT_TRUE(is_future_callback_called);
}
