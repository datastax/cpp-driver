/*
  Copyright (c) 2014-2015 DataStax

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

#ifdef STAND_ALONE
#   define BOOST_TEST_MODULE cassandra
#endif

#include "timer.hpp"

#include <boost/test/unit_test.hpp>


void on_timer(cass::Timer* timer) {
  bool* was_timer_called = static_cast<bool*>(timer->data());
  *was_timer_called = true;
  BOOST_CHECK(!timer->is_running());
}

BOOST_AUTO_TEST_SUITE(timer)

BOOST_AUTO_TEST_CASE(simple)
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

  timer.start(loop, 1, &was_timer_called, on_timer);

  BOOST_CHECK(timer.is_running());

  uv_run(loop, UV_RUN_DEFAULT);

  BOOST_CHECK(!timer.is_running());
  BOOST_CHECK(was_timer_called);

#if UV_VERSION_MAJOR == 0
  uv_loop_delete(loop);
#else
  uv_loop_close(loop);
#endif

}

BOOST_AUTO_TEST_SUITE_END()

