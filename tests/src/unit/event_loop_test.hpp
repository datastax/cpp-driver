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

#ifndef EVENT_LOOP_TEST_HPP
#define EVENT_LOOP_TEST_HPP

#include "event_loop.hpp"
#include "unit.hpp"

using datastax::internal::core::EventLoop;
using datastax::internal::core::Future;

class EventLoopTest : public Unit {
public:
  EventLoopTest(const datastax::String& thread_name)
      : thread_name_(thread_name) {}

  virtual void SetUp() {
    Unit::SetUp();
    ASSERT_EQ(0, event_loop_.init(thread_name_));
    ASSERT_EQ(0, event_loop_.run());
  }

  virtual void TearDown() {
    Unit::TearDown();
    event_loop_.close_handles();
    event_loop_.join();
  }

  /**
   * Queue a task to be run on the event loop thread
   * (thread-safe).
   *
   * @param task A task to run on the event loop.
   */
  void add_task(Task* task) { event_loop_.add(task); }
  EventLoop* event_loop() { return &event_loop_; }
  uv_loop_t* loop() { return event_loop_.loop(); }

  /**
   * Execute the outage plan task on the event loop thread
   * (thread-safe).
   *
   * @param outage_plan The outage plan to execute.
   * @return Generic future instance.
   */
  Future::Ptr execute_outage_plan(OutagePlan* outage_plan) {
    Future::Ptr future(new Future(Future::FUTURE_TYPE_GENERIC));
    add_task(new ExecuteOutagePlan(outage_plan, future));
    return future;
  }

private:
  EventLoop event_loop_;
  datastax::String thread_name_;
};

#endif // EVENT_LOOP_TEST_HPP
