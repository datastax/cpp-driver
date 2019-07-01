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

#include "atomic.hpp"
#include "event_loop.hpp"
#include "test_utils.hpp"

using namespace datastax::internal;
using namespace datastax::internal::core;

class EventLoopUnitTest : public testing::Test {
public:
  class MarkTaskCompleted : public Task {
  public:
    MarkTaskCompleted(EventLoopUnitTest* event_loop_unit_test)
        : event_loop_unit_test_(event_loop_unit_test) {}
    virtual void run(EventLoop* event_loop) { event_loop_unit_test_->mark_task_completed(); }

  private:
    EventLoopUnitTest* event_loop_unit_test_;
  };

  class MarkIsRunningOn : public Task {
  public:
    MarkIsRunningOn(EventLoopUnitTest* event_loop_unit_test, EventLoop* event_loop)
        : event_loop_unit_test_(event_loop_unit_test)
        , event_loop_(event_loop) {}
    virtual void run(EventLoop* event_loop) {
      event_loop_unit_test_->set_is_running_on(event_loop_->is_running_on());
    }

  private:
    EventLoopUnitTest* event_loop_unit_test_;
    EventLoop* event_loop_;
  };

  class StartIoTime : public Task {
  public:
    virtual void run(EventLoop* event_loop) { event_loop->maybe_start_io_time(); }
  };

  class SetIoTimeElapsed : public Task {
  public:
    SetIoTimeElapsed(EventLoopUnitTest* event_loop_unit_test)
        : event_loop_unit_test_(event_loop_unit_test) {}
    virtual void run(EventLoop* event_loop) {
      event_loop_unit_test_->set_io_time_elapsed(event_loop->io_time_elapsed());
    }

  private:
    EventLoopUnitTest* event_loop_unit_test_;
  };

public:
  EventLoopUnitTest()
      : is_task_completed_(false)
      , is_running_on_(false)
      , io_time_elapsed_(0) {}

  bool is_task_completed() { return is_task_completed_; }
  bool is_running_on() { return is_running_on_; }
  uint64_t io_time_elapsed() { return io_time_elapsed_; }

protected:
  void mark_task_completed() { is_task_completed_ = true; }
  void set_is_running_on(bool is_running_on) { is_running_on_ = is_running_on; }
  void set_io_time_elapsed(uint64_t io_time_elapsed) { io_time_elapsed_ = io_time_elapsed; }

private:
  bool is_task_completed_;
  bool is_running_on_;
  uint64_t io_time_elapsed_;
};

class TestEventLoop : public EventLoop {
public:
  TestEventLoop()
      : is_on_run_completed_(false)
      , is_after_run_completed_(false) {}

  bool is_on_run_completed() { return is_on_run_completed_.load(); }
  bool is_after_run_completed() { return is_after_run_completed_; }

protected:
  void on_run() { is_on_run_completed_.store(true); }
  void on_after_run() { is_after_run_completed_ = true; }

private:
  Atomic<bool> is_on_run_completed_;
  bool is_after_run_completed_;
};

TEST_F(EventLoopUnitTest, ExecuteTask) {
  EventLoop event_loop;
  ASSERT_EQ(0, event_loop.init("EventLoopUnitTest::ExecuteTask"));
  ASSERT_STREQ("EventLoopUnitTest::ExecuteTask", event_loop.name().c_str());
  ASSERT_EQ(0, event_loop.run());

  ASSERT_FALSE(is_task_completed());
  event_loop.add(new MarkTaskCompleted(this));

  event_loop.close_handles();
  event_loop.join();
  ASSERT_TRUE(is_task_completed());
}

TEST_F(EventLoopUnitTest, ThreadRunningOn) {
  EventLoop event_loop;
  ASSERT_EQ(0, event_loop.init("EventLoopUnitTest::ThreadRunningOn"));
  ASSERT_STREQ("EventLoopUnitTest::ThreadRunningOn", event_loop.name().c_str());
  ASSERT_EQ(0, event_loop.run());

  ASSERT_FALSE(is_running_on());
  event_loop.add(new MarkIsRunningOn(this, &event_loop));

  event_loop.close_handles();
  event_loop.join();
  ASSERT_TRUE(is_running_on());
}

TEST_F(EventLoopUnitTest, ThreadNotRunningOn) {
  EventLoop event_loop;
  ASSERT_EQ(0, event_loop.init("EventLoopUnitTest::ThreadNotRunningOn (EventLoop 1)"));
  ASSERT_STREQ("EventLoopUnitTest::ThreadNotRunningOn (EventLoop 1)", event_loop.name().c_str());
  ASSERT_EQ(0, event_loop.run());

  ASSERT_FALSE(is_running_on());

  EventLoop event_loop_2;
  ASSERT_EQ(0, event_loop_2.init("EventLoopUnitTest::ThreadNotRunningOn (EventLoop 2)"));
  ASSERT_STREQ("EventLoopUnitTest::ThreadNotRunningOn (EventLoop 2)", event_loop_2.name().c_str());
  ASSERT_EQ(0, event_loop_2.run());
  event_loop_2.add(new MarkIsRunningOn(this, &event_loop));
  event_loop_2.close_handles();
  event_loop_2.join();

  event_loop.close_handles();
  event_loop.join();
  ASSERT_FALSE(is_running_on());
}

TEST_F(EventLoopUnitTest, BeforeAndAfterRun) {
  TestEventLoop event_loop;

  ASSERT_FALSE(event_loop.is_on_run_completed());
  ASSERT_FALSE(event_loop.is_after_run_completed());
  ASSERT_EQ(0, event_loop.init("EventLoopUnitTest::BeforeAndAfterRun"));
  ASSERT_STREQ("EventLoopUnitTest::BeforeAndAfterRun", event_loop.name().c_str());
  ASSERT_EQ(0, event_loop.run());
  while (!event_loop.is_on_run_completed())
    test::Utils::msleep(1); // Poll to wait for thread to be started
  ASSERT_TRUE(event_loop.is_on_run_completed());
  ASSERT_FALSE(event_loop.is_after_run_completed());

  event_loop.close_handles();
  event_loop.join();
  ASSERT_TRUE(event_loop.is_on_run_completed());
  ASSERT_TRUE(event_loop.is_after_run_completed());
}

TEST_F(EventLoopUnitTest, IoTimeElapsed) {
  // TODO:

  /*
   * io_time_elapsed() measures the time between the start of I/O processing
   * (which is started via maybe_start_io_time()) and the end of I/O
   * processing which is handled by a uv_check_t. A potential way to verify it
   * would involve putting sleep inside of an I/O callback (or another
   * uv_check_t as long as the call ordering is correct) then checking
   * io_time_elapsed() using a uv_prepare_t on the same uv_run() iteration.
   */
}
