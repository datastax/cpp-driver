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

#ifndef __CASS_THREAD_HPP_INCLUDED__
#define __CASS_THREAD_HPP_INCLUDED__

#include "async.hpp"
#include "atomic.hpp"
#include "cassconfig.hpp"
#include "logger.hpp"
#include "deque.hpp"
#include "macros.hpp"
#include "scoped_lock.hpp"
#include "utils.hpp"

#include <assert.h>
#include <uv.h>

namespace cass {

class EventLoop;

/**
 * A task executed on an event loop thread.
 */
class Task {
public:
  virtual ~Task() { }
  virtual void run(EventLoop* event_loop) = 0;
};

/**
 * An event loop thread. Use tasks to run logic on an event loop.
 */
class EventLoop {
public:
  EventLoop();

  virtual ~EventLoop();

  uv_loop_t* loop() { return &loop_; }

  /**
   * Initialize the event loop. This creates/initializes libuv objects that can
   * potentially fail.
   *
   * @param thread_name (WINDOWS DEBUG ONLY) Names thread for debugger (optional)
   * @return Returns 0 if successful, otherwise an error occurred.
   */
  int init(const String& thread_name = "");

  /**
   * Start the event loop thread.
   *
   * @return Returns 0 if successful, otherwise an error occurred.
   */
  int run();

  /**
   * Closes the libuv handles (thread-safe).
   */
  void close_handles();

  /**
   * Waits for the event loop thread to exit (thread-safe).
   */
  void join();

  /**
   * Queue a task to be run on the event loop thread (thread-safe).
   *
   * @param task A task to run on the event loop.
   */
  void add(Task* task);

protected:
  /**
   * A callback that's run before the event loop is run.
   */
  virtual void on_run();

  /**
   * A callback that's run after the event loop exits.
   */
  virtual void on_after_run() { }

private:
  class TaskQueue {
  public:
    TaskQueue();
    ~TaskQueue();

    bool enqueue(Task* task);
    bool dequeue(Task*& task);
    bool is_empty();

  private:
    uv_mutex_t lock_;
    Deque<Task*> queue_;
  };

private:
  static void internal_on_run(void* data);
  void handle_run();

  static void on_task(Async* async);
  void handle_task();

  uv_loop_t loop_;
  bool is_loop_initialized_;

#if defined(HAVE_SIGTIMEDWAIT) && !defined(HAVE_NOSIGPIPE)
  static void on_prepare(uv_prepare_t *prepare);

  uv_prepare_t prepare_;
#endif

  uv_thread_t thread_;
  bool is_joinable_;
  Async async_;
  TaskQueue tasks_;
  Atomic<bool> is_closing_;

#if defined(_MSC_VER) && defined(_DEBUG)
  String thread_name_;
#endif
};

/**
 * A generic group of event loop threads.
 */
class EventLoopGroup {
public:
  virtual ~EventLoopGroup() { }

  /**
   * Queue a task on any available event loop thread.
   * @param task The task to be run on an event loop.
   * @return The event loop that will run the task.
   */
  virtual EventLoop* add(Task* task) = 0;

  /**
   * Get a specific event loop by index.
   *
   * @param index The index of an event loop that must be less than the number of
   * event loops.
   * @return The event loop at index.
   */
  virtual EventLoop* get(size_t index) = 0;


  /**
   * Get the number of event loops in this group.
   *
   * @return The number of event loops.
   */
  virtual size_t size() const = 0;
};

/**
 * A groups of event loops where tasks are assigned to a specific event loop
 * using round-robin.
 */
class RoundRobinEventLoopGroup : public EventLoopGroup {
public:
  RoundRobinEventLoopGroup(size_t num_threads)
    : current_(0)
    , threads_(num_threads) { }

  int init(const String& thread_name = "");
  int run();
  void close_handles();
  void join();

  virtual EventLoop* add(Task* task);
  virtual EventLoop* get(size_t index) { return &threads_[index]; }
  virtual size_t size() const { return threads_.size(); }

private:
  Atomic<size_t> current_;
  DynamicArray<EventLoop> threads_;
};

} // namespace cass

#endif
