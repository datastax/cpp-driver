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

#include "event_loop.hpp"
#include "ssl.hpp"

#if !defined(_WIN32)
#include <signal.h>
#endif

using namespace datastax;
using namespace datastax::internal::core;

#if defined(HAVE_SIGTIMEDWAIT) && !defined(HAVE_NOSIGPIPE)
static int block_sigpipe() {
  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGPIPE);
  return pthread_sigmask(SIG_BLOCK, &set, NULL);
}

static void consume_blocked_sigpipe() {
  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGPIPE);
  struct timespec ts = { 0, 0 };
  int num = sigtimedwait(&set, NULL, &ts);
  if (num > 0) {
    LOG_WARN("Caught and ignored SIGPIPE on loop thread");
  }
}
#endif

EventLoop::EventLoop()
    : is_loop_initialized_(false)
    , is_joinable_(false)
    , is_closing_(false)
    , io_time_start_(0)
    , io_time_elapsed_(0) {
  // Set user data for PooledConnection to start the I/O elapsed time.
  loop_.data = this;
}

EventLoop::~EventLoop() {
  if (is_loop_initialized_) {
    int rc = uv_loop_close(loop());
    if (rc != 0) {
      uv_run(loop(), UV_RUN_DEFAULT);
      rc = uv_loop_close(loop());
      if (rc != 0) {
        uv_print_all_handles(loop(), stderr);
        assert(false && "Event loop still has pending handles");
      }
    }
  }
}

int EventLoop::init(const String& thread_name /*= ""*/) {
  name_ = thread_name;
  int rc = 0;
  rc = uv_loop_init(&loop_);
  if (rc != 0) return rc;
  rc = async_.start(loop(), bind_callback(&EventLoop::on_task, this));
  if (rc != 0) return rc;
  rc = check_.start(loop(), bind_callback(&EventLoop::on_check, this));
  is_loop_initialized_ = true;

#if defined(HAVE_SIGTIMEDWAIT) && !defined(HAVE_NOSIGPIPE)
  rc = block_sigpipe();
  if (rc != 0) return rc;
  rc = uv_prepare_init(loop(), &prepare_);
  if (rc != 0) return rc;
  rc = uv_prepare_start(&prepare_, on_prepare);
  if (rc != 0) return rc;
#endif
  return rc;
}

int EventLoop::run() {
  int rc = uv_thread_create(&thread_, internal_on_run, this);
  if (rc == 0) is_joinable_ = true;
  return rc;
}

void EventLoop::close_handles() {
  is_closing_.store(true);
  async_.send();
}

void EventLoop::join() {
  if (is_joinable_) {
    is_joinable_ = false;
    int rc = uv_thread_join(&thread_);
    UNUSED_(rc);
    assert(rc == 0);
  }
}

void EventLoop::add(Task* task) {
  tasks_.enqueue(task);
  async_.send();
}

void EventLoop::maybe_start_io_time() {
  if (io_time_start_ == 0) {
    io_time_start_ = uv_hrtime();
  }
}

bool EventLoop::is_running_on() const { return uv_thread_self() == thread_; }

void EventLoop::on_run() {
  if (name_.empty()) name_ = "Event Loop";
#if defined(_MSC_VER)
  char temp[64];
  sprintf(temp, "%s - %lu", name_.c_str(),
          static_cast<unsigned long>(GetThreadId(uv_thread_self())));
  name_ = temp;
#endif
  set_thread_name(name_);
}

EventLoop::TaskQueue::TaskQueue() { uv_mutex_init(&lock_); }

EventLoop::TaskQueue::~TaskQueue() { uv_mutex_destroy(&lock_); }

bool EventLoop::TaskQueue::enqueue(Task* task) {
  ScopedMutex l(&lock_);
  queue_.push_back(task);
  return true;
}

bool EventLoop::TaskQueue::dequeue(Task*& task) {
  ScopedMutex l(&lock_);
  if (queue_.empty()) {
    return false;
  }
  task = queue_.front();
  queue_.pop_front();
  return true;
}

bool EventLoop::TaskQueue::is_empty() {
  ScopedMutex l(&lock_);
  return queue_.empty();
}

void EventLoop::internal_on_run(void* arg) {
  EventLoop* thread = static_cast<EventLoop*>(arg);
  thread->handle_run();
}

void EventLoop::handle_run() {
  on_run();
  uv_run(loop(), UV_RUN_DEFAULT);
  on_after_run();
  SslContextFactory::thread_cleanup();
}

void EventLoop::on_check(Check* check) {
  uint64_t now = uv_hrtime();
  if (io_time_start_ > 0) {
    io_time_elapsed_ = now - io_time_start_;
    io_time_start_ = 0;
  } else {
    io_time_elapsed_ = 0;
  }
}

void EventLoop::on_task(Async* async) {
  Task* task = NULL;
  while (tasks_.dequeue(task)) {
    if (task) {
      task->run(this);
      delete task;
    }
  }

  if (is_closing_.load() && tasks_.is_empty()) {
    async_.close_handle();
    check_.close_handle();
#if defined(HAVE_SIGTIMEDWAIT) && !defined(HAVE_NOSIGPIPE)
    uv_prepare_stop(&prepare_);
    uv_close(reinterpret_cast<uv_handle_t*>(&prepare_), NULL);
#endif
    is_closing_.store(false);
  }
}

#if defined(HAVE_SIGTIMEDWAIT) && !defined(HAVE_NOSIGPIPE)
void EventLoop::on_prepare(uv_prepare_t* prepare) { consume_blocked_sigpipe(); }
#endif

int RoundRobinEventLoopGroup::init(const String& thread_name /*= ""*/) {
  for (size_t i = 0; i < num_threads_; ++i) {
    int rc = threads_[i].init(thread_name);
    if (rc != 0) return rc;
  }
  return 0;
}

int RoundRobinEventLoopGroup::run() {
  for (size_t i = 0; i < num_threads_; ++i) {
    int rc = threads_[i].run();
    if (rc != 0) return rc;
  }
  return 0;
}

void RoundRobinEventLoopGroup::close_handles() {
  for (size_t i = 0; i < num_threads_; ++i) {
    threads_[i].close_handles();
  }
}

void RoundRobinEventLoopGroup::join() {
  for (size_t i = 0; i < num_threads_; ++i) {
    threads_[i].join();
  }
}

EventLoop* RoundRobinEventLoopGroup::add(Task* task) {
  EventLoop* event_loop = &threads_[current_.fetch_add(1) % num_threads_];
  event_loop->add(task);
  return event_loop;
}
