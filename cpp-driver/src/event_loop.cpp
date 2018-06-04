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

namespace cass {

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

class NopTimerCallback : public EventLoop::TimerCallback {
public:
  virtual void on_timeout()  { }
};

static NopTimerCallback nop_timer_callback__;

EventLoop::EventLoop()
  : is_loop_initialized_(false)
  , is_joinable_(false)
#ifndef HAVE_TIMERFD
  , timeout_(0)
#endif
  , timer_callback_(&nop_timer_callback__)
  , is_closing_(false)
  , io_time_start_(0)
  , io_time_elapsed_(0) {
  // Set user data for PooledConnection to start the I/O timer.
  loop_.data = this;
}

EventLoop::~EventLoop() {
  if (is_loop_initialized_) {
    uv_loop_close(&loop_);
  }
}

int EventLoop::init(const String& thread_name /*= ""*/) {
#if defined(_MSC_VER) && defined(_DEBUG)
  thread_name_ = thread_name;
#endif
  int rc = 0;
  rc = uv_loop_init(&loop_);
  if (rc != 0) return rc;
  rc = async_.start(loop(), this, on_task);
  if (rc != 0) return rc;
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

void EventLoop::start_timer(uint64_t timeout_us, TimerCallback* callback) {
#ifdef HAVE_TIMERFD
  timer_.start(loop(), timeout_us, this, internal_on_timer);
#else
  timeout_ = uv_hrtime() + timeout_us * 1000;
#endif

  timer_callback_ = callback ? callback : &nop_timer_callback__;
}

void EventLoop::stop_timer() {
#ifdef HAVE_TIMERFD
  timer_.stop();
#else
  timeout_ = 0;
#endif
}

bool EventLoop::is_timer_running() {
#ifdef HAVE_TIMERFD
  return timer_.is_running();
#else
  return timeout_ != 0;
#endif
}

void EventLoop::maybe_start_io_time() {
  if (io_time_start_ == 0) {
    io_time_start_ = uv_hrtime();
  }
}

void EventLoop::on_run() {
#if defined(_MSC_VER) && defined(_DEBUG)
  char temp[64];
  unsigned long thread_id = static_cast<unsigned long>(GetThreadId(uv_thread_self()));
  if (thread_name_.empty()) {
    sprintf(temp, "Event Loop - %lu", thread_id);
  } else {
    sprintf(temp, "%s - %lu", thread_name_.c_str(), thread_id);
  }
  thread_name_ = temp;
  set_thread_name(thread_name_);
#endif
}

EventLoop::TaskQueue::TaskQueue() {
  uv_mutex_init(&lock_);
}

EventLoop::TaskQueue::~TaskQueue() {
  uv_mutex_destroy(&lock_);
}

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

void EventLoop::internal_on_run(void* data) {
  EventLoop* thread = static_cast<EventLoop*>(data);
  thread->handle_run();
}

void EventLoop::handle_run() {
  int result = 0;
  on_run();

  uint64_t now = uv_hrtime();
  do {
    uv_run_mode mode = UV_RUN_ONCE;

#ifndef HAVE_TIMERFD
    if (timeout_ > now) {
      uint64_t delta =  timeout_ - now;
      // Don't busy UV_RUN_NOWAIT if the timeout is greater than a
      // millisecond (within 5%).
      if (delta > 950 * 1000) {
        uint64_t ms = delta / (1000 * 1000); // Convert to milliseconds
        if (ms == 0) {
          ms = 1;
        }
        timer_.start(loop(), ms, this, internal_on_timer);
      } else {
        mode = UV_RUN_NOWAIT; // Spin
      }
    } else {
      timeout_ = 0;
      timer_callback_->on_timeout();
      // The timeout could change in the callback so it needs to be checked
      // again.
      continue;
    }
#endif

    result = uv_run(loop(), mode);
    now = uv_hrtime();

    if (io_time_start_ > 0) {
      io_time_elapsed_ = now - io_time_start_;
      io_time_start_ = 0;
    } else {
      io_time_elapsed_ = 0;
    }
  } while (result != 0);

  on_after_run();
  SslContextFactory::thread_cleanup();
}

#ifdef HAVE_TIMERFD
void EventLoop::internal_on_timer(TimerFd* timer) {
  EventLoop* thread = static_cast<EventLoop*>(timer->data());
  thread->handle_timer();
}
#else
void EventLoop::internal_on_timer(Timer* timer) {
  EventLoop* thread = static_cast<EventLoop*>(timer->data());
  thread->handle_timer();
}
#endif

void EventLoop::handle_timer() {
#ifdef HAVE_TIMERFD
    timer_callback_->on_timeout();
#else
  if (timeout_ != 0 && timeout_ <= uv_hrtime()) {
    timeout_ = 0;
    timer_callback_->on_timeout();
  }
#endif
}

void EventLoop::on_task(Async* async) {
  EventLoop* thread = static_cast<EventLoop*>(async->data());
  thread->handle_task();
}

void EventLoop::handle_task() {
  Task* task;
  while (tasks_.dequeue(task)) {
    task->run(this);
    Memory::deallocate(task);
  }

  if (is_closing_.load() && tasks_.is_empty()) {
    async_.close_handle();
    timer_.close_handle();
#if defined(HAVE_SIGTIMEDWAIT) && !defined(HAVE_NOSIGPIPE)
    uv_prepare_stop(&prepare_);
    uv_close(reinterpret_cast<uv_handle_t*>(&prepare_), NULL);
#endif
    is_closing_.store(false);
  }
}

#if defined(HAVE_SIGTIMEDWAIT) && !defined(HAVE_NOSIGPIPE)
void EventLoop::on_prepare(uv_prepare_t* prepare) {
  consume_blocked_sigpipe();
}
#endif

int RoundRobinEventLoopGroup::init(const String& thread_name /*= ""*/) {
  for (size_t i = 0; i < threads_.size(); ++i) {
    int rc = threads_[i].init(thread_name);
    if (rc != 0) return rc;
  }
  return 0;
}

int RoundRobinEventLoopGroup::run() {
  for (size_t i = 0; i < threads_.size(); ++i) {
    int rc = threads_[i].run();
    if (rc != 0) return rc;
  }
  return 0;
}

void RoundRobinEventLoopGroup::close_handles() {
  for (size_t i = 0; i < threads_.size(); ++i) {
    threads_[i].close_handles();
  }
}

void RoundRobinEventLoopGroup::join() {
  for (size_t i = 0; i < threads_.size(); ++i) {
    threads_[i].join();
  }
}

EventLoop* RoundRobinEventLoopGroup::add(Task* task) {
  EventLoop* event_loop = &threads_[current_.fetch_add(1) % threads_.size()];
  event_loop->add(task);
  return event_loop;
}

} // namespace cass
