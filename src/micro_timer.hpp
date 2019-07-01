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

#ifndef DATASTAX_INTERNAL_MICRO_TIMER_HPP
#define DATASTAX_INTERNAL_MICRO_TIMER_HPP

#include "allocated.hpp"
#include "callback.hpp"
#include "driver_config.hpp"
#include "macros.hpp"

#ifndef HAVE_TIMERFD
#include "loop_watcher.hpp"
#include "timer.hpp"
#endif

#include <uv.h>

// Only affects busy wait timer version
#define CASS_PERCENT_OF_MILLSECOND_THRESHOLD 95

namespace datastax { namespace internal { namespace core {

/**
 * A timer that supports microsecond precision. It is not intended for general
 * use and should be used judiciously with really only a single `MicroTimer`
 * instance per event loop.
 *
 * On Linux, this class uses the timerfd interface and requires a extra file
 * descriptor and an extra system call to start/stop the timer.
 *
 * On all other platforms, the timer busy waits for the sub-millisecond part of
 * the timeout using a `uv_idle_t`, otherwise, a `uv_timer_t` is used for full
 * millisecond part of a timeout.
 */
class MicroTimer {
public:
  typedef internal::Callback<void, MicroTimer*> Callback;

  MicroTimer();
  ~MicroTimer() { stop(); }

  /**
   * Start the timer.
   *
   * Important: Unlike `Timer` this cannot be restarted by calling this method
   * when a the timer is currently running.
   *
   * @param loop The event loop where the timer should run.
   * @param timeout_us The timeout in microseconds.
   * @param callback The callback that handles the timeout.
   * @return 0 for success (or no change), otherwise an error occurred.
   */
  int start(uv_loop_t* loop, uint64_t timeout_us, const Callback& callback);

  /**
   * Stop the timer and cleanup its handles.
   */
  void stop();

  /**
   * Gets the status of the timer.
   *
   * @return true if the timer is running.
   */
  bool is_running() const;

private:
#ifdef HAVE_TIMERFD
private:
  static void on_timeout(uv_poll_t* poll, int status, int events);
  void handle_timeout();

  static void on_close(uv_handle_t* handle);
#else
  void on_timeout(Timer* timer);
#endif

private:
#ifdef HAVE_TIMERFD
  enum State { CLOSED, STOPPED, STARTED };

  AllocatedT<uv_poll_t>* handle_;
  int fd_;
  State state_;
#else
  uint64_t timeout_ns_; // Nanoseconds
  Timer timer_;
#endif

  Callback callback_;

private:
  DISALLOW_COPY_AND_ASSIGN(MicroTimer);
};

}}} // namespace datastax::internal::core

#endif
