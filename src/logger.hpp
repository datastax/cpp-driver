/*
  Copyright 2014 DataStax

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

#ifndef __CASS_LOGGER_HPP_INCLUDED__
#define __CASS_LOGGER_HPP_INCLUDED__

#include "async_queue.hpp"
#include "cassandra.h"
#include "get_time.hpp"
#include "loop_thread.hpp"
#include "mpmc_queue.hpp"
#include "scoped_ptr.hpp"

#include <stdarg.h>
#include <stdio.h>
#include <string>

namespace cass {

class Logger {
public:
  static void set_level(CassLogLevel level);
  static void set_queue_size(size_t queue_size);
  static void set_callback(CassLogCallback cb, void* data);

  static void init();

#ifdef __GNUC__
#define ATTR_FORMAT(type, string, first) __attribute__((format(type, string, first)))
#else
#define ATTR_FORMAT(type, string , first)
#endif

#define LOG_METHOD(name, severity)            \
  ATTR_FORMAT(printf, 1, 2)                   \
  static void name(const char* format, ...) { \
    if (severity <= level_) {                 \
      va_list args;                           \
      va_start(args, format);                 \
      thread_->log(severity, format, args);   \
      va_end(args);                           \
    }                                         \
  }

  LOG_METHOD(critical, CASS_LOG_CRITICAL)
  LOG_METHOD(error, CASS_LOG_ERROR)
  LOG_METHOD(warn, CASS_LOG_WARN)
  LOG_METHOD(info, CASS_LOG_INFO)
  LOG_METHOD(debug, CASS_LOG_DEBUG)
  LOG_METHOD(trace, CASS_LOG_TRACE)

#undef LOG_MESSAGE
#undef ATTR_FORMAT

  // Testing only
  static bool is_queue_empty() { return thread_->is_queue_empty(); }

private:
  class LogThread : public LoopThread {
  public:
    LogThread(CassLogCallback cb, void* data, size_t queue_size)
      : cb_(cb)
      , data_(data)
      , log_queue_(queue_size) {
      log_queue_.init(loop(), this, on_log);
    }

    void log(CassLogLevel severity, const char* format, va_list args);

    bool is_queue_empty() const { return log_queue_.is_empty(); }

  private:
    struct LogMessage {
      uint64_t time;
      CassLogLevel severity;
      std::string message;
    };

    std::string format_message(const char* format, va_list args);

    static void on_log(uv_async_t* async, int status);

    CassLogCallback cb_;
    void* data_;
    AsyncQueue<MPMCQueue<LogMessage*> > log_queue_;
  };

private:
  static void internal_init();

  static CassLogLevel level_;
  static CassLogCallback cb_;
  static void* data_;
  static size_t queue_size_;
  static LogThread* thread_;

  Logger(); // Keep this object from being created
};

} // namespace cass

#endif
