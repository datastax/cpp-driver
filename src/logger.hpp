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

#include <stdarg.h>

#include <string>

#include "event_thread.hpp"
#include "cassandra.h"
#include "async_queue.hpp"
#include "mpmc_queue.hpp"
#include "config.hpp"

namespace cass {

class Logger : public LoopThread {
public:
  Logger(const Config& config)
      : data_(config.log_data())
      , cb_(config.log_callback())
      , log_level_(config.log_level())
      , log_queue_(config.queue_size_log())
      , is_closing_(false) {}

  int init() { return log_queue_.init(loop(), this, on_log); }

  void close_async() {
    is_closing_ = true;
    while (!log_queue_.enqueue(nullptr)) {
      // Keep trying
    }
  }

#define LOG_MESSAGE(severity)    \
  if (severity <= log_level_) {  \
    va_list args;                \
    va_start(args, format);      \
    log(severity, format, args); \
    va_end(args);                \
  }

  void critical(const char* format, ...) {
    LOG_MESSAGE(CASS_LOG_CRITICAL);
  }

  void error(const char* format, ...) { LOG_MESSAGE(CASS_LOG_ERROR); }

  void warn(const char* format, ...) { LOG_MESSAGE(CASS_LOG_WARN); }

  void info(const char* format, ...) { LOG_MESSAGE(CASS_LOG_INFO); }

  void debug(const char* format, ...) { LOG_MESSAGE(CASS_LOG_DEBUG); }

#undef LOG_MESSAGE

private:
  struct LogMessage {
    uint64_t time;
    CassLogLevel severity;
    std::string message;
  };

  void close() { log_queue_.close_handles(); }

  std::string format_message(const char* format, va_list args) {
    char buffer[1024];
    int n = vsnprintf(buffer, sizeof(buffer), format, args);
    return std::string(buffer, n);
  }

  void log(CassLogLevel severity, const char* format, va_list args) {
    if (is_closing_) {
      return;
    }
    LogMessage* log_message = new LogMessage;
    log_message->severity = severity;
    log_message->message = format_message(format, args);
    log_queue_.enqueue(log_message);
  }

  static void on_log(uv_async_t* async, int status) {
    Logger* logger = static_cast<Logger*>(async->data);

    LogMessage* log_message;
    while (logger->log_queue_.dequeue(log_message)) {
      if (log_message != nullptr) {
        if (log_message->severity != CASS_LOG_DISABLED) {
          CassString message = cass_string_init2(log_message->message.data(),
                                                 log_message->message.size());
          logger->cb_(logger->data_, log_message->time, log_message->severity,
                      message);
        }
        delete log_message;
      }
    }

    if (logger->is_closing_) {
      logger->close();
    }
  }

  void* data_;
  CassLogCallback cb_;
  CassLogLevel log_level_;
  AsyncQueue<MPMCQueue<LogMessage*>> log_queue_;
  std::atomic<bool> is_closing_;
};

} // namespace cass

#endif
