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

#include "logger.hpp"

#include <uv.h>

extern "C" {

void cass_log_cleanup() {
  cass::Logger::cleanup();
}

void cass_log_set_level(CassLogLevel log_level) {
  cass::Logger::set_log_level(log_level);
}

void cass_log_set_callback(CassLogCallback callback,
                           void* data) {
  cass::Logger::set_callback(callback, data);
}

void cass_log_set_queue_size(cass_size_t queue_size) {
  cass::Logger::set_queue_size(queue_size);
}

} // extern "C"

namespace cass {

uv_once_t logger_init_guard = UV_ONCE_INIT;

void stderr_log_callback(const CassLogMessage* message, void* data) {
  fprintf(stderr, "%u.%03u [%s] (%s:%d:%s): %s\n",
          static_cast<unsigned int>(message->time_ms / 1000),
          static_cast<unsigned int>(message->time_ms % 1000),
          cass_log_level_string(message->severity),
          message->file, message->line, message->function,
          message->message);
}

Logger::LogThread::LogThread(size_t queue_size)
    : log_queue_(queue_size)
    , has_been_warned_(false)
    , is_initialized_(init() == 0 && log_queue_.init(loop(), this, on_log) == 0) {
  if (is_initialized_) {
    run();
  } else {
    fprintf(stderr, "Unable to initialize logging thread\n");
  }
}

Logger::LogThread::~LogThread() {
  if (!is_initialized_) return;
  CassLogMessage log_message;
  log_message.severity = CASS_LOG_DISABLED;
  while (!log_queue_.enqueue(log_message)) {
    // Keep trying
  }
  join();
}

void Logger::LogThread::log(CassLogLevel severity,
                            const char* file, int line, const char* function,
                            const char* format, va_list args) {
  CassLogMessage message = {
    get_time_since_epoch_ms(), severity,
    file, line, function,
    ""
  };

  vsnprintf(message.message, sizeof(message.message), format, args);

  if (!log_queue_.enqueue(message)) {
    if (has_been_warned_) {
      has_been_warned_ = true;
      fprintf(stderr, "Warning: Exceeded log queue size (decreased logging performance)\n");
    }
    Logger::cb_(&message, Logger::data_);
  }
}

void Logger::LogThread::close_handles() {
  LoopThread::close_handles();
  log_queue_.close_handles();
}

#if UV_VERSION_MAJOR == 0
void Logger::LogThread::on_log(uv_async_t* async, int status) {
#else
void Logger::LogThread::on_log(uv_async_t* async) {
#endif
  LogThread* logger = static_cast<LogThread*>(async->data);
  CassLogMessage log_message;
  bool is_closing = false;
  while (logger->log_queue_.dequeue(log_message)) {
    if (log_message.severity != CASS_LOG_DISABLED) {
      Logger::cb_(&log_message, Logger::data_);
    } else {
      is_closing = true;
    }
  }

  if (is_closing) {
    logger->close_handles();
  }
}

CassLogLevel Logger::log_level_ = CASS_LOG_WARN;
CassLogCallback Logger::cb_ = stderr_log_callback;
void* Logger::data_ = NULL;
size_t Logger::queue_size_ = 2048;
ScopedPtr<Logger::LogThread> Logger::thread_;

void Logger::set_log_level(CassLogLevel log_level) {
  log_level_ = log_level;
}

void Logger::set_queue_size(size_t queue_size) {
  queue_size_ = queue_size;
}

void Logger::set_callback(CassLogCallback cb, void* data) {
  cb_ = cb;
  data_ = data;
}

void Logger::init() {
  if (log_level_ == CASS_LOG_DISABLED) return;
  uv_once(&logger_init_guard, Logger::internal_init);
}

void Logger::cleanup() {
  thread_.reset();
}

void Logger::internal_init() {
  thread_.reset(new LogThread(queue_size_));
}

} // namespace cass
