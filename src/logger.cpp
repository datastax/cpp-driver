/*
  Copyright (c) 2014 DataStax

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

uv_once_t logger_init_guard = UV_ONCE_INIT;

extern "C" {

void cass_log_set_level(CassLogLevel level) {
  cass::Logger::set_level(level);
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

void default_log_callback(cass_uint64_t time_ms, CassLogLevel severity,
                          CassString message, void* data) {
  fprintf(stderr, "%u.%03u [%s]: %.*s\n",
          static_cast<unsigned int>(time_ms/1000), static_cast<unsigned int>(time_ms % 1000),
          cass_log_level_string(severity),
          static_cast<int>(message.length), message.data);
}

void Logger::LogThread::log(CassLogLevel severity, const char* format, va_list args) {
  LogMessage* log_message = new LogMessage;
  log_message->time = get_time_since_epoch_ms();
  log_message->severity = severity;
  log_message->message = format_message(format, args);
  if(!log_queue_.enqueue(log_message)) {
    fprintf(stderr, "Exceeded logging queue max size\n");
    CassString message = cass_string_init2(log_message->message.data(),
                                           log_message->message.size());
    cb_(log_message->time, log_message->severity, message, data_);
    delete log_message;
  }
}

std::string Logger::LogThread::format_message(const char* format, va_list args) {
  char buffer[1024];
  int n = vsnprintf(buffer, sizeof(buffer), format, args);
  return std::string(buffer, n);
}

void Logger::LogThread::on_log(uv_async_t* async, int status) {
  LogThread* logger = static_cast<LogThread*>(async->data);

  LogMessage* log_message;
  while (logger->log_queue_.dequeue(log_message)) {
    if (log_message != NULL) {
      if (log_message->severity != CASS_LOG_DISABLED) {
        CassString message = cass_string_init2(log_message->message.data(),
                                               log_message->message.size());
        logger->cb_(log_message->time, log_message->severity,
                    message, logger->data_);
      }
      delete log_message;
    }
  }
}

CassLogLevel Logger::level_        = CASS_LOG_WARN;
CassLogCallback Logger::cb_        = default_log_callback;
void* Logger::data_                = NULL;
size_t Logger::queue_size_         = 16 * 1024;
Logger::LogThread* Logger::thread_ = NULL;

void Logger::set_level(CassLogLevel level) {
  level_ = level;
}

void Logger::set_queue_size(size_t queue_size) {
  queue_size_ = queue_size;
}

void Logger::set_callback(CassLogCallback cb, void* data) {
  cb_ = cb;
  data_ = data;
}

void Logger::init() {
  uv_once(&logger_init_guard, Logger::internal_init);
}

void Logger::internal_init() {
  thread_ = new LogThread(cb_, data_, queue_size_);
  thread_->run();
}

} // namespace cass
