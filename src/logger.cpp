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

#include "logger.hpp"

using namespace datastax::internal;

extern "C" {

void cass_log_cleanup() {
  // Deprecated
}

void cass_log_set_level(CassLogLevel log_level) { Logger::set_log_level(log_level); }

void cass_log_set_callback(CassLogCallback callback, void* data) {
  Logger::set_callback(callback, data);
}

void cass_log_set_queue_size(size_t queue_size) {
  // Deprecated
}

} // extern "C"

namespace datastax { namespace internal { namespace core {

void stderr_log_callback(const CassLogMessage* message, void* data) {
  fprintf(
      stderr, "%u.%03u [%s] (%s:%d:%s): %s\n", static_cast<unsigned int>(message->time_ms / 1000),
      static_cast<unsigned int>(message->time_ms % 1000), cass_log_level_string(message->severity),
      message->file, message->line, message->function, message->message);
}

}}} // namespace datastax::internal::core

void noop_log_callback(const CassLogMessage* message, void* data) {}

CassLogLevel Logger::log_level_ = CASS_LOG_WARN;
CassLogCallback Logger::cb_ = core::stderr_log_callback;
void* Logger::data_ = NULL;

void Logger::internal_log(CassLogLevel severity, const char* file, int line, const char* function,
                          const char* format, va_list args) {
  CassLogMessage message = { get_time_since_epoch_ms(), severity, file, line, function, "" };
  vsnprintf(message.message, sizeof(message.message), format, args);
  Logger::cb_(&message, Logger::data_);
}

void Logger::set_log_level(CassLogLevel log_level) { log_level_ = log_level; }

void Logger::set_callback(CassLogCallback cb, void* data) {
  cb_ = cb == NULL ? noop_log_callback : cb;
  data_ = data;
}
