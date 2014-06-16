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

#ifndef __CASS_CONFIG_HPP_INCLUDED__
#define __CASS_CONFIG_HPP_INCLUDED__

#include <list>
#include <string>

#include "cassandra.h"

namespace cass {

void default_log_callback(void* data, cass_uint64_t time, CassLogLevel severity,
                          CassString message);

class Config {
public:
  Config()
      : port_(9042)
      , version_("3.0.0")
      , thread_count_io_(1)
      , queue_size_io_(4096)
      , queue_size_event_(4096)
      , queue_size_log_(4096)
      , core_connections_per_host_(2)
      , max_connections_per_host_(4)
      , reconnect_wait_time_(2000)
      , max_simultaneous_creation_(1)
      , max_pending_requests_(128 * max_connections_per_host_)
      ,
      // TODO(mpenick): Determine good timeout durations
      connect_timeout_(1000)
      , write_timeout_(1000)
      , read_timeout_(1000)
      , log_level_(CASS_LOG_ERROR)
      , log_data_(nullptr)
      , log_callback_(default_log_callback) {}

  void log_callback(CassLogCallback callback) { log_callback_ = callback; }

  size_t thread_count_io() const { return thread_count_io_; }

  size_t queue_size_io() const { return queue_size_io_; }

  size_t queue_size_event() const { return queue_size_event_; }

  size_t queue_size_log() const { return queue_size_log_; }

  size_t core_connections_per_host() const {
    return core_connections_per_host_;
  }

  size_t max_connections_per_host() const { return max_connections_per_host_; }

  size_t reconnect_wait() const { return reconnect_wait_time_; }

  size_t max_simultaneous_creation() const {
    return max_simultaneous_creation_;
  }

  size_t max_pending_requests() const { return max_pending_requests_; }

  size_t connect_timeout() const { return connect_timeout_; }

  size_t write_timeout() const { return write_timeout_; }

  size_t read_timeout() const { return read_timeout_; }

  const std::list<std::string>& contact_points() const {
    return contact_points_;
  }

  int port() const { return port_; }

  CassLogLevel log_level() const { return log_level_; }

  void* log_data() const { return log_data_; }

  CassLogCallback log_callback() const { return log_callback_; }

  CassError set_option(CassOption option, const void* value, size_t size);
  CassError option(CassOption option, void* value, size_t* size) const;

private:
  int port_;
  std::string version_;
  std::list<std::string> contact_points_;
  size_t thread_count_io_;
  size_t queue_size_io_;
  size_t queue_size_event_;
  size_t queue_size_log_;
  size_t core_connections_per_host_;
  size_t max_connections_per_host_;
  size_t reconnect_wait_time_;
  size_t max_simultaneous_creation_;
  size_t max_pending_requests_;
  size_t connect_timeout_;
  size_t write_timeout_;
  size_t read_timeout_;
  CassLogLevel log_level_;
  void* log_data_;
  CassLogCallback log_callback_;
};

} // namespace cass

#endif
