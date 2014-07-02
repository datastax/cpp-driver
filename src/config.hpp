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

#include "cassandra.h"

#include <list>
#include <string>

namespace cass {

void default_log_callback(void* data, cass_uint64_t time, CassLogLevel severity,
                          CassString message);

class Config {
public:
  typedef std::list<std::string> ContactPointList;

  Config()
      : port_(9042)
      , protocol_version_(2)
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
      // TODO(mpenick): Determine good timeout durations
      , connect_timeout_(1000)
      , write_timeout_(1000)
      , read_timeout_(1000)
      , log_level_(CASS_LOG_WARN)
      , log_data_(NULL)
      , log_callback_(default_log_callback) {}

  void log_callback(CassLogCallback callback) { log_callback_ = callback; }

  unsigned thread_count_io() const { return thread_count_io_; }

  unsigned queue_size_io() const { return queue_size_io_; }

  unsigned queue_size_event() const { return queue_size_event_; }

  unsigned queue_size_log() const { return queue_size_log_; }

  unsigned core_connections_per_host() const {
    return core_connections_per_host_;
  }

  unsigned max_connections_per_host() const { return max_connections_per_host_; }

  unsigned reconnect_wait() const { return reconnect_wait_time_; }

  unsigned max_simultaneous_creation() const {
    return max_simultaneous_creation_;
  }

  unsigned max_pending_requests() const { return max_pending_requests_; }

  unsigned connect_timeout() const { return connect_timeout_; }

  unsigned write_timeout() const { return write_timeout_; }

  unsigned read_timeout() const { return read_timeout_; }

  const std::list<std::string>& contact_points() const {
    return contact_points_;
  }

  int port() const { return port_; }

  int protocol_version() const { return protocol_version_; }

  CassLogLevel log_level() const { return log_level_; }

  void* log_data() const { return log_data_; }

  CassLogCallback log_callback() const { return log_callback_; }

  CassError set_option(CassOption option, const void* value, size_t size);
  CassError option(CassOption option, void* value, size_t* size) const;

private:
  int port_;
  int protocol_version_;
  std::string version_;
  ContactPointList contact_points_;
  unsigned thread_count_io_;
  unsigned queue_size_io_;
  unsigned queue_size_event_;
  unsigned queue_size_log_;
  unsigned core_connections_per_host_;
  unsigned max_connections_per_host_;
  unsigned reconnect_wait_time_;
  unsigned max_simultaneous_creation_;
  unsigned max_pending_requests_;
  unsigned connect_timeout_;
  unsigned write_timeout_;
  unsigned read_timeout_;
  CassLogLevel log_level_;
  void* log_data_;
  CassLogCallback log_callback_;
};

} // namespace cass

#endif
