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

#ifndef __CQL_CLUSTER_HPP_INCLUDED__
#define __CQL_CLUSTER_HPP_INCLUDED__

#include <list>
#include <string>

#include "cql_session.hpp"

namespace cql {

class Cluster {
  std::string            port_;
  std::string            cql_version_;
  int                    compression_;
  size_t                 max_schema_agreement_wait_;
  size_t                 control_connection_timeout_;
  std::list<std::string> contact_points_;
  size_t                 thread_count_io_;
  size_t                 thread_count_callback_;
  LogCallback            log_callback_;


 public:
  Cluster() :
      port_("9042"),
      cql_version_("3.0.0"),
      compression_(0),
      max_schema_agreement_wait_(10),
      control_connection_timeout_(10),
      thread_count_io_(1),
      thread_count_callback_(4),
      log_callback_(nullptr)
  {}

  void
  log_callback(
      LogCallback callback) {
    log_callback_ = callback;
  }

  cql::Session*
  connect() {
    return connect(NULL, 0);
  }

  cql::Session*
  connect(
      const std::string keyspace) {
    return connect(keyspace.c_str(), keyspace.size());
  }

  cql::Session*
  connect(
      const char* keyspace,
      size_t      size) {
    (void) keyspace;
    (void) size;
    return NULL;
  }

  void
  option(
      int         option,
      const void* value,
      size_t      size) {
    int int_value = *(reinterpret_cast<const int*>(value));

    switch (option) {
      case CQL_OPTION_THREADS_IO:
        thread_count_io_ = int_value;
        break;

      case CQL_OPTION_THREADS_CALLBACK:
        thread_count_callback_ = int_value;
        break;

      case CQL_OPTION_CONTACT_POINT_ADD:
        contact_points_.push_back(
            std::string(reinterpret_cast<const char*>(value), size));
        break;

      case CQL_OPTION_PORT:
        port_.assign(reinterpret_cast<const char*>(value), size);
        break;

      case CQL_OPTION_CQL_VERSION:
        cql_version_.assign(reinterpret_cast<const char*>(value), size);
        break;

      case CQL_OPTION_COMPRESSION:
        compression_ = int_value;
        break;

      case CQL_OPTION_CONTROL_CONNECTION_TIMEOUT:
        control_connection_timeout_ = int_value;
        break;

      case CQL_OPTION_SCHEMA_AGREEMENT_WAIT:
        max_schema_agreement_wait_ = int_value;
        break;
    }
  }

};
}
#endif
