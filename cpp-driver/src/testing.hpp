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

#ifndef DATASTAX_INTERNAL_TESTING_HPP
#define DATASTAX_INTERNAL_TESTING_HPP

#include "cassandra.h"
#include "string.hpp"
#include "string_ref.hpp"
#include "vector.hpp"

#include <stdint.h>

namespace datastax { namespace internal { namespace testing {

CASS_EXPORT String get_host_from_future(CassFuture* future);

CASS_EXPORT StringVec get_attempted_hosts_from_future(CassFuture* future);

CASS_EXPORT unsigned get_connect_timeout_from_cluster(CassCluster* cluster);

CASS_EXPORT int get_port_from_cluster(CassCluster* cluster);

CASS_EXPORT String get_contact_points_from_cluster(CassCluster* cluster);

CASS_EXPORT uint64_t get_host_latency_average(CassSession* session, String ip_address, int port);

CASS_EXPORT CassConsistency get_consistency(const CassStatement* statement);

CASS_EXPORT CassConsistency get_serial_consistency(const CassStatement* statement);

CASS_EXPORT uint64_t get_request_timeout_ms(const CassStatement* statement);

CASS_EXPORT const CassRetryPolicy* get_retry_policy(const CassStatement* statement);

CASS_EXPORT String get_server_name(CassFuture* future);

CASS_EXPORT void set_record_attempted_hosts(CassStatement* statement, bool enable);

}}} // namespace datastax::internal::testing

#endif
