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

#include "testing.hpp"

#include "address.hpp"
#include "cluster_config.hpp"
#include "external.hpp"
#include "future.hpp"
#include "get_time.hpp"
#include "logger.hpp"
#include "metadata.hpp"
#include "murmur3.hpp"
#include "request_handler.hpp"
#include "result_response.hpp"
#include "session.hpp"

namespace cass {

String get_host_from_future(CassFuture* future) {
  if (future->type() != cass::Future::FUTURE_TYPE_RESPONSE) {
    return "";
  }
  cass::ResponseFuture* response_future =
      static_cast<cass::ResponseFuture*>(future->from());
  return response_future->address().to_string();
}

unsigned get_connect_timeout_from_cluster(CassCluster* cluster) {
  return cluster->config().connect_timeout_ms();
}

int get_port_from_cluster(CassCluster* cluster) {
  return cluster->config().port();
}

String get_contact_points_from_cluster(CassCluster* cluster) {
  String str;

  const ContactPointList& contact_points
      = cluster->config().contact_points();

  for (ContactPointList::const_iterator it = contact_points.begin(),
       end = contact_points.end();
       it != end; ++it) {
    if (str.size() > 0) {
      str.push_back(',');
    }
    str.append(*it);
  }

  return str;
}

int64_t create_murmur3_hash_from_string(const String &value) {
  return MurmurHash3_x64_128(value.data(), value.size(), 0);
}

uint64_t get_time_since_epoch_in_ms() {
  return cass::get_time_since_epoch_ms();
}

uint64_t get_host_latency_average(CassSession* session, String ip_address, int port) {
  Address address;
  if (Address::from_string(ip_address, port, &address)) {
    Host::Ptr host(session->cluster()->find_host(address));
    return host ? host->get_current_average().average : 0;
  }
  return 0;
}

} // namespace cass
