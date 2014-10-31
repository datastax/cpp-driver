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

#include "testing.hpp"

#include "address.hpp"
#include "result_response.hpp"
#include "types.hpp"

namespace cass {

std::string get_host_from_future(CassFuture* future) {
  if (future->type() != cass::CASS_FUTURE_TYPE_RESPONSE) {
    return "";
  }
  cass::ResponseFuture* response_future =
      static_cast<cass::ResponseFuture*>(future->from());
  return response_future->get_host_address().to_string();
}

unsigned get_connect_timeout_from_cluster(CassCluster* cluster) {
  return cluster->config().connect_timeout_ms();
}

int get_port_from_cluster(CassCluster* cluster) {
  return cluster->config().port();
}

std::string get_contact_points_from_cluster(CassCluster* cluster) {
  std::string str;

  const cass::Config::ContactPointList& contact_points
      = cluster->config().contact_points();

  for (cass::Config::ContactPointList::const_iterator it = contact_points.begin(),
       end = contact_points.end();
       it != end; ++it) {
    if (str.size() > 0) {
      str.push_back(',');
    }
    str.append(*it);
  }

  return str;
}

} // namespace cass
