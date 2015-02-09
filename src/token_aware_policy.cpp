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

#include "token_aware_policy.hpp"

namespace cass {

// The number of replicas is bounded by replication factor per DC. In practice, the number
// of replicas is fairly small so a linear search should be extremely fast.
static inline bool contains(const CopyOnWriteHostVec& replicas, const Address& address) {
  for (HostVec::const_iterator i = replicas->begin(),
       end = replicas->end(); i != end; ++i) {
    if ((*i)->address() == address) {
      return true;
    }
  }
  return false;
}

QueryPlan* TokenAwarePolicy::new_query_plan(const std::string& connected_keyspace,
                                            const Request* request,
                                            const TokenMap& token_map) {
  if (request != NULL) {
    switch (request->opcode()) {
      {
      case CQL_OPCODE_QUERY:
      case CQL_OPCODE_EXECUTE:
      case CQL_OPCODE_BATCH:
        const RoutableRequest* rr = static_cast<const RoutableRequest*>(request);
        const std::string& statement_keyspace = rr->keyspace();
        const std::string& keyspace = statement_keyspace.empty()
                                      ? connected_keyspace : statement_keyspace;
        std::string routing_key;
        if (rr->get_routing_key(&routing_key) && !keyspace.empty()) {
          CopyOnWriteHostVec replicas = token_map.get_replicas(keyspace, routing_key);
          if (!replicas->empty()) {
            return new TokenAwareQueryPlan(child_policy_.get(),
                                           child_policy_->new_query_plan(connected_keyspace, request, token_map),
                                           replicas,
                                           index_++);
          }
        }
        break;
      }

      default:
        break;
    }
  }
  return child_policy_->new_query_plan(connected_keyspace, request, token_map);
}

SharedRefPtr<Host> TokenAwarePolicy::TokenAwareQueryPlan::compute_next()  {
  while (remaining_ > 0) {
    --remaining_;
    const SharedRefPtr<Host>& host((*replicas_)[index_++ % replicas_->size()]);
    if (host->is_up() && child_policy_->distance(host) == CASS_HOST_DISTANCE_LOCAL) {
      return host;
    }
  }

  SharedRefPtr<Host> host;
  while ((host = child_plan_->compute_next())) {
    if (!contains(replicas_, host->address()) ||
        child_policy_->distance(host) != CASS_HOST_DISTANCE_LOCAL) {
      return host;
    }
  }
  return SharedRefPtr<Host>();
}

} // namespace cass
