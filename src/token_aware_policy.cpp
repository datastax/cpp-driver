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

#include "token_aware_policy.hpp"

namespace cass {


QueryPlan* TokenAwarePolicy::new_query_plan(const std::string& connected_keyspace,
                                            const Request* request,
                                            const DHTMetadata& dht) {
  if (request != NULL) {
    switch(request->opcode()) {
      {
      case CQL_OPCODE_QUERY:
      case CQL_OPCODE_EXECUTE:
      case CQL_OPCODE_BATCH:
        const RoutableRequest* rr = static_cast<const RoutableRequest*>(request);
        const std::string& statement_keyspace = rr->keyspace();
        const BufferRefs& keys = rr->key_parts();
        const std::string& keyspace = statement_keyspace.empty()
                                      ? connected_keyspace : statement_keyspace;
        if (!keys.empty() && !keyspace.empty()) {
          COWHostVec replicas = dht.get_replicas(keyspace, keys);
          if (!replicas->empty()) {
            return new TokenAwareQueryPlan(child_policy_.get(),
                                           child_policy_->new_query_plan(connected_keyspace, request, dht),
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
  return child_policy_->new_query_plan(connected_keyspace, request, dht);
}

bool TokenAwarePolicy::TokenAwareQueryPlan::compute_next(Address* address)  {
  while (remaining_ > 0) {
    --remaining_;
    const SharedRefPtr<Host>& host((*replicas_)[index_++ % replicas_->size()]);
    replicas_attempted_.insert(host->address());
    if (host->is_up() && child_policy_->distance(host) == CASS_HOST_DISTANCE_LOCAL) {
      *address = host->address();
      return true;
    }
  }
  while (child_plan_->compute_next(address)) {
    if (replicas_attempted_.find(*address) == replicas_attempted_.end()) {
      return true;
    }
  }
  return false;
}

} // namespace cass
