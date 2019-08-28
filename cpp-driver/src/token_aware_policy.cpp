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

#include "token_aware_policy.hpp"

#include "random.hpp"
#include "request_handler.hpp"

#include <algorithm>

using namespace datastax;
using namespace datastax::internal;
using namespace datastax::internal::core;

// The number of replicas is bounded by replication factor per DC. In practice, the number
// of replicas is fairly small so a linear search should be extremely fast.
static inline bool contains(const CopyOnWriteHostVec& replicas, const Address& address) {
  for (HostVec::const_iterator i = replicas->begin(), end = replicas->end(); i != end; ++i) {
    if ((*i)->address() == address) {
      return true;
    }
  }
  return false;
}

void TokenAwarePolicy::init(const Host::Ptr& connected_host, const HostMap& hosts, Random* random,
                            const String& local_dc) {
  if (random != NULL) {
    if (shuffle_replicas_) {
      // Store random so that it can be used to shuffle replicas.
      random_ = random;
    } else {
      // Make sure that different instances of the token aware policy (e.g. different sessions)
      // don't use the same host order.
      index_ = random->next(std::max(static_cast<size_t>(1), hosts.size()));
    }
  }
  ChainedLoadBalancingPolicy::init(connected_host, hosts, random, local_dc);
}

QueryPlan* TokenAwarePolicy::new_query_plan(const String& keyspace, RequestHandler* request_handler,
                                            const TokenMap* token_map) {
  if (request_handler != NULL) {
    const RoutableRequest* request =
        static_cast<const RoutableRequest*>(request_handler->request());
    switch (request->opcode()) {
      {
        case CQL_OPCODE_QUERY:
        case CQL_OPCODE_EXECUTE:
        case CQL_OPCODE_BATCH:
          String routing_key;
          if (request->get_routing_key(&routing_key) && !keyspace.empty()) {
            if (token_map != NULL) {
              CopyOnWriteHostVec replicas = token_map->get_replicas(keyspace, routing_key);
              if (replicas && !replicas->empty()) {
                if (random_ != NULL) {
                  random_shuffle(replicas->begin(), replicas->end(), random_);
                }
                return new TokenAwareQueryPlan(
                    child_policy_.get(),
                    child_policy_->new_query_plan(keyspace, request_handler, token_map), replicas,
                    index_);
              }
            }
          }
          break;
      }

      default:
        break;
    }
  }
  return child_policy_->new_query_plan(keyspace, request_handler, token_map);
}

Host::Ptr TokenAwarePolicy::TokenAwareQueryPlan::compute_next() {
  while (remaining_ > 0) {
    --remaining_;
    const Host::Ptr& host((*replicas_)[index_++ % replicas_->size()]);
    if (child_policy_->is_host_up(host->address()) &&
        child_policy_->distance(host) == CASS_HOST_DISTANCE_LOCAL) {
      return host;
    }
  }

  Host::Ptr host;
  while ((host = child_plan_->compute_next())) {
    if (!contains(replicas_, host->address()) ||
        child_policy_->distance(host) != CASS_HOST_DISTANCE_LOCAL) {
      return host;
    }
  }
  return Host::Ptr();
}
