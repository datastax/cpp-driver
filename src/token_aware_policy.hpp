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

#ifndef __CASS_TOKEN_AWARE_POLICY_HPP_INCLUDED__
#define __CASS_TOKEN_AWARE_POLICY_HPP_INCLUDED__

#include "dht_meta.hpp"
#include "load_balancing.hpp"
#include "host.hpp"
#include "scoped_ptr.hpp"

namespace cass {

class ChainedLoadBalancingPolicy : public LoadBalancingPolicy {
public:
  ChainedLoadBalancingPolicy(LoadBalancingPolicy* child_policy)
    : child_policy_(child_policy) {}

  virtual ~ChainedLoadBalancingPolicy() {}

  virtual void init(const HostMap& hosts) { return child_policy_->init(hosts); }

  virtual CassHostDistance distance(const SharedRefPtr<Host>& host) { return child_policy_->distance(host); }

  virtual void on_add(const SharedRefPtr<Host>& host) { child_policy_->on_add(host); }

  virtual void on_remove(const SharedRefPtr<Host>& host) { child_policy_->on_remove(host); }

  virtual void on_up(const SharedRefPtr<Host>& host) { child_policy_->on_up(host); }

  virtual void on_down(const SharedRefPtr<Host>& host) { child_policy_->on_down(host); }

protected:
  ScopedRefPtr<LoadBalancingPolicy> child_policy_;
};


class TokenAwarePolicy : public ChainedLoadBalancingPolicy {
public:
  TokenAwarePolicy(LoadBalancingPolicy* child_policy)
      : ChainedLoadBalancingPolicy(child_policy)
      , index_(0) {}

  virtual ~TokenAwarePolicy() {}

  virtual QueryPlan* new_query_plan(const std::string& connected_keyspace,
                                    const Request* request,
                                    const DHTMeta& dht) {

//    ByteBuffer partitionKey = statement.getRoutingKey();
//            String keyspace = statement.getKeyspace();
//            if (keyspace == null)
//                keyspace = loggedKeyspace;
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

  LoadBalancingPolicy* new_instance() { return new TokenAwarePolicy(child_policy_->new_instance()); }

private:
  class TokenAwareQueryPlan : public QueryPlan {
  public:
    TokenAwareQueryPlan(LoadBalancingPolicy* child_policy, QueryPlan* child_plan, const COWHostVec& replicas, size_t start_index)
      : child_policy_(child_policy)
      , child_plan_(child_plan)
      , replicas_(replicas)
      , index_(start_index)
      , remaining_(replicas->size()) {}

    bool compute_next(Address* address)  {
      while (remaining_ > 0) {
        --remaining_;
        const SharedRefPtr<Host>& host((*replicas_)[index_++ % replicas_->size()]);
        replicas_attempted_.insert(host->address());
        if (host->is_up() && child_policy_->distance(host) == CASS_HOST_DISTANCE_LOCAL) {
          *address = host->address();
          printf("TA: %s\n", address->to_string().c_str());
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

  private:
    LoadBalancingPolicy* child_policy_;
    ScopedPtr<QueryPlan> child_plan_;
    COWHostVec replicas_;
    size_t index_;
    size_t remaining_;
    std::set<const Address> replicas_attempted_;
  };

  size_t index_;

private:
  DISALLOW_COPY_AND_ASSIGN(TokenAwarePolicy);
};
} // namespace cass

#endif
