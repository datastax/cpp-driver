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

#ifndef DATASTAX_INTERNAL_WHITELIST_POLICY_HPP
#define DATASTAX_INTERNAL_WHITELIST_POLICY_HPP

#include "host.hpp"
#include "list_policy.hpp"
#include "load_balancing.hpp"
#include "scoped_ptr.hpp"

namespace datastax { namespace internal { namespace core {

class WhitelistPolicy : public ListPolicy {
public:
  WhitelistPolicy(LoadBalancingPolicy* child_policy, const ContactPointList& hosts)
      : ListPolicy(child_policy)
      , hosts_(hosts) {}

  virtual ~WhitelistPolicy() {}

  WhitelistPolicy* new_instance() {
    return new WhitelistPolicy(child_policy_->new_instance(), hosts_);
  }

private:
  bool is_valid_host(const Host::Ptr& host) const;

  ContactPointList hosts_;

private:
  DISALLOW_COPY_AND_ASSIGN(WhitelistPolicy);
};

}}} // namespace datastax::internal::core

#endif
