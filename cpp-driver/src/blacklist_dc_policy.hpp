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

#ifndef DATASTAX_INTERNAL_BLACKLIST_DC_POLICY_HPP
#define DATASTAX_INTERNAL_BLACKLIST_DC_POLICY_HPP

#include "host.hpp"
#include "list_policy.hpp"
#include "load_balancing.hpp"
#include "scoped_ptr.hpp"

namespace datastax { namespace internal { namespace core {

class BlacklistDCPolicy : public ListPolicy {
public:
  BlacklistDCPolicy(LoadBalancingPolicy* child_policy, const DcList& dcs)
      : ListPolicy(child_policy)
      , dcs_(dcs) {}

  virtual ~BlacklistDCPolicy() {}

  BlacklistDCPolicy* new_instance() {
    return new BlacklistDCPolicy(child_policy_->new_instance(), dcs_);
  }

private:
  bool is_valid_host(const Host::Ptr& host) const;

  DcList dcs_;

private:
  DISALLOW_COPY_AND_ASSIGN(BlacklistDCPolicy);
};

}}} // namespace datastax::internal::core

#endif
