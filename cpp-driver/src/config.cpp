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

#include "config.hpp"

namespace cass {

void Config::init_profiles() {
  // Build the cluster profile load balancing policy
  default_profile_.build_load_balancing_policy();
  load_balancing_policies_.clear();
  load_balancing_policies_.push_back(default_profile_.load_balancing_policy());

  // Build/Assign the load balancing policies and update profile settings
  for (ExecutionProfile::Map::iterator it = profiles_.begin();
       it != profiles_.end(); ++it) {
    if (it->second.consistency() == CASS_CONSISTENCY_UNKNOWN) {
      it->second.set_consistency(default_profile_.consistency());
    }

    if (it->second.serial_consistency() == CASS_CONSISTENCY_UNKNOWN) {
      it->second.set_serial_consistency(default_profile_.serial_consistency());
    }

    if (it->second.request_timeout_ms() == CASS_UINT64_MAX) {
      it->second.set_request_timeout(default_profile_.request_timeout_ms());
    }

    it->second.build_load_balancing_policy();
    const LoadBalancingPolicy::Ptr& load_balancing_policy = it->second.load_balancing_policy();
    if (load_balancing_policy) {
      LOG_TRACE("Built load balancing policy for '%s' execution profile",
                it->first.c_str());
      load_balancing_policies_.push_back(it->second.load_balancing_policy());
    } else {
      it->second.set_load_balancing_policy(default_profile_.load_balancing_policy().get());
    }

    if (!it->second.retry_policy()) {
      it->second.set_retry_policy(default_profile_.retry_policy().get());
    }
  }
}

} // namespace cass
