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

using namespace datastax::internal::core;

void Config::init_profiles() {
  // Initialize the profile settings (if needed)
  for (ExecutionProfile::Map::iterator it = profiles_.begin(); it != profiles_.end(); ++it) {
    if (it->second.serial_consistency() == CASS_CONSISTENCY_UNKNOWN) {
      it->second.set_serial_consistency(default_profile_.serial_consistency());
    }

    if (it->second.request_timeout_ms() == CASS_UINT64_MAX) {
      it->second.set_request_timeout(default_profile_.request_timeout_ms());
    }

    if (!it->second.retry_policy()) {
      it->second.set_retry_policy(default_profile_.retry_policy().get());
    }

    if (!it->second.speculative_execution_policy()) {
      it->second.set_speculative_execution_policy(
          default_profile_.speculative_execution_policy()->new_instance());
    }
  }
}
