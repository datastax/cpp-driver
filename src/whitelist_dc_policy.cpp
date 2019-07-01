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

#include "whitelist_dc_policy.hpp"

using namespace datastax::internal::core;

bool WhitelistDCPolicy::is_valid_host(const Host::Ptr& host) const {
  const String& host_dc = host->dc();
  for (DcList::const_iterator it = dcs_.begin(), end = dcs_.end(); it != end; ++it) {
    if (host_dc.compare(*it) == 0) {
      return true;
    }
  }
  return false;
}
