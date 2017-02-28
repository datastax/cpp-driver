/*
  Copyright (c) 2014-2016 DataStax

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

#include "blacklist_policy.hpp"

namespace cass {

bool BlacklistPolicy::is_valid_host(const Host::Ptr& host) const {
  const std::string& host_address = host->address().to_string(false);
  for (ContactPointList::const_iterator it = hosts_.begin(),
                                                end = hosts_.end();
       it != end; ++it) {
    if (host_address.compare(*it) == 0) {
      return false;
    }
  }
  return true;
}

} // namespace cass
