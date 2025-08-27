/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

#include "blacklist_policy.hpp"

using namespace datastax::internal::core;

bool BlacklistPolicy::is_valid_host(const Host::Ptr& host) const {
  const String& host_address = host->address().hostname_or_address();
  for (ContactPointList::const_iterator it = hosts_.begin(), end = hosts_.end(); it != end; ++it) {
    if (host_address.compare(*it) == 0) {
      return false;
    }
  }
  return true;
}
