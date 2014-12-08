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

#include "host.hpp"

namespace cass {

void add_host(CopyOnWriteHostVec& hosts, const SharedRefPtr<Host>& host) {
  HostVec::iterator i;
  for (i = hosts->begin(); i != hosts->end(); ++i) {
    if ((*i)->address() == host->address()) {
      *i = host;
      break;
    }
  }
  if (i == hosts->end()) {
    hosts->push_back(host);
  }
}

void remove_host(CopyOnWriteHostVec& hosts, const SharedRefPtr<Host>& host) {
  HostVec::iterator i;
  for (i = hosts->begin(); i != hosts->end(); ++i) {
    if ((*i)->address() == host->address()) {
      hosts->erase(i);
      break;
    }
  }
}

} // namespace cass
