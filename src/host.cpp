/*
  Copyright (c) 2014-2015 DataStax

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

void copy_hosts(const HostMap& from_hosts, CopyOnWriteHostVec& to_hosts) {
  to_hosts->reserve(from_hosts.size());
  for (HostMap::const_iterator i = from_hosts.begin(),
       end = from_hosts.end(); i != end; ++i) {
    to_hosts->push_back(i->second);
  }
}

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

void Host::LatencyTracker::update(uint64_t latency_ns) {
  uint64_t now = uv_hrtime();

  ScopedSpinlock l(SpinlockPool<LatencyTracker>::get_spinlock(this));

  TimestampedAverage previous = current_;

  if (previous.num_measured < threshold_to_account_) {
    current_.average = -1;
  } else if (previous.average < 0) {
    current_.average = latency_ns;
  } else {
    int64_t delay = now - previous.timestamp;
    if (delay <= 0) {
      return;
    }

    double scaled_delay = static_cast<double>(delay) / scale_ns_;
    double weight = log(scaled_delay + 1) / scaled_delay;
    current_.average = static_cast<int64_t>((1.0 - weight) * latency_ns + weight * previous.average);
  }

  current_.num_measured = previous.num_measured + 1;
  current_.timestamp = now;
}

} // namespace cass
