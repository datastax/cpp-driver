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

#include "host.hpp"

#include "row.hpp"
#include "value.hpp"
#include "collection_iterator.hpp"

namespace cass {

void add_host(CopyOnWriteHostVec& hosts, const Host::Ptr& host) {
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

void remove_host(CopyOnWriteHostVec& hosts, const Host::Ptr& host) {
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

bool VersionNumber::parse(const String& version) {
  return sscanf(version.c_str(), "%d.%d.%d", &major_version_, &minor_version_, &patch_version_) >= 2;
}

void Host::set(const Row* row, bool use_tokens) {
  const Value* v;

  String rack;
  row->get_string_by_name("rack", &rack);

  String dc;
  row->get_string_by_name("data_center", &dc);

  String release_version;
  row->get_string_by_name("release_version", &release_version);

  rack_ = rack;
  dc_ = dc;

  VersionNumber server_version;
  if (server_version.parse(release_version)) {
    server_version_ = server_version;
  } else {
    LOG_WARN("Invalid release version string \"%s\" on host %s",
             release_version.c_str(),
             address().to_string().c_str());
  }

  // Possibly correct for invalid Cassandra version numbers for specific
  // versions of DSE.
  if (server_version_ >= VersionNumber(4, 0, 0) &&
      row->get_by_name("dse_version") != NULL) { // DSE only
    String dse_version_str;
    row->get_string_by_name("dse_version", &dse_version_str);

    VersionNumber dse_version;
    if (dse_version.parse(dse_version_str)) {
      // Versions before DSE 6.7 erroneously return they support Cassandra 4.0.0
      // features even though they don't.
      if (dse_version < VersionNumber(6, 7, 0)) {
        server_version_ = VersionNumber(3, 11, 0);
      }
    } else {
      LOG_WARN("Invalid DSE version string \"%s\" on host %s",
               dse_version_str.c_str(),
               address().to_string().c_str());
    }
  }

  row->get_string_by_name("partitioner", &partitioner_);

  if (use_tokens) {
    v = row->get_by_name("tokens");
    if (v != NULL && v->is_collection()) {
      CollectionIterator iterator(v);
      while (iterator.next()) {
        tokens_.push_back(iterator.value()->to_string());
      }
    }
  }
}

} // namespace cass
