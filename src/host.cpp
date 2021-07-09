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

#include "collection_iterator.hpp"
#include "row.hpp"
#include "value.hpp"

using namespace datastax;
using namespace datastax::internal::core;

namespace datastax { namespace internal { namespace core {

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
  remove_host(hosts, host->address());
}

bool remove_host(CopyOnWriteHostVec& hosts, const Address& address) {
  HostVec::iterator i;
  for (i = hosts->begin(); i != hosts->end(); ++i) {
    if ((*i)->address() == address) {
      hosts->erase(i);
      return true;
    }
  }
  return false;
}

}}} // namespace datastax::internal::core

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
    current_.average =
        static_cast<int64_t>((1.0 - weight) * latency_ns + weight * previous.average);
  }

  current_.num_measured = previous.num_measured + 1;
  current_.timestamp = now;
}

bool VersionNumber::parse(const String& version) {
  return sscanf(version.c_str(), "%d.%d.%d", &major_version_, &minor_version_, &patch_version_) >=
         2;
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
    LOG_WARN("Invalid release version string \"%s\" on host %s", release_version.c_str(),
             address().to_string().c_str());
  }

  // Possibly correct for invalid Cassandra version numbers for specific
  // versions of DSE.
  if (server_version_ >= VersionNumber(4, 0, 0) &&
      row->get_by_name("dse_version") != NULL) { // DSE only
    String dse_version_str;
    row->get_string_by_name("dse_version", &dse_version_str);

    if (dse_server_version_.parse(dse_version_str)) {
      // Versions before DSE 6.7 erroneously return they support Cassandra 4.0.0
      // features even though they don't.
      if (dse_server_version_ < VersionNumber(6, 7, 0)) {
        server_version_ = VersionNumber(3, 11, 0);
      }
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

  v = row->get_by_name("rpc_address");
  if (v && !v->is_null()) {
    if (!v->decoder().as_inet(v->size(), address_.port(), &rpc_address_)) {
      LOG_WARN("Invalid address format for `rpc_address`");
    }
    if (Address("0.0.0.0", 0).equals(rpc_address_, false) ||
        Address("::", 0).equals(rpc_address_, false)) {
      LOG_WARN("Found host with 'bind any' for rpc_address; using listen_address (%s) to contact "
               "instead. "
               "If this is incorrect you should configure a specific interface for rpc_address on "
               "the server.",
               address_string_.c_str());
      v = row->get_by_name("listen_address"); // Available in system.local
      if (v && !v->is_null()) {
        v->decoder().as_inet(v->size(), address_.port(), &rpc_address_);
      } else {
        v = row->get_by_name("peer"); // Available in system.peers
        if (v && !v->is_null()) {
          v->decoder().as_inet(v->size(), address_.port(), &rpc_address_);
        }
      }
      if (!rpc_address_.is_valid()) {
        LOG_WARN("Unable to set rpc_address from either listen_address or peer");
      }
    }
  } else {
    LOG_WARN("No rpc_address for host %s in system.local or system.peers.",
             address_string_.c_str());
  }
}

static CassInet to_inet(const Host::Ptr& host) {
  CassInet address;
  if (host->address().is_resolved()) {
    address.address_length = host->address().to_inet(address.address);
  } else {
    address.address_length = host->rpc_address().to_inet(&address.address);
  }
  return address;
}

ExternalHostListener::ExternalHostListener(const CassHostListenerCallback callback, void* data)
    : callback_(callback)
    , data_(data) {}

void ExternalHostListener::on_host_up(const Host::Ptr& host) {
  callback_(CASS_HOST_LISTENER_EVENT_UP, to_inet(host), data_);
}

void ExternalHostListener::on_host_down(const Host::Ptr& host) {
  callback_(CASS_HOST_LISTENER_EVENT_DOWN, to_inet(host), data_);
}

void ExternalHostListener::on_host_added(const Host::Ptr& host) {
  callback_(CASS_HOST_LISTENER_EVENT_ADD, to_inet(host), data_);
}

void ExternalHostListener::on_host_removed(const Host::Ptr& host) {
  callback_(CASS_HOST_LISTENER_EVENT_REMOVE, to_inet(host), data_);
}
