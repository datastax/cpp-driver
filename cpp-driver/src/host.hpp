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

#ifndef __CASS_HOST_HPP_INCLUDED__
#define __CASS_HOST_HPP_INCLUDED__

#include "address.hpp"
#include "atomic.hpp"
#include "copy_on_write_ptr.hpp"
#include "get_time.hpp"
#include "logger.hpp"
#include "macros.hpp"
#include "ref_counted.hpp"
#include "scoped_ptr.hpp"
#include "spin_lock.hpp"

#include <map>
#include <math.h>
#include <set>
#include <sstream>
#include <stdint.h>
#include <vector>

namespace cass {

struct TimestampedAverage {
  TimestampedAverage()
    : average(-1)
    , timestamp(0)
    , num_measured(0) { }

  int64_t average;
  uint64_t timestamp;
  uint64_t num_measured;
};

class VersionNumber {
public:
  VersionNumber()
    : major_version_(0)
    , minor_version_(0)
    , patch_version_(0) { }

  VersionNumber(int major_version, int minor_version, int patch_version)
    : major_version_(major_version)
    , minor_version_(minor_version)
    , patch_version_(patch_version) { }

  bool operator >=(const VersionNumber& other) const {
    return compare(other) >= 0;
  }

  bool operator <(const VersionNumber& other) const {
    return compare(other) < 0;
  }

  int compare(const VersionNumber& other) const {
    if (major_version_ < other.major_version_) return -1;
    if (major_version_ > other.major_version_) return  1;

    if (minor_version_ < other.minor_version_) return -1;
    if (minor_version_ > other.minor_version_) return  1;

    if (patch_version_ < other.patch_version_) return -1;
    if (patch_version_ > other.patch_version_) return  1;

    return 0;
  }

  bool parse(const std::string& version);

  int major_version() const { return major_version_; }
  int minor_version() const { return minor_version_; }
  int patch_version() const { return patch_version_; }

private:
  int major_version_;
  int minor_version_;
  int patch_version_;
};

class Host : public RefCounted<Host> {
public:
  typedef SharedRefPtr<Host> Ptr;
  typedef SharedRefPtr<const Host> ConstPtr;

  class StateListener {
  public:
    virtual ~StateListener() { }
    virtual void on_add(const Ptr& host) = 0;
    virtual void on_remove(const Ptr& host) = 0;
    virtual void on_up(const Ptr& host) = 0;
    virtual void on_down(const Ptr& host) = 0;
  };

  enum HostState {
    ADDED,
    UP,
    DOWN
  };

  Host(const Address& address, bool mark)
      : address_(address)
      , rack_id_(0)
      , dc_id_(0)
      , mark_(mark)
      , state_(ADDED)
      , address_string_(address.to_string()) { }

  const Address& address() const { return address_; }
  const std::string& address_string() const { return address_string_; }

  bool mark() const { return mark_; }
  void set_mark(bool mark) { mark_ = mark; }

  const std::string hostname() const { return hostname_; }
  void set_hostname(const std::string& hostname) {
    if (!hostname.empty() && hostname[hostname.size() - 1] == '.') {
      // Strip off trailing dot for hostcheck comparison
      hostname_ = hostname.substr(0, hostname.size() - 1);
    } else {
      hostname_ = hostname;
    }
  }

  const std::string& rack() const { return rack_; }
  const std::string& dc() const { return dc_; }
  void set_rack_and_dc(const std::string& rack, const std::string& dc) {
    rack_ = rack;
    dc_ = dc;
  }

  uint32_t rack_id() const { return rack_id_; }
  uint32_t dc_id() const { return dc_id_; }
  void set_rack_and_dc_ids(uint32_t rack_id, uint32_t dc_id) {
    rack_id_ = rack_id;
    dc_id_ = dc_id;
  }

  const std::string& listen_address() const { return listen_address_; }
  void set_listen_address(const std::string& listen_address) {
    listen_address_ = listen_address;
  }

  const VersionNumber& cassandra_version() const { return cassandra_version_; }
  void set_cassaandra_version(const VersionNumber& cassandra_version) {
    cassandra_version_ = cassandra_version;
  }

  bool was_just_added() const { return state() == ADDED; }

  bool is_up() const { return state() == UP; }
  void set_up() { set_state(UP); }
  bool is_down() const { return state() == DOWN; }
  void set_down() { set_state(DOWN); }

  std::string to_string() const {
    std::ostringstream ss;
    ss << address_string_;
    if (!rack_.empty() || !dc_.empty()) {
      ss << " [" << rack_ << ':' << dc_ << "]";
    }
    return ss.str();
  }

  void enable_latency_tracking(uint64_t scale, uint64_t min_measured) {
    if (!latency_tracker_) {
      latency_tracker_.reset(new LatencyTracker(scale, (30LL * min_measured) / 100LL));
    }
  }

  void update_latency(uint64_t latency_ns) {
    if (latency_tracker_) {
      LOG_TRACE("Latency %f ms for %s", static_cast<double>(latency_ns) / 1e6, to_string().c_str());
      latency_tracker_->update(latency_ns);
    }
  }

  TimestampedAverage get_current_average() const {
    if (latency_tracker_) {
      return latency_tracker_->get();
    }
    return TimestampedAverage();
  }

private:
  class LatencyTracker {
  public:
    LatencyTracker(uint64_t scale_ns, uint64_t threshold_to_account)
      : scale_ns_(scale_ns)
      , threshold_to_account_(threshold_to_account) { }

    void update(uint64_t latency_ns);

    TimestampedAverage get() const {
      ScopedSpinlock l(SpinlockPool<LatencyTracker>::get_spinlock(this));
      return current_;
    }

  private:
    uint64_t scale_ns_;
    uint64_t threshold_to_account_;
    TimestampedAverage current_;

  private:
    DISALLOW_COPY_AND_ASSIGN(LatencyTracker);
  };

private:
  HostState state() const {
    return state_.load(MEMORY_ORDER_ACQUIRE);
  }

  void set_state(HostState state) {
    state_.store(state, MEMORY_ORDER_RELEASE);
  }

  Address address_;
  uint32_t rack_id_;
  uint32_t dc_id_;
  bool mark_;
  Atomic<HostState> state_;
  std::string address_string_;
  std::string listen_address_;
  VersionNumber cassandra_version_;
  std::string hostname_;
  std::string rack_;
  std::string dc_;

  ScopedPtr<LatencyTracker> latency_tracker_;

private:
  DISALLOW_COPY_AND_ASSIGN(Host);
};

typedef std::map<Address, Host::Ptr> HostMap;
struct GetHost {
  typedef std::pair<Address, Host::Ptr> Pair;
  Host::Ptr operator()(const Pair& pair) const {
    return pair.second;
  }
};
typedef std::pair<Address, Host::Ptr> HostPair;
typedef std::vector<Host::Ptr> HostVec;
typedef CopyOnWritePtr<HostVec> CopyOnWriteHostVec;

void add_host(CopyOnWriteHostVec& hosts, const Host::Ptr& host);
void remove_host(CopyOnWriteHostVec& hosts, const Host::Ptr& host);

} // namespace cass

#endif
