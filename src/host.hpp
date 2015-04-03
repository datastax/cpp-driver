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
#include <vector>

namespace cass {

struct TimestampedAverage {
  TimestampedAverage()
    : average(-1)
    , timestamp(0)
    , num_measured(0) {}

  int64_t average;
  uint64_t timestamp;
  uint64_t num_measured;
};

class Host : public RefCounted<Host> {
public:
  class StateListener {
  public:
    virtual ~StateListener() {}
    virtual void on_add(const SharedRefPtr<Host>& host) = 0;
    virtual void on_remove(const SharedRefPtr<Host>& host) = 0;
    virtual void on_up(const SharedRefPtr<Host>& host) = 0;
    virtual void on_down(const SharedRefPtr<Host>& host) = 0;
  };

  enum HostState {
    ADDED,
    UP,
    DOWN
  };

  Host(const Address& address, bool mark)
      : address_(address)
      , mark_(mark)
      , state_(ADDED) {}

  const Address& address() const { return address_; }

  bool mark() const { return mark_; }
  void set_mark(bool mark) { mark_ = mark; }

  const std::string& rack() const { return rack_; }
  const std::string& dc() const { return dc_; } 
  void set_rack_and_dc(const std::string& rack, const std::string& dc) {
    rack_ = rack;
    dc_ = dc;
  }

  const std::string& listen_address() const { return listen_address_; }
  void set_listen_address(const std::string& listen_address) {
    listen_address_ = listen_address;
  }

  bool was_just_added() const { return state() == ADDED; }

  bool is_up() const { return state() == UP; }
  void set_up() { set_state(UP); }
  bool is_down() const { return state() == DOWN; }
  void set_down() { set_state(DOWN); }

  std::string to_string() const {
    std::ostringstream ss;
    ss << address_.to_string();
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
      , threshold_to_account_(threshold_to_account) {}

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
  bool mark_;
  Atomic<HostState> state_;
  std::string listen_address_;
  std::string rack_;
  std::string dc_;

  ScopedPtr<LatencyTracker> latency_tracker_;

private:
  DISALLOW_COPY_AND_ASSIGN(Host);
};

typedef std::map<Address, SharedRefPtr<Host> > HostMap;
typedef std::vector<SharedRefPtr<Host> > HostVec;
typedef CopyOnWritePtr<HostVec> CopyOnWriteHostVec;

void copy_hosts(const HostMap& from_hosts, CopyOnWriteHostVec& to_hosts);
void add_host(CopyOnWriteHostVec& hosts, const SharedRefPtr<Host>& host);
void remove_host(CopyOnWriteHostVec& hosts, const SharedRefPtr<Host>& host);

} // namespace cass

#endif
