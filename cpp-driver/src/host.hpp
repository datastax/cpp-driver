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

#ifndef DATASTAX_INTERNAL_HOST_HPP
#define DATASTAX_INTERNAL_HOST_HPP

#include "address.hpp"
#include "allocated.hpp"
#include "atomic.hpp"
#include "copy_on_write_ptr.hpp"
#include "get_time.hpp"
#include "logger.hpp"
#include "macros.hpp"
#include "map.hpp"
#include "ref_counted.hpp"
#include "scoped_ptr.hpp"
#include "spin_lock.hpp"
#include "vector.hpp"

#include <math.h>
#include <stdint.h>

namespace datastax { namespace internal { namespace core {

class Row;

struct TimestampedAverage {
  TimestampedAverage()
      : average(-1)
      , timestamp(0)
      , num_measured(0) {}

  int64_t average;
  uint64_t timestamp;
  uint64_t num_measured;
};

class VersionNumber {
public:
  VersionNumber()
      : major_version_(0)
      , minor_version_(0)
      , patch_version_(0) {}

  VersionNumber(int major_version, int minor_version, int patch_version)
      : major_version_(major_version)
      , minor_version_(minor_version)
      , patch_version_(patch_version) {}

  bool operator>=(const VersionNumber& other) const { return compare(other) >= 0; }

  bool operator<(const VersionNumber& other) const { return compare(other) < 0; }

  int compare(const VersionNumber& other) const {
    if (major_version_ < other.major_version_) return -1;
    if (major_version_ > other.major_version_) return 1;

    if (minor_version_ < other.minor_version_) return -1;
    if (minor_version_ > other.minor_version_) return 1;

    if (patch_version_ < other.patch_version_) return -1;
    if (patch_version_ > other.patch_version_) return 1;

    return 0;
  }

  bool parse(const String& version);

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

  Host(const Address& address)
      : address_(address)
      , rpc_address_(address)
      , rack_id_(0)
      , dc_id_(0)
      , address_string_(address.to_string())
      , connection_count_(0)
      , inflight_request_count_(0) {}

  const Address& address() const { return address_; }
  const String& address_string() const { return address_string_; }

  const Address& rpc_address() const { return rpc_address_; }

  void set(const Row* row, bool use_tokens);

  const String& rack() const { return rack_; }
  const String& dc() const { return dc_; }
  void set_rack_and_dc(const String& rack, const String& dc) {
    rack_ = rack;
    dc_ = dc;
  }

  uint32_t rack_id() const { return rack_id_; }
  uint32_t dc_id() const { return dc_id_; }
  void set_rack_and_dc_ids(uint32_t rack_id, uint32_t dc_id) {
    rack_id_ = rack_id;
    dc_id_ = dc_id;
  }

  const String& partitioner() const { return partitioner_; }

  const Vector<String>& tokens() const { return tokens_; }

  const VersionNumber& server_version() const { return server_version_; }

  const VersionNumber& dse_server_version() const { return dse_server_version_; }

  String to_string() const {
    OStringStream ss;
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

  void increment_connection_count() { connection_count_.fetch_add(1, MEMORY_ORDER_RELAXED); }

  void decrement_connection_count() { connection_count_.fetch_sub(1, MEMORY_ORDER_RELAXED); }

  int32_t connection_count() const { return connection_count_.load(MEMORY_ORDER_RELAXED); }

  void increment_inflight_requests() { inflight_request_count_.fetch_add(1, MEMORY_ORDER_RELAXED); }

  void decrement_inflight_requests() { inflight_request_count_.fetch_sub(1, MEMORY_ORDER_RELAXED); }

  int32_t inflight_request_count() const {
    return inflight_request_count_.load(MEMORY_ORDER_RELAXED);
  }

private:
  class LatencyTracker : public Allocated {
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
  Address address_;
  Address rpc_address_;
  uint32_t rack_id_;
  uint32_t dc_id_;
  String address_string_;
  VersionNumber server_version_;
  VersionNumber dse_server_version_;
  String rack_;
  String dc_;
  String partitioner_;
  Vector<String> tokens_;
  Atomic<int32_t> connection_count_;
  Atomic<int32_t> inflight_request_count_;

  ScopedPtr<LatencyTracker> latency_tracker_;

private:
  DISALLOW_COPY_AND_ASSIGN(Host);
};

/**
 * A listener that handles cluster topology and host status changes.
 */
class HostListener {
public:
  virtual ~HostListener() {}

  /**
   * A callback that's called when a host is marked as being UP.
   *
   * @param host A fully populated host object.
   */
  virtual void on_host_up(const Host::Ptr& host) = 0;

  /**
   * A callback that's called when a host is marked as being DOWN.
   *
   * @param host A fully populated host object.
   */
  virtual void on_host_down(const Host::Ptr& host) = 0;

  /**
   * A callback that's called when a new host is added to the cluster.
   *
   * @param host A fully populated host object.
   */
  virtual void on_host_added(const Host::Ptr& host) = 0;

  /**
   * A callback that's called when a host is removed from a cluster.
   *
   * @param address The address of the host.
   */
  virtual void on_host_removed(const Host::Ptr& host) = 0;
};

class DefaultHostListener
    : public HostListener
    , public RefCounted<DefaultHostListener> {
public:
  typedef SharedRefPtr<DefaultHostListener> Ptr;

  virtual void on_host_up(const Host::Ptr& host) {}
  virtual void on_host_down(const Host::Ptr& host) {}
  virtual void on_host_added(const Host::Ptr& host) {}
  virtual void on_host_removed(const Host::Ptr& host) {}
};

class ExternalHostListener : public DefaultHostListener {
public:
  typedef SharedRefPtr<ExternalHostListener> Ptr;
  ExternalHostListener(const CassHostListenerCallback callback, void* data);

  virtual void on_host_up(const Host::Ptr& host);
  virtual void on_host_down(const Host::Ptr& host);
  virtual void on_host_added(const Host::Ptr& host);
  virtual void on_host_removed(const Host::Ptr& host);

private:
  const CassHostListenerCallback callback_;
  void* data_;
};

typedef Map<Address, Host::Ptr> HostMap;

struct GetAddress {
  typedef std::pair<Address, Host::Ptr> Pair;
  const Address& operator()(const Pair& pair) const { return pair.first; }
};

struct GetHost {
  typedef std::pair<Address, Host::Ptr> Pair;
  Host::Ptr operator()(const Pair& pair) const { return pair.second; }
};

typedef std::pair<Address, Host::Ptr> HostPair;
typedef Vector<Host::Ptr> HostVec;
typedef CopyOnWritePtr<HostVec> CopyOnWriteHostVec;

void add_host(CopyOnWriteHostVec& hosts, const Host::Ptr& host);
void remove_host(CopyOnWriteHostVec& hosts, const Host::Ptr& host);
bool remove_host(CopyOnWriteHostVec& hosts, const Address& address);

}}} // namespace datastax::internal::core

#endif
