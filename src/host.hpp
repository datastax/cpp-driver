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

#ifndef __CASS_HOST_HPP_INCLUDED__
#define __CASS_HOST_HPP_INCLUDED__

#include "address.hpp"
#include "copy_on_write_ptr.hpp"
#include "ref_counted.hpp"

#include <boost/atomic.hpp>

#include <map>
#include <set>
#include <sstream>
#include <vector>

namespace cass {

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

private:
  HostState state() const {
    return state_.load(boost::memory_order_acquire);
  }

  void set_state(HostState state) {
    state_.store(state, boost::memory_order_release);
  }

  Address address_;
  bool mark_;
  boost::atomic<HostState> state_;
  std::string listen_address_;
  std::string rack_;
  std::string dc_;
};

typedef std::map<Address, SharedRefPtr<Host> > HostMap;
typedef std::vector<SharedRefPtr<Host> > HostVec;
typedef CopyOnWritePtr<HostVec> CopyOnWriteHostVec;

void add_host(CopyOnWriteHostVec& hosts, const SharedRefPtr<Host>& host);
void remove_host(CopyOnWriteHostVec& hosts, const SharedRefPtr<Host>& host);

} // namespace cass

#endif
