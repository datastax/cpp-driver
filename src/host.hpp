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

#include <set>
#include <vector>

namespace cass {

class Host {
public:
  class StateListener {
  public:
    virtual void on_add(const Host& host) = 0;
    virtual void on_remove(const Host& host) = 0;
    virtual void on_up(const Host& host) = 0;
    virtual void on_down(const Host& host) = 0;
  };

  Host() {}

  Host(const Address& address)
      : address_(address)
      , is_up_(false) {}

  const Address& address() const { return address_; }

  bool is_up() const { return is_up_; }

  void set_up() { is_up_= true; }
  void set_down() { is_up_ = false; }

private:
  Address address_;
  bool is_up_;
};

typedef std::vector<Host> HostVec;
typedef std::set<Host> HostSet;

inline bool operator<(const Host& a, const Host& b) {
  return a.address() < b.address();
}

inline bool operator==(const Host& a, const Host& b) {
  return a.address() == b.address();
}

} // namespace cass

#endif
