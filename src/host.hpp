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

#include <sstream>
#include <vector>

namespace cass {

struct Host {
  Address address;
  std::string rack;
  std::string dc;

  Host() {}

  Host(const Address& address,
       const std::string& rack = "",
       const std::string& dc = "")
      : address(address),
        rack(rack),
        dc(dc) {}

  std::string to_string() const {
    std::ostringstream ss;
    ss << address.to_string();
    if (!rack.empty() || !dc.empty()) {
      ss << " [" << rack << ':' << dc << "]";
    }
    return ss.str();
  }
};

typedef std::vector<Host> HostVec;

inline bool operator<(const Host& a, const Host& b) {
  return a.address < b.address;
}

inline bool operator==(const Host& a, const Host& b) {
  return a.address == b.address;
}

} // namespace cass

#endif
