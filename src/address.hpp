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

#ifndef __CASS_ADDRESS_HPP_INCLUDED__
#define __CASS_ADDRESS_HPP_INCLUDED__

#include "common.hpp"

#include <uv.h>
#include <set>
#include <string.h>
#include <string>
#include <vector>

namespace cass {

class Address {
public:
  Address();

  Address(const std::string& ip, int port);


  static bool from_string(const std::string& ip, int port,
                          Address* output = NULL);

  static void from_inet(const char* data, size_t size, int port,
                              Address* output = NULL);

  bool init(const struct sockaddr* addr);

  const struct sockaddr* addr() const {
    return copy_cast<const struct sockaddr_storage*, const struct sockaddr*>(&addr_);
  }

  struct sockaddr_in* addr_in() {
    return copy_cast<struct sockaddr_storage*, struct sockaddr_in*>(&addr_);
  }

  const struct sockaddr_in* addr_in() const {
    return copy_cast<const struct sockaddr_storage*, const sockaddr_in*>(&addr_);
  }

  struct sockaddr_in6* addr_in6() {
    return copy_cast<struct sockaddr_storage*, sockaddr_in6*>(&addr_);
  }

  const struct sockaddr_in6* addr_in6() const {
    return copy_cast<const struct sockaddr_storage*, const sockaddr_in6*>(&addr_);
  }

  int family() const { return addr_.ss_family; }

  int port() const;

  std::string to_string(bool with_port = false) const;

  int compare(const Address& a) const;

private:
  void init() { memset(&addr_, 0, sizeof(addr_)); }

  struct sockaddr_storage addr_;
};

typedef std::vector<Address> AddressVec;
typedef std::set<Address> AddressSet;

inline bool operator<(const Address& a, const Address& b) {
  return a.compare(b) < 0;
}

inline bool operator==(const Address& a, const Address& b) {
  return a.compare(b) == 0;
}

inline std::ostream& operator<<(std::ostream& os, const Address& addr) {
  return os << addr.to_string();
}

} // namespace cass

#endif
