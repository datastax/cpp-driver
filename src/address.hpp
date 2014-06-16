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

#ifndef __CASS_ADDRESS_HPP_INCLUDED__
#define __CASS_ADDRESS_HPP_INCLUDED__

#include <uv.h>
#include <assert.h>
#include <string.h>

#include <string>
#include <sstream>

namespace cass {

class Address {
private:
  struct sockaddr_storage addr_;

public:
  Address() { memset(&addr_, 0, sizeof(addr_)); }

  static bool from_string(const std::string& ip, int port,
                          Address* output = nullptr) {
    char buf[sizeof(struct in6_addr)];
    if (uv_inet_pton(AF_INET, ip.c_str(), &buf).code == UV_OK) {
      if (output != nullptr) {
        struct sockaddr_in addr = uv_ip4_addr(ip.c_str(), port);
        output->init(reinterpret_cast<const struct sockaddr*>(&addr));
      }
      return true;
    } else if (uv_inet_pton(AF_INET6, ip.c_str(), &buf).code == UV_OK) {
      if (output != nullptr) {
        struct sockaddr_in6 addr = uv_ip6_addr(ip.c_str(), port);
        output->init(reinterpret_cast<const struct sockaddr*>(&addr));
      }
      return true;
    } else {
      return false;
    }
  }

  bool init(const struct sockaddr* addr) {
    if (addr->sa_family == AF_INET) {
      memcpy(&addr_, addr, sizeof(struct sockaddr_in));
      return true;
    } else if (addr->sa_family == AF_INET6) {
      memcpy(&addr_, addr, sizeof(struct sockaddr_in6));
      return true;
    }
    return false;
  }

  struct sockaddr* addr() {
    return reinterpret_cast<struct sockaddr*>(&addr_);
  }

  const struct sockaddr* addr() const {
    return reinterpret_cast<const struct sockaddr*>(&addr_);
  }

  struct sockaddr_in* addr_in() {
    return reinterpret_cast<sockaddr_in*>(&addr_);
  }

  const struct sockaddr_in* addr_in() const {
    return reinterpret_cast<const sockaddr_in*>(&addr_);
  }

  struct sockaddr_in6* addr_in6() {
    return reinterpret_cast<sockaddr_in6*>(&addr_);
  }

  const struct sockaddr_in6* addr_in6() const {
    return reinterpret_cast<const sockaddr_in6*>(&addr_);
  }

  int family() const { return addr()->sa_family; }

  int port() const {
    if (addr()->sa_family == AF_INET) {
      return htons(addr_in()->sin_port);
    } else if (addr()->sa_family == AF_INET6) {
      return htons(addr_in6()->sin6_port);
    } else {
      assert(false);
      return -1;
    }
  }

  std::string to_string() const {
    std::stringstream ss;
    char host[INET6_ADDRSTRLEN + 1] = {'\0'};
    if (addr()->sa_family == AF_INET) {
      uv_ip4_name(const_cast<struct sockaddr_in*>(addr_in()), host,
                  INET_ADDRSTRLEN);
      ss << host << ":" << port();
    } else if (addr()->sa_family == AF_INET6) {
      uv_ip6_name(const_cast<struct sockaddr_in6*>(addr_in6()), host,
                  INET6_ADDRSTRLEN);
      ss << "[" << host << "]:" << port();
    } else {
      assert(false);
    }
    return ss.str();
  }

  int compare(const Address& a) const {
    if (family() != a.family()) {
      return family() - a.family();
    }
    if (family() == AF_INET) {
      return (addr_in()->sin_addr.s_addr - a.addr_in()->sin_addr.s_addr);
    } else if (family() == AF_INET6) {
      return memcmp(&(addr_in6()->sin6_addr), &(a.addr_in6()->sin6_addr),
                    sizeof(addr_in6()->sin6_addr));
    } else {
      assert(false);
      return -1;
    }
  }
};

inline bool operator<(const Address& a, const Address& b) {
  return a.compare(b) < 0;
}

inline bool operator==(const Address& a, const Address& b) {
  return a.compare(b) == 0;
}

} // namespace cass

namespace std {

template <>
struct hash<cass::Address> {
  typedef cass::Address argument_type;
  typedef size_t result_type;
  size_t operator()(const cass::Address& a) const {
    if (a.family() == AF_INET) {
      return static_cast<size_t>(a.addr_in()->sin_addr.s_addr);
    } else if (a.family() == AF_INET6) {
      // TODO:(mpenick) fix this. maybe not ideal
      std::hash<std::string> hash_func;
      return hash_func(
          std::string(reinterpret_cast<const char*>(&(a.addr_in6()->sin6_addr)),
                      sizeof(a.addr_in6()->sin6_addr)));
    } else {
      assert(false);
    }
  }
};

} // namespace std

#endif
