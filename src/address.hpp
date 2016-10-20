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

#ifndef __CASS_ADDRESS_HPP_INCLUDED__
#define __CASS_ADDRESS_HPP_INCLUDED__

#include "hash.hpp"

#include <sparsehash/dense_hash_set>

#include <ostream>
#include <string.h>
#include <string>
#include <uv.h>
#include <vector>

namespace cass {

class Address {
public:
  static const Address EMPTY_KEY;
  static const Address DELETED_KEY;

  static const Address BIND_ANY_IPV4;
  static const Address BIND_ANY_IPV6;

  Address();
  Address(const std::string& ip, int port); // Tests only

  static bool from_string(const std::string& ip, int port,
                          Address* output = NULL);

  static bool from_inet(const char* data, size_t size, int port,
                        Address* output = NULL);

  bool init(const struct sockaddr* addr);

#ifdef _WIN32
  const struct sockaddr* addr() const { return reinterpret_cast<const struct sockaddr*>(&addr_); }
  const struct sockaddr_in* addr_in() const { return reinterpret_cast<const struct sockaddr_in*>(&addr_); }
  const struct sockaddr_in6* addr_in6() const { return reinterpret_cast<const struct sockaddr_in6*>(&addr_); }
#else
  const struct sockaddr* addr() const { return &addr_; }
  const struct sockaddr_in* addr_in() const { return &addr_in_; }
  const struct sockaddr_in6* addr_in6() const { return &addr_in6_; }
#endif

  bool is_valid() const { return family() == AF_INET || family() == AF_INET6; }
  int family() const { return addr()->sa_family; }
  int port() const;

  std::string to_string(bool with_port = false) const;
  uint8_t to_inet(uint8_t* data) const;

  int compare(const Address& a, bool with_port = true) const;

private:
  void init() { addr()->sa_family = AF_UNSPEC; }
  void init(const struct sockaddr_in* addr);
  void init(const struct sockaddr_in6* addr);

#ifdef _WIN32
  struct sockaddr* addr() { return reinterpret_cast<struct sockaddr*>(&addr_); }
  struct sockaddr_in* addr_in() { return reinterpret_cast<struct sockaddr_in*>(&addr_); }
  struct sockaddr_in6* addr_in6() { return reinterpret_cast<struct sockaddr_in6*>(&addr_); }

  struct sockaddr_storage addr_;
#else
  struct sockaddr* addr() { return &addr_; }
  struct sockaddr_in* addr_in() { return &addr_in_; }
  struct sockaddr_in6* addr_in6() { return &addr_in6_; }

  union {
    struct sockaddr addr_;
    struct sockaddr_in addr_in_;
    struct sockaddr_in6 addr_in6_;
  };
#endif
};

struct AddressHash {
  std::size_t operator()(const cass::Address& a) const {
    if (a.family() == AF_INET) {
      return cass::hash::fnv1a(reinterpret_cast<const char*>(a.addr()),
                               sizeof(struct sockaddr_in));
    } else if (a.family() == AF_INET6) {
      return cass::hash::fnv1a(reinterpret_cast<const char*>(a.addr()),
                               sizeof(struct sockaddr_in6));
    }
    return 0;
  }
};

typedef std::vector<Address> AddressVec;
typedef sparsehash::dense_hash_set<Address, AddressHash> AddressSet;

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
