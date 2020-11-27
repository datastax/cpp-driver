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

#ifndef DATASTAX_INTERNAL_ADDRESS_HPP
#define DATASTAX_INTERNAL_ADDRESS_HPP

#include "allocated.hpp"
#include "callback.hpp"
#include "dense_hash_set.hpp"
#include "external.hpp"
#include "string.hpp"
#include "vector.hpp"

#include <uv.h>

namespace datastax { namespace internal { namespace core {

class Row;

class Address : public Allocated {
public:
  static const Address EMPTY_KEY;
  static const Address DELETED_KEY;

  enum Family { UNRESOLVED, IPv4, IPv6 };

#ifdef _WIN32
  struct SocketStorage {
    struct sockaddr* addr() {
      return reinterpret_cast<struct sockaddr*>(&storage);
    }
    struct sockaddr_in* addr_in() {
      return reinterpret_cast<struct sockaddr_in*>(&storage);
    }
    struct sockaddr_in6* addr_in6() {
      return reinterpret_cast<struct sockaddr_in6*>(&storage);
    }
    struct sockaddr_storage storage;
  };
#else
  struct SocketStorage {
    struct sockaddr* addr() {
      return &storage.addr;
    }
    struct sockaddr_in* addr_in() {
      return &storage.addr_in;
    }
    struct sockaddr_in6* addr_in6() {
      return &storage.addr_in6;
    }
    union {
      struct sockaddr addr;
      struct sockaddr_in addr_in;
      struct sockaddr_in6 addr_in6;
    } storage;
  };
#endif

  Address();
  Address(const Address& other, const String& server_name);
  Address(const String& hostname_or_address, int port, const String& server_name = String());
  Address(const uint8_t* address, uint8_t address_length, int port);
  Address(const struct sockaddr* addr);

  bool equals(const Address& other, bool with_port = true) const;

  bool operator==(const Address& other) const { return equals(other); }
  bool operator!=(const Address& other) const { return !equals(other); }
  bool operator<(const Address& other) const;

public:
  String hostname_or_address() const;
  const String& server_name() const { return server_name_; }
  Family family() const { return family_; }
  int port() const { return port_; }

  bool is_valid() const { return !hostname_or_address_.empty(); }
  bool is_resolved() const { return family_ == IPv4 || family_ == IPv6; }
  bool is_valid_and_resolved() const { return is_valid() && is_resolved(); }

public:
  size_t hash_code() const;
  uint8_t to_inet(void* address) const;
  const struct sockaddr* to_sockaddr(SocketStorage* storage) const;
  String to_string(bool with_port = false) const;

private:
  String hostname_or_address_;
  String server_name_;
  Family family_;
  int port_;
};

String determine_listen_address(const Address& address, const Row* row);

}}} // namespace datastax::internal::core

namespace std {

#if defined(HASH_IN_TR1) && !defined(_WIN32)
namespace tr1 {
#endif

template <>
struct hash<datastax::internal::core::Address> {
  size_t operator()(const datastax::internal::core::Address& address) const {
    return address.hash_code();
  }
};

template <>
struct hash<datastax::internal::core::Address::Family> {
  size_t operator()(datastax::internal::core::Address::Family family) const {
    return hasher(static_cast<int>(family));
  }
  SPARSEHASH_HASH<int> hasher;
};

#if defined(HASH_IN_TR1) && !defined(_WIN32)
} // namespace tr1
#endif

} // namespace std

namespace datastax { namespace internal { namespace core {

class AddressSet : public DenseHashSet<Address> {
public:
  AddressSet() {
    set_empty_key(Address::EMPTY_KEY);
    set_deleted_key(Address::DELETED_KEY);
  }
};
typedef Vector<Address> AddressVec;

}}} // namespace datastax::internal::core

namespace std {

inline std::ostream& operator<<(std::ostream& os, const datastax::internal::core::Address& a) {
  return os << a.to_string();
}

inline std::ostream& operator<<(std::ostream& os, const datastax::internal::core::AddressVec& v) {
  os << "[";
  bool first = true;
  for (datastax::internal::core::AddressVec::const_iterator it = v.begin(), end = v.end();
       it != end; ++it) {
    if (!first) os << ", ";
    first = false;
    os << *it;
  }
  os << "]";
  return os;
}

} // namespace std

EXTERNAL_TYPE(datastax::internal::core::Address, CassNode)

#endif
