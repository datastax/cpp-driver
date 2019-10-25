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

#include "address.hpp"

#include "logger.hpp"
#include "macros.hpp"
#include "row.hpp"
#include "value.hpp"

using namespace datastax;
using namespace datastax::internal::core;

const Address Address::EMPTY_KEY(String(), 0);
const Address Address::DELETED_KEY(String(), 1);

namespace {

template <class T>
inline void hash_combine(std::size_t& seed, const T& v) {
  SPARSEHASH_HASH<T> hasher;
  seed ^= hasher(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
}

} // namespace

Address::Address()
    : family_(UNRESOLVED)
    , port_(0) {}

Address::Address(const Address& other, const String& server_name)
    : hostname_or_address_(other.hostname_or_address_)
    , server_name_(server_name)
    , family_(other.family_)
    , port_(other.port_) {}

Address::Address(const String& hostname, int port, const String& server_name)
    : server_name_(server_name)
    , family_(UNRESOLVED)
    , port_(port) {
  char addr[16];
  if (uv_inet_pton(AF_INET, hostname.c_str(), addr) == 0) {
    hostname_or_address_.assign(addr, addr + 4);
    family_ = IPv4;
  } else if (uv_inet_pton(AF_INET6, hostname.c_str(), addr) == 0) {
    hostname_or_address_.assign(addr, addr + 16);
    family_ = IPv6;
  } else {
    hostname_or_address_ = hostname;
  }
}

Address::Address(const uint8_t* address, uint8_t address_length, int port)
    : family_(UNRESOLVED)
    , port_(port) {
  if (address_length == 4) {
    hostname_or_address_.assign(reinterpret_cast<const char*>(address), address_length);
    family_ = IPv4;
  } else if (address_length == 16) {
    hostname_or_address_.assign(reinterpret_cast<const char*>(address), address_length);
    family_ = IPv6;
  }
}

Address::Address(const struct sockaddr* addr)
    : family_(UNRESOLVED)
    , port_(0) {
  if (addr->sa_family == AF_INET) {
    const struct sockaddr_in* addr_in = reinterpret_cast<const struct sockaddr_in*>(addr);
    hostname_or_address_.assign(reinterpret_cast<const char*>(&addr_in->sin_addr), 4);
    port_ = ntohs(addr_in->sin_port);
    family_ = IPv4;
  } else if (addr->sa_family == AF_INET6) {
    const struct sockaddr_in6* addr_in6 = reinterpret_cast<const struct sockaddr_in6*>(addr);
    hostname_or_address_.assign(reinterpret_cast<const char*>(&addr_in6->sin6_addr), 16);
    port_ = ntohs(addr_in6->sin6_port);
    family_ = IPv6;
  }
}

bool Address::equals(const Address& other, bool with_port) const {
  if (family_ != other.family_) return false;
  if (with_port && port_ != other.port_) return false;
  if (server_name_ != other.server_name_) return false;
  if (hostname_or_address_ != other.hostname_or_address_) return false;
  return true;
}

bool Address::operator<(const Address& other) const {
  if (family_ != other.family_) return family_ < other.family_;
  if (port_ != other.port_) return port_ < other.port_;
  if (server_name_ != other.server_name_) return server_name_ < other.server_name_;
  return hostname_or_address_ < other.hostname_or_address_;
}

String Address::hostname_or_address() const {
  if (family_ == IPv4) {
    char name[INET_ADDRSTRLEN + 1] = { '\0' };
    uv_inet_ntop(AF_INET, hostname_or_address_.data(), name, INET_ADDRSTRLEN);
    return name;
  } else if (family_ == IPv6) {
    char name[INET6_ADDRSTRLEN + 1] = { '\0' };
    uv_inet_ntop(AF_INET6, hostname_or_address_.data(), name, INET6_ADDRSTRLEN);
    return name;
  } else {
    return hostname_or_address_;
  }
}

size_t Address::hash_code() const {
  SPARSEHASH_HASH<Family> hasher;
  size_t code = hasher(family_);
  hash_combine(code, port_);
  hash_combine(code, server_name_);
  hash_combine(code, hostname_or_address_);
  return code;
}

uint8_t Address::to_inet(void* address) const {
  if (family_ == IPv4 || family_ == IPv6) {
    size_t size = hostname_or_address_.size();
    assert((size == 4 || size == 16) && "Invalid size for address");
    hostname_or_address_.copy(reinterpret_cast<char*>(address), size);
    return static_cast<uint8_t>(size);
  }
  return 0;
}

const struct sockaddr* Address::to_sockaddr(SocketStorage* storage) const {
  int rc = 0;
  if (family_ == IPv4) {
    char name[INET_ADDRSTRLEN + 1] = { '\0' };
    rc = uv_inet_ntop(AF_INET, hostname_or_address_.data(), name, INET_ADDRSTRLEN);
    if (rc != 0) return NULL;
    rc = uv_ip4_addr(name, port_, storage->addr_in());
  } else if (family_ == IPv6) {
    char name[INET6_ADDRSTRLEN + 1] = { '\0' };
    rc = uv_inet_ntop(AF_INET6, hostname_or_address_.data(), name, INET6_ADDRSTRLEN);
    if (rc != 0) return NULL;
    rc = uv_ip6_addr(name, port_, storage->addr_in6());
  } else {
    return NULL;
  }
  if (rc != 0) return NULL;
  return storage->addr();
}

String Address::to_string(bool with_port) const {
  OStringStream ss;
  if (family_ == IPv6 && with_port) {
    ss << "[" << hostname_or_address() << "]";
  } else {
    ss << hostname_or_address();
  }
  if (with_port) {
    ss << ":" << port_;
  }
  if (!server_name_.empty()) {
    ss << " (" << server_name_ << ")";
  }
  return ss.str();
}

namespace datastax { namespace internal { namespace core {

String determine_listen_address(const Address& address, const Row* row) {
  const Value* v = row->get_by_name("peer");
  if (v != NULL) {
    Address listen_address;
    if (v->decoder().as_inet(v->size(), address.port(), &listen_address)) {
      return listen_address.to_string();
    } else {
      LOG_WARN("Invalid address format for listen address for host %s",
               address.to_string().c_str());
    }
  }
  return "";
}

}}} // namespace datastax::internal::core
