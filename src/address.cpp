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

#include <assert.h>
#include <string.h>

using namespace datastax;
using namespace datastax::internal::core;

const Address Address::EMPTY_KEY("0.0.0.0", 0);
const Address Address::DELETED_KEY("0.0.0.0", 1);

const Address Address::BIND_ANY_IPV4("0.0.0.0", 0);
const Address Address::BIND_ANY_IPV6("::", 0);

Address::Address() { memset(&addr_, 0, sizeof(addr_)); }

Address::Address(const String& ip, int port) {
  init();
  bool result = from_string(ip, port, this);
  UNUSED_(result);
  assert(result);
}

bool Address::from_string(const String& ip, int port, Address* output) {
  char buf[sizeof(struct in6_addr)];
  if (uv_inet_pton(AF_INET, ip.c_str(), &buf) == 0) {
    if (output != NULL) {
      struct sockaddr_in addr;
      uv_ip4_addr(ip.c_str(), port, &addr);
      output->init(&addr);
    }
    return true;
  } else if (uv_inet_pton(AF_INET6, ip.c_str(), &buf) == 0) {
    if (output != NULL) {
      struct sockaddr_in6 addr;
      uv_ip6_addr(ip.c_str(), port, &addr);
      output->init(&addr);
    }
    return true;
  } else {
    return false;
  }
}

bool Address::from_inet(const void* data, size_t size, int port, Address* output) {

  if (size == 4) {
    char buf[INET_ADDRSTRLEN];
    if (uv_inet_ntop(AF_INET, data, buf, sizeof(buf)) != 0) {
      return false;
    }
    if (output != NULL) {
      struct sockaddr_in addr;
      uv_ip4_addr(buf, port, &addr);
      output->init(&addr);
    }

    return true;
  } else if (size == 16) {
    char buf[INET6_ADDRSTRLEN];
    if (uv_inet_ntop(AF_INET6, data, buf, sizeof(buf)) != 0) {
      return false;
    }
    if (output != NULL) {
      struct sockaddr_in6 addr;
      uv_ip6_addr(buf, port, &addr);
      output->init(&addr);
    }

    return true;
  }
  return false;
}

bool Address::init(const sockaddr* addr) {
  if (addr->sa_family == AF_INET) {
    memcpy(addr_in(), addr, sizeof(struct sockaddr_in));
    return true;
  } else if (addr->sa_family == AF_INET6) {
    memcpy(addr_in6(), addr, sizeof(struct sockaddr_in6));
    return true;
  }
  return false;
}

void Address::init(const struct sockaddr_in* addr) { *addr_in() = *addr; }

void Address::init(const struct sockaddr_in6* addr) { *addr_in6() = *addr; }

int Address::port() const {
  if (family() == AF_INET) {
    return htons(addr_in()->sin_port);
  } else if (family() == AF_INET6) {
    return htons(addr_in6()->sin6_port);
  }
  return -1;
}

String Address::to_string(bool with_port) const {
  OStringStream ss;
  char host[INET6_ADDRSTRLEN + 1] = { '\0' };
  if (family() == AF_INET) {
    uv_ip4_name(const_cast<struct sockaddr_in*>(addr_in()), host, INET_ADDRSTRLEN);
    ss << host;
    if (with_port) ss << ":" << port();
  } else if (family() == AF_INET6) {
    uv_ip6_name(const_cast<struct sockaddr_in6*>(addr_in6()), host, INET6_ADDRSTRLEN);
    if (with_port) ss << "[";
    ss << host;
    if (with_port) ss << "]:" << port();
  }
  return ss.str();
}

uint8_t Address::to_inet(uint8_t* data) const {
  if (family() == AF_INET) {
    memcpy(data, &addr_in()->sin_addr, 4);
    return 4;
  } else if (family() == AF_INET6) {
    memcpy(data, &addr_in6()->sin6_addr, 16);
    return 16;
  }
  return 0;
}

int Address::compare(const Address& a, bool with_port) const {
  if (family() != a.family()) {
    return family() < a.family() ? -1 : 1;
  }
  if (with_port && port() != a.port()) {
    return port() < a.port() ? -1 : 1;
  }
  if (family() == AF_INET) {
    if (addr_in()->sin_addr.s_addr != a.addr_in()->sin_addr.s_addr) {
      return addr_in()->sin_addr.s_addr < a.addr_in()->sin_addr.s_addr ? -1 : 1;
    }
  } else if (family() == AF_INET6) {
    return memcmp(&(addr_in6()->sin6_addr), &(a.addr_in6()->sin6_addr),
                  sizeof(addr_in6()->sin6_addr));
  }
  return 0;
}

namespace datastax { namespace internal { namespace core {

bool determine_address_for_peer_host(const Address& connected_address, const Value* peer_value,
                                     const Value* rpc_value, Address* output) {
  Address peer_address;
  if (!peer_value ||
      !peer_value->decoder().as_inet(peer_value->size(), connected_address.port(), &peer_address)) {
    LOG_WARN("Invalid address format for peer address");
    return false;
  }
  if (rpc_value && !rpc_value->is_null()) {
    if (!rpc_value->decoder().as_inet(rpc_value->size(), connected_address.port(), output)) {
      LOG_WARN("Invalid address format for rpc address");
      return false;
    }
    if (connected_address == *output || connected_address == peer_address) {
      LOG_DEBUG("system.peers on %s contains a line with rpc_address for itself. "
                "This is not normal, but is a known problem for some versions of DSE. "
                "Ignoring this entry.",
                connected_address.to_string(false).c_str());
      return false;
    }
    if (Address::BIND_ANY_IPV4.compare(*output, false) == 0 ||
        Address::BIND_ANY_IPV6.compare(*output, false) == 0) {
      LOG_WARN("Found host with 'bind any' for rpc_address; using listen_address (%s) to contact "
               "instead. "
               "If this is incorrect you should configure a specific interface for rpc_address on "
               "the server.",
               peer_address.to_string(false).c_str());
      *output = peer_address;
    }
  } else {
    LOG_WARN("No rpc_address for host %s in system.peers on %s. "
             "Ignoring this entry.",
             peer_address.to_string(false).c_str(), connected_address.to_string(false).c_str());
    return false;
  }
  return true;
}

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
