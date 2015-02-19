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

#include "address.hpp"

#include <assert.h>
#include <sstream>

namespace cass {

Address::Address() {
  init();
}

Address::Address(const std::string& ip, int port) {
  init();
  from_string(ip, port, this);
}

bool Address::from_string(const std::string& ip, int port, Address* output) {
  char buf[sizeof(struct in6_addr)];
#if UV_VERSION_MAJOR == 0
  if (uv_inet_pton(AF_INET, ip.c_str(), &buf).code == UV_OK) {
#else
  if (uv_inet_pton(AF_INET, ip.c_str(), &buf) == 0) {
#endif
    if (output != NULL) {
      struct sockaddr_in addr;
#if UV_VERSION_MAJOR == 0
      addr = uv_ip4_addr(ip.c_str(), port);
#else
      uv_ip4_addr(ip.c_str(), port, &addr);
#endif
      output->init(copy_cast<struct sockaddr_in*, struct sockaddr*>(&addr));
    }
    return true;
#if UV_VERSION_MAJOR == 0
  } else if (uv_inet_pton(AF_INET6, ip.c_str(), &buf).code == UV_OK) {
#else
  } else if (uv_inet_pton(AF_INET6, ip.c_str(), &buf) == 0) {
#endif
    if (output != NULL) {
      struct sockaddr_in6 addr;
#if UV_VERSION_MAJOR == 0
      addr = uv_ip6_addr(ip.c_str(), port);
#else
      uv_ip6_addr(ip.c_str(), port, &addr);
#endif
      output->init(copy_cast<struct sockaddr_in6*, struct sockaddr*>(&addr));
    }
    return true;
  } else {
    return false;
  }
}

void Address::from_inet(const char* data, size_t size, int port, Address* output) {

  assert(size == 4 || size == 16);
  if (size == 4) {
    char buf[INET_ADDRSTRLEN];
    uv_inet_ntop(AF_INET, data, buf, sizeof(buf));
    if (output != NULL) {
      struct sockaddr_in addr;
#if UV_VERSION_MAJOR == 0
      addr = uv_ip4_addr(buf, port);
#else
      uv_ip4_addr(buf, port, &addr);
#endif
      output->init(copy_cast<struct sockaddr_in*, struct sockaddr*>(&addr));
    }
  } else {
    char buf[INET6_ADDRSTRLEN];
    uv_inet_ntop(AF_INET6, data, buf, sizeof(buf));
    if (output != NULL) {
      struct sockaddr_in6 addr;
#if UV_VERSION_MAJOR == 0
      addr = uv_ip6_addr(buf, port);
#else
      uv_ip6_addr(buf, port, &addr);
#endif
      output->init(copy_cast<struct sockaddr_in6*, struct sockaddr*>(&addr));
    }
  }
}

bool Address::init(const sockaddr* addr) {
  if (addr->sa_family == AF_INET) {
    memcpy(&addr_, addr, sizeof(struct sockaddr_in));
    return true;
  } else if (addr->sa_family == AF_INET6) {
    memcpy(&addr_, addr, sizeof(struct sockaddr_in6));
    return true;
  }
  return false;
}

int Address::port() const {
  if (family() == AF_INET) {
    return htons(addr_in()->sin_port);
  } else if (family() == AF_INET6) {
    return htons(addr_in6()->sin6_port);
  } else {
    assert(false);
    return -1;
  }
}

std::string Address::to_string(bool with_port) const {
  std::stringstream ss;
  char host[INET6_ADDRSTRLEN + 1] = {'\0'};
  if (family() == AF_INET) {
    uv_ip4_name(const_cast<struct sockaddr_in*>(addr_in()), host,
                INET_ADDRSTRLEN);
    ss << host;
    if (with_port) ss << ":" << port();
  } else if (family() == AF_INET6) {
    uv_ip6_name(const_cast<struct sockaddr_in6*>(addr_in6()), host,
                INET6_ADDRSTRLEN);
    if (with_port) ss << "[";
    ss << host;
    if (with_port) ss << "]:" << port();
  } else {
    assert(false);
  }
  return ss.str();
}

int Address::compare(const Address& a) const {
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

}
