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

#ifndef __CQL_HOST_HPP_INCLUDED__
#define __CQL_HOST_HPP_INCLUDED__

#include <string>

#define CQL_ADDRESS_MAX_LENGTH 46

enum CqlHostDistance {
  LOCAL,
  REMOTE,
  IGNORED
};

struct CqlHost {
  typedef std::function<void(CqlHost* host, CqlError* err)> ResolveCallback;

  struct sockaddr_in address;
  std::string        address_string;
  int                address_family;
  std::string        hostname;
  std::string        port;
  uv_getaddrinfo_t   resolver;
  struct addrinfo    resolver_hints;
  bool               resolved;
  ResolveCallback    resolve_callback;
  uv_loop_t*         resolve_loop;

  CqlHost() :
      resolved(false) {
    resolver.data = this;
    address_family = PF_INET;
    resolver_hints.ai_family = address_family;
    resolver_hints.ai_socktype = SOCK_STREAM;
    resolver_hints.ai_protocol = IPPROTO_TCP;
    resolver_hints.ai_flags = 0;
  }

  inline bool
  needs_resolve() {
    return resolved;
  }

  static void
  on_resolve(
      uv_getaddrinfo_t* resolver,
      int               status,
      struct addrinfo*  res) {
    CqlHost* host
        = reinterpret_cast<CqlHost*>(resolver->data);

    if (status == -1) {
      CqlError* err = new CqlError(
          CQL_ERROR_SOURCE_LIBRARY,
          CQL_ERROR_LIB_HOST_RESOLUTION,
          uv_err_name(uv_last_error(host->resolve_loop)),
          __FILE__,
          __LINE__);

      if (host->resolve_callback) {
        host->resolve_callback(host, err);
      }
      return;
    }

    host->resolved = true;

    // store the human readable address
    char address_string[CQL_ADDRESS_MAX_LENGTH];
    memset(address_string, 0, sizeof(address_string));

    if (res->ai_family == AF_INET) {
      uv_ip4_name((struct sockaddr_in*) res->ai_addr,
                  address_string,
                  CQL_ADDRESS_MAX_LENGTH);
    } else if (res->ai_family == AF_INET6) {
      uv_ip6_name((struct sockaddr_in6*) res->ai_addr,
                  address_string,
                  CQL_ADDRESS_MAX_LENGTH);
    }
    host->address_string.assign(address_string);

    host->address = *(struct sockaddr_in*) res->ai_addr;
    uv_freeaddrinfo(res);

    if (host->resolve_callback) {
      host->resolve_callback(host, nullptr);
    }
  }

  void
  resolve(
      uv_loop_t*      loop,
      ResolveCallback callback) {
    resolve_loop = loop;
    resolve_callback = callback;

    uv_getaddrinfo(
        loop,
        &resolver,
        CqlHost::on_resolve,
        hostname.c_str(),
        port.c_str(),
        &resolver_hints);
  }
};

#endif
