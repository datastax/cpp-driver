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

#include "cql_common.hpp"
#include "cql_address.hpp"

struct CqlHost {
  Address address;

  CqlHost() { }

  CqlHost(const Address& address)
    : address(address) { }
};

bool operator<(const CqlHost& a, const CqlHost& b) {
  return a.address < b.address;
}

bool operator==(const CqlHost& a, const CqlHost& b) {
  return a.address == b.address;
}

namespace std {
  template<>
  struct hash<CqlHost> {
    typedef CqlHost argument_type;
    typedef size_t result_type;
    size_t operator()(const CqlHost& h) const {
      std::hash<Address> hash_func;
      return hash_func(h.address);
    }
  };
}

#endif
