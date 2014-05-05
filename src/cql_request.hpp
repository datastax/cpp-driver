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

#ifndef __CQL_REQUEST_HPP_INCLUDED__
#define __CQL_REQUEST_HPP_INCLUDED__

#include <list>
#include <string>

#include "cql_host.hpp"

struct CqlRequest {
  CqlMessageFutureImpl*  future;
  CqlMessage*            message;
  std::list<CqlHost> hosts;
  std::list<std::string> hosts_attempted;

  CqlRequest() :
      future(nullptr),
      message(nullptr) {
  }

  CqlRequest(
      CqlMessageFutureImpl* future,
      CqlMessage*           message) :
      future(future),
      message(message) {
  }

 private:
  CqlRequest(const CqlRequest&) {}
  void operator=(const CqlRequest&) {}
};

#endif
