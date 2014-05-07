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

#ifndef __CASS_REQUEST_HPP_INCLUDED__
#define __CASS_REQUEST_HPP_INCLUDED__

#include <list>
#include <string>

#include "host.hpp"

namespace cass {

struct Request {
  MessageFutureImpl*  future;
  Message*            message;
  std::list<Host> hosts;
  std::list<std::string> hosts_attempted;

  Request() :
      future(nullptr),
      message(nullptr) {
  }

  Request(
      MessageFutureImpl* future,
      Message*           message) :
      future(future),
      message(message) {
  }

 private:
  Request(const Request&) {}
  void operator=(const Request&) {}
};

} // namespace cass

#endif
