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

#ifndef __CASS_REQUEST_FUTURE_HPP_INCLUDED__
#define __CASS_REQUEST_FUTURE_HPP_INCLUDED__

#include "message.hpp"

namespace cass {

class Timer;

struct RequestFuture : public Future {
    RequestFuture(Message* message)
      : Future(CASS_FUTURE_TYPE_REQUEST)
      , message(message)
      , timer(nullptr) { }

    ~RequestFuture() {
      message->body.release(); // This memory doesn't belong to us
      delete message;
    }

    Message* message;
    Timer* timer;
    std::list<Host> hosts;
    std::list<Host> hosts_attempted;
    std::string statement;
};

} // namespace cass

#endif
