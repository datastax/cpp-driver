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

#ifndef __CASS_SESSION_FUTURE_HPP_INCLUDED__
#define __CASS_SESSION_FUTURE_HPP_INCLUDED__

#include "future.hpp"

namespace cass {

struct Session;
struct SessionFuture : public Future {
    SessionFuture()
      : Future(CASS_FUTURE_TYPE_SESSION) { }
    Session* session;
};



struct ShutdownSessionFuture : public SessionFuture {
    virtual ResultOrError* get();
    virtual void wait();
    virtual bool wait_for(size_t timeout);
};

} // namespace cass

#endif
