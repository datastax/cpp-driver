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

#ifndef __CASS_MULTIPLE_REQUEST_HANDLER_HPP_INCLUDED__
#define __CASS_MULTIPLE_REQUEST_HANDLER_HPP_INCLUDED__

#include "handler.hpp"
#include "ref_counted.hpp"
#include "request.hpp"

#include <string>
#include <vector>

namespace cass {

class Connection;
class Response;

class MultipleRequestHandler : public RefCounted<MultipleRequestHandler> {
public:
  typedef std::vector<Response*> ResponseVec;

  MultipleRequestHandler(Connection* connection)
    : connection_(connection)
    , has_errors_or_timeouts_(false)
    , remaining_(0) {}

  virtual ~MultipleRequestHandler();

  void execute_query(const std::string& query);

  virtual void on_set(const ResponseVec& responses) = 0;
  virtual void on_error(CassError code, const std::string& message) = 0;
  virtual void on_timeout() = 0;

  Connection* connection() {
    return connection_;
  }

private:
  class InternalHandler : public Handler {
  public:
    InternalHandler(MultipleRequestHandler* parent, Request* request, int index)
      : parent_(parent)
      , request_(request)
      , index_(index) {}

    const Request* request() const {
      return request_.get();
    }

    virtual void on_set(ResponseMessage* response);
    virtual void on_error(CassError code, const std::string& message);
    virtual void on_timeout();

  private:
    ScopedRefPtr<MultipleRequestHandler> parent_;
    ScopedRefPtr<Request> request_;
    int index_;
  };

  Connection* connection_;
  bool has_errors_or_timeouts_;
  int remaining_;
  ResponseVec responses_;
};

} // namespace cass

#endif
