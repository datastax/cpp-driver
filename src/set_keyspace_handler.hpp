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

#ifndef __CASS_SET_KEYSPACE_HANDLER_HPP_INCLUDED__
#define __CASS_SET_KEYSPACE_HANDLER_HPP_INCLUDED__

#include "query_request.hpp"
#include "ref_counted.hpp"
#include "request_handler.hpp"
#include "scoped_ptr.hpp"

namespace cass {

class ResponseMessage;
class Connection;

class SetKeyspaceHandler : public Handler {
public:
  SetKeyspaceHandler(Connection* connection,
                     const std::string& keyspace,
                     RequestHandler* request_handler);

  virtual Request* request() const { return request_.get(); }

  virtual void on_set(ResponseMessage* response);

  virtual void on_error(CassError code, const std::string& message);

  virtual void on_timeout();

private:
  void on_result_response(ResponseMessage* response);

private:
  Connection* connection_;
  ScopedRefPtr<QueryRequest> request_;
  ScopedRefPtr<RequestHandler> request_handler_;
};

} // namespace cass

#endif
