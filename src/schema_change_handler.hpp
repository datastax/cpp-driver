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

#ifndef __CASS_SCHEMA_CHANGE_HANDLER_HPP_INCLUDED__
#define __CASS_SCHEMA_CHANGE_HANDLER_HPP_INCLUDED__

#include "multiple_request_handler.hpp"
#include "ref_counted.hpp"
#include "request_handler.hpp"
#include "scoped_ptr.hpp"

#include "third_party/boost/boost/utility/string_ref.hpp"
#include "third_party/boost/boost/cstdint.hpp"

namespace cass {

class Connection;
class Logger;
class Response;

class SchemaChangeHandler : public MultipleRequestHandler {
public:
  SchemaChangeHandler(Connection* connection,
                      RequestHandler* request_handler,
                      Response* response,
                      uint64_t elapsed = 0);

  void execute();

  virtual void on_set(const ResponseVec& responses);
  virtual void on_error(CassError code, const std::string& message);
  virtual void on_timeout();
  void on_closing();

private:
  bool has_schema_agreement(const ResponseVec& responses);

  Logger* logger_;

  ScopedRefPtr<RequestHandler> request_handler_;
  Response* request_response_;
  uint64_t start_ms_;
  uint64_t elapsed_ms_;
};

} // namespace cass

#endif
