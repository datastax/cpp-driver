/*
  Copyright (c) 2014-2016 DataStax

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

#ifndef __CASS_SCHEMA_CHANGE_CALLBACK_HPP_INCLUDED__
#define __CASS_SCHEMA_CHANGE_CALLBACK_HPP_INCLUDED__

#include "ref_counted.hpp"
#include "request_handler.hpp"
#include "scoped_ptr.hpp"
#include "string_ref.hpp"

#include <uv.h>

namespace cass {

class Connection;
class Response;

class SchemaChangeCallback : public MultipleRequestCallback {
public:
  typedef SharedRefPtr<SchemaChangeCallback> Ptr;

  SchemaChangeCallback(Connection* connection,
                      const SpeculativeExecution::Ptr& speculative_execution,
                      const Response::Ptr& response,
                      uint64_t elapsed = 0);

  void execute();

  virtual void on_set(const ResponseMap& responses);
  virtual void on_error(CassError code, const std::string& message);
  virtual void on_timeout();
  void on_closing();

private:
  bool has_schema_agreement(const ResponseMap& responses);

  SpeculativeExecution::Ptr speculative_execution_;
  Response::Ptr request_response_;
  uint64_t start_ms_;
  uint64_t elapsed_ms_;
};

} // namespace cass

#endif
