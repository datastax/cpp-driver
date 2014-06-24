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

#ifndef __CASS_BATCH_REQUEST_HPP_INCLUDED__
#define __CASS_BATCH_REQUEST_HPP_INCLUDED__

#include "request.hpp"
#include "constants.hpp"

#include <list>
#include <map>
#include <string>

#define CASS_QUERY_FLAG_VALUES 0x01
#define CASS_QUERY_FLAG_SKIP_METADATA 0x02
#define CASS_QUERY_FLAG_PAGE_SIZE 0x04
#define CASS_QUERY_FLAG_PAGING_STATE 0x08
#define CASS_QUERY_FLAG_SERIAL_CONSISTENCY 0x10

namespace cass {

class Statement;
class ExecuteRequest;

class BatchRequest : public Request {
public:
  BatchRequest(size_t consistency, uint8_t type_)
      : Request(CQL_OPCODE_BATCH)
      , type_(type_)
      , consistency_(consistency) {}

  ~BatchRequest();

  void add_statement(Statement* statement);

  bool prepared_statement(const std::string& id, std::string* statement);

  bool prepare(size_t reserved, char** output, size_t& size);

private:
  typedef std::list<Statement*> StatementList;
  typedef std::map<std::string, ExecuteRequest*> PreparedMap;

  uint8_t type_;
  StatementList statements_;
  PreparedMap prepared_statements_;
  int16_t consistency_;
};

} // namespace cass

#endif
