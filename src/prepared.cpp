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

#include "prepared.hpp"

#include "execute_request.hpp"
#include "logger.hpp"
#include "types.hpp"

extern "C" {

void cass_prepared_free(const CassPrepared* prepared) {
  prepared->dec_ref();
}

CassStatement* cass_prepared_bind(const CassPrepared* prepared) {
  cass::ExecuteRequest* execute
      = new cass::ExecuteRequest(prepared);
  execute->inc_ref();
  return CassStatement::to(execute);
}

} // extern "C"

namespace cass {

Prepared::Prepared(const ResultResponse* result,
                   const std::string& statement,
                   const std::vector<std::string>& key_columns)
      : result_(result)
      , id_(result->prepared())
      , statement_(statement) {
    ResultMetadata::IndexVec indices;
    // If the statement has bound parameters find the key indices
    if (result->column_count() > 0) {
      for (std::vector<std::string>::const_iterator i = key_columns.begin();
           i != key_columns.end(); ++i) {
        if (result->find_column_indices(StringRef(*i), &indices) > 0) {
          key_indices_.push_back(indices[0]);
        } else {
          LOG_WARN("Unable to find key column '%s' in prepared query", i->c_str());
          key_indices_.clear();
          break;
        }
      }
    }
  }
}
