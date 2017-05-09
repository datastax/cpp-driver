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

#ifndef __CASS_EXECUTE_REQUEST_HPP_INCLUDED__
#define __CASS_EXECUTE_REQUEST_HPP_INCLUDED__

#include <string>
#include <vector>

#include "statement.hpp"
#include "constants.hpp"
#include "prepared.hpp"
#include "ref_counted.hpp"

namespace cass {

class ExecuteRequest : public Statement {
public:
  ExecuteRequest(const Prepared* prepared)
      : Statement(prepared)
      , prepared_(prepared) {
      // If the prepared statement has result metadata then there is no
      // need to get the metadata with this request too.
      if (prepared->result()->result_metadata()) {
        set_skip_metadata(true);
      }
  }

  const Prepared::ConstPtr& prepared() const { return prepared_; }

  virtual int encode(int version, RequestCallback* callback, BufferVec* bufs) const;

  bool get_routing_key(std::string* routing_key, EncodingCache* cache)  const {
    return calculate_routing_key(prepared_->key_indices(), routing_key, cache);
  }

private:
  virtual size_t get_indices(StringRef name, IndexVec* indices) {
    return prepared_->result()->metadata()->get_indices(name, indices);
  }

  virtual const DataType::ConstPtr& get_type(size_t index) const {
    return prepared_->result()->metadata()->get_column_definition(index).data_type;
  }

private:
  Prepared::ConstPtr prepared_;
};

} // namespace cass

#endif
