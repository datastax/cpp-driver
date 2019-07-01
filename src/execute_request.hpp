/*
  Copyright (c) DataStax, Inc.

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

#ifndef DATASTAX_INTERNAL_EXECUTE_REQUEST_HPP
#define DATASTAX_INTERNAL_EXECUTE_REQUEST_HPP

#include "constants.hpp"
#include "prepared.hpp"
#include "ref_counted.hpp"
#include "statement.hpp"
#include "string.hpp"
#include "vector.hpp"

namespace datastax { namespace internal { namespace core {

class ExecuteRequest : public Statement {
public:
  ExecuteRequest(const Prepared* prepared);

  const Prepared::ConstPtr& prepared() const { return prepared_; }

  virtual int encode(ProtocolVersion version, RequestCallback* callback, BufferVec* bufs) const;

  bool get_routing_key(String* routing_key) const {
    return calculate_routing_key(prepared_->key_indices(), routing_key);
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

}}} // namespace datastax::internal::core

#endif
