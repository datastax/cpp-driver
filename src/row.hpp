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

#ifndef DATASTAX_INTERNAL_ROW_HPP
#define DATASTAX_INTERNAL_ROW_HPP

#include "decoder.hpp"
#include "external.hpp"
#include "string_ref.hpp"
#include "value.hpp"

namespace datastax { namespace internal { namespace core {

class ResultResponse;

class Row {
public:
  Row()
      : result_(NULL) {}

  Row(const ResultResponse* result)
      : result_(result) {}

  OutputValueVec values;

  const Value* get_by_name(const StringRef& name) const;

  bool get_string_by_name(const StringRef& name, String* out) const;

  bool get_uuid_by_name(const StringRef& name, CassUuid* out) const;

  const ResultResponse* result() const { return result_; }

  void set_result(ResultResponse* result) { result_ = result; }

private:
  const ResultResponse* result_;
};

bool decode_row(Decoder& decoder, const ResultResponse* result, OutputValueVec& output);
bool decode_next_row(Decoder& decoder, OutputValueVec& output);

}}} // namespace datastax::internal::core

EXTERNAL_TYPE(datastax::internal::core::Row, CassRow)

#endif
