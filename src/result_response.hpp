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

#ifndef DATASTAX_INTERNAL_RESULT_RESPONSE_HPP
#define DATASTAX_INTERNAL_RESULT_RESPONSE_HPP

#include "constants.hpp"
#include "data_type.hpp"
#include "macros.hpp"
#include "response.hpp"
#include "result_metadata.hpp"
#include "row.hpp"
#include "string_ref.hpp"
#include "vector.hpp"

namespace datastax { namespace internal { namespace core {

class ResultIterator;

class ResultResponse : public Response {
public:
  typedef SharedRefPtr<ResultResponse> Ptr;
  typedef SharedRefPtr<const ResultResponse> ConstPtr;
  typedef Vector<size_t> PKIndexVec;

  ResultResponse()
      : Response(CQL_OPCODE_RESULT)
      , kind_(CASS_RESULT_KIND_VOID)
      , has_more_pages_(false)
      , row_count_(0) {
    first_row_.set_result(this);
  }

  int32_t kind() const { return kind_; }

  ProtocolVersion protocol_version() const { return protocol_version_; }

  bool has_more_pages() const { return has_more_pages_; }

  int32_t column_count() const { return (metadata_ ? metadata_->column_count() : 0); }

  bool no_metadata() const { return !metadata_; }

  const ResultMetadata::Ptr& metadata() const { return metadata_; }

  void set_metadata(const ResultMetadata::Ptr& metadata);

  const ResultMetadata::Ptr& result_metadata() const { return result_metadata_; }

  StringRef paging_state() const { return paging_state_; }
  StringRef prepared_id() const { return prepared_id_; }
  StringRef result_metadata_id() const { return result_metadata_id_; }
  StringRef keyspace() const { return keyspace_; }
  StringRef table() const { return table_; }

  String quoted_keyspace() const {
    String temp(keyspace_.to_string());
    return escape_id(temp);
  }

  bool metadata_changed() { return new_metadata_id_.size() > 0; }
  StringRef new_metadata_id() const { return new_metadata_id_; }

  const Decoder& row_decoder() const { return row_decoder_; }

  int32_t row_count() const { return row_count_; }

  const Row& first_row() const { return first_row_; }

  const PKIndexVec& pk_indices() const { return pk_indices_; }

  virtual bool decode(Decoder& decoder);

private:
  bool decode_metadata(Decoder& decoder, ResultMetadata::Ptr* metadata,
                       bool has_pk_indices = false);

  bool decode_first_row();

  bool decode_rows(Decoder& decoder);

  bool decode_set_keyspace(Decoder& decoder);

  bool decode_prepared(Decoder& decoder);

  bool decode_schema_change(Decoder& decoder);

private:
  int32_t kind_;
  ProtocolVersion protocol_version_;
  bool has_more_pages_; // row data
  ResultMetadata::Ptr metadata_;
  ResultMetadata::Ptr result_metadata_;
  StringRef paging_state_;       // row paging
  StringRef prepared_id_;        // prepared result
  StringRef result_metadata_id_; // prepared result, protocol v5/DSEv2
  StringRef change_;             // schema change
  StringRef keyspace_;           // rows, set keyspace, and schema change
  StringRef table_;              // rows, and schema change
  StringRef new_metadata_id_;    // rows result, protocol v5/DSEv2
  int32_t row_count_;
  Decoder row_decoder_;
  Row first_row_;
  PKIndexVec pk_indices_;

private:
  DISALLOW_COPY_AND_ASSIGN(ResultResponse);
};

}}} // namespace datastax::internal::core

EXTERNAL_TYPE(datastax::internal::core::ResultResponse, CassResult)

#endif
