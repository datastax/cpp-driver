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

#ifndef __CASS_RESULT_RESPONSE_HPP_INCLUDED__
#define __CASS_RESULT_RESPONSE_HPP_INCLUDED__

#include "response.hpp"
#include "constants.hpp"
#include "row.hpp"

#include <map>
#include <string>
#include <vector>

namespace cass {

class ResultIterator;

class ResultResponse : public Response {
public:
  struct ColumnMetaData {
    ColumnMetaData()
        : type(CASS_VALUE_TYPE_UNKNOWN)
        , keyspace(NULL)
        , keyspace_size(0)
        , table(NULL)
        , table_size(0)
        , name(NULL)
        , name_size(0)
        , class_name(NULL)
        , class_name_size(0)
        , collection_primary_type(CASS_VALUE_TYPE_UNKNOWN)
        , collection_primary_class(NULL)
        , collection_primary_class_size(0)
        , collection_secondary_type(CASS_VALUE_TYPE_UNKNOWN)
        , collection_secondary_class(NULL)
        , collection_secondary_class_size(0) {}

    uint16_t type;
    char* keyspace;
    size_t keyspace_size;

    char* table;
    size_t table_size;

    char* name;
    size_t name_size;

    char* class_name;
    size_t class_name_size;

    uint16_t collection_primary_type;
    char* collection_primary_class;

    size_t collection_primary_class_size;
    uint16_t collection_secondary_type;

    char* collection_secondary_class;
    size_t collection_secondary_class_size;
  };

  typedef std::vector<ColumnMetaData> MetaDataVec;
  typedef std::map<std::string, size_t> MetaDataIndex;

  ResultResponse()
      : Response(CQL_OPCODE_RESULT)
      , kind_(0)
      , more_pages_(false)
      , no_metadata_(false)
      , global_table_spec_(true)
      , column_count_(0)
      , page_state_(NULL)
      , page_state_size_(0)
      , prepared_(NULL)
      , prepared_size_(0)
      , change_(NULL)
      , change_size_(0)
      , keyspace_(NULL)
      , keyspace_size_(0)
      , table_(NULL)
      , table_size_(0)
      , row_count_(0)
      , rows_(NULL) {}

  int32_t kind() const { return kind_; }

  int32_t column_count() const { return column_count_; }

  const MetaDataVec& column_metadata() const { return column_metadata_; }

  std::string prepared() const {
    return std::string(prepared_, prepared_size_);
  }

  std::string keyspace() const {
    return std::string(keyspace_, keyspace_size_);
  }

  char* rows() const { return rows_; }

  int32_t row_count() const { return row_count_; }

  const Row& first_row() const { return first_row_; }

  bool consume(char* input, size_t size);

  char* decode_metadata(char* input);

  bool decode_rows(char* input);

  bool decode_set_keyspace(char* input);

  bool decode_prepared(char* input);

  bool decode_schema_change(char* input);

private:
  int32_t kind_;
  bool more_pages_; // row data
  bool no_metadata_;
  bool global_table_spec_;
  int32_t column_count_;
  MetaDataVec column_metadata_;
  MetaDataIndex column_index_;
  char* page_state_; // row paging
  size_t page_state_size_;
  char* prepared_; // prepared result
  size_t prepared_size_;
  char* change_; // schema change
  size_t change_size_;
  char* keyspace_; // rows, set keyspace, and schema change
  size_t keyspace_size_;
  char* table_; // rows, and schema change
  size_t table_size_;
  int32_t row_count_;
  char* rows_;
  Row first_row_;
};

} // namespace cass

#endif
