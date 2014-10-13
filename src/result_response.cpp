/*
  Copyright (c) 2014 DataStax

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

#include "result_response.hpp"

#include "metadata.hpp"
#include "serialization.hpp"
#include "types.hpp"


extern "C" {

void cass_result_free(const CassResult* result) {
  delete result->from();
}

size_t cass_result_row_count(const CassResult* result) {
  if (result->kind() == CASS_RESULT_KIND_ROWS) {
    return result->row_count();
  }
  return 0;
}

size_t cass_result_column_count(const CassResult* result) {
  if (result->kind() == CASS_RESULT_KIND_ROWS) {
    return result->column_count();
  }
  return 0;
}

CassString cass_result_column_name(const CassResult* result, size_t index) {
  CassString str;
  if (result->kind() == CASS_RESULT_KIND_ROWS &&
      index < result->metadata()->column_count()) {
    const cass::ColumnDefinition def = result->metadata()->get(index);
    str.data = def.name;
    str.length = def.name_size;
  } else {
    str.data = "";
    str.length = 0;
  }
  return str;
}

CassValueType cass_result_column_type(const CassResult* result, size_t index) {
  if (result->kind() == CASS_RESULT_KIND_ROWS &&
      index < result->metadata()->column_count()) {
    return static_cast<CassValueType>(result->metadata()->get(index).type);
  }
  return CASS_VALUE_TYPE_UNKNOWN;
}

const CassRow* cass_result_first_row(const CassResult* result) {
  if (result->kind() == CASS_RESULT_KIND_ROWS && result->row_count() > 0) {
    return CassRow::to(&result->first_row());
  }
  return NULL;
}

cass_bool_t cass_result_has_more_pages(const CassResult* result) {
  return static_cast<cass_bool_t>(result->has_more_pages());
}

} // extern "C"

namespace cass {

size_t ResultResponse::find_column_indices(boost::string_ref name,
                                           Metadata::IndexVec* result) const {
  return metadata_->get(name, result);
}

bool ResultResponse::decode(int version, char* input, size_t size) {
  char* buffer = decode_int32(input, kind_);

  switch (kind_) {
    case CASS_RESULT_KIND_VOID:
      return true;
      break;

    case CASS_RESULT_KIND_ROWS:
      return decode_rows(buffer);
      break;

    case CASS_RESULT_KIND_SET_KEYSPACE:
      return decode_set_keyspace(buffer);
      break;

    case CASS_RESULT_KIND_PREPARED:
      return decode_prepared(version, buffer);
      break;

    case CASS_RESULT_KIND_SCHEMA_CHANGE:
      return decode_schema_change(buffer);
      break;

    default:
      assert(false);
  }
  return false;
}

char* ResultResponse::decode_metadata(char* input, ScopedRefPtr<Metadata>* metadata) {
  int32_t flags = 0;
  char* buffer = decode_int32(input, flags);

  int32_t column_count = 0;
  buffer = decode_int32(buffer, column_count);

  if (flags & CASS_RESULT_FLAG_HAS_MORE_PAGES) {
    has_more_pages_ = true;
    buffer = decode_bytes(buffer, &paging_state_, paging_state_size_);
  } else {
    has_more_pages_ = false;
  }

  if (!(flags & CASS_RESULT_FLAG_NO_METADATA)) {
    bool global_table_spec = flags & CASS_RESULT_FLAG_GLOBAL_TABLESPEC;

    if (global_table_spec) {
      buffer = decode_string(buffer, &keyspace_, keyspace_size_);
      buffer = decode_string(buffer, &table_, table_size_);
    }

    metadata->reset(new Metadata(column_count));

    for (int i = 0; i < column_count; ++i) {
      ColumnDefinition def;

      def.index = i;

      if (!global_table_spec) {
        buffer = decode_string(buffer, &def.keyspace, def.keyspace_size);
        buffer = decode_string(buffer, &def.table, def.table_size);
      }

      buffer = decode_string(buffer, &def.name, def.name_size);
      buffer = decode_option(buffer, def.type, &def.class_name,
                             def.class_name_size);

      if (def.type == CASS_VALUE_TYPE_SET ||
          def.type == CASS_VALUE_TYPE_LIST ||
          def.type == CASS_VALUE_TYPE_MAP) {
        buffer = decode_option(buffer, def.collection_primary_type,
                               &def.collection_primary_class,
                               def.collection_primary_class_size);
      }

      if (def.type == CASS_VALUE_TYPE_MAP) {
        buffer = decode_option(buffer, def.collection_secondary_type,
                               &def.collection_secondary_class,
                               def.collection_secondary_class_size);
      }

      (*metadata)->insert(def);
    }
  }
  return buffer;
}

void ResultResponse::decode_first_row() {
  if (row_count_ > 0) {
    first_row_.values.reserve(column_count());
    rows_ = decode_row(rows_, this, first_row_.values);
  }
}

bool ResultResponse::decode_rows(char* input) {
  char* buffer = decode_metadata(input, &metadata_);
  rows_ = decode_int32(buffer, row_count_);
  return true;
}

bool ResultResponse::decode_set_keyspace(char* input) {
  decode_string(input, &keyspace_, keyspace_size_);
  return true;
}

bool ResultResponse::decode_prepared(int version, char* input) {
  char* buffer = decode_string(input, &prepared_, prepared_size_);
  buffer = decode_metadata(buffer, &metadata_);
  if (version > 1) {
    decode_metadata(buffer, &result_metadata_);
  }
  return true;
}

bool ResultResponse::decode_schema_change(char* input) {
  char* buffer = decode_string(input, &change_, change_size_);
  buffer = decode_string(buffer, &keyspace_, keyspace_size_);
  buffer = decode_string(buffer, &table_, table_size_);
  return true;
}

} // namespace cass
