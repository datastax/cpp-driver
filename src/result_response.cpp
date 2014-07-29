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

#include "types.hpp"
#include "result_response.hpp"
#include "serialization.hpp"

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
      index < result->column_metadata().size()) {
    str.data = result->column_metadata()[index].name;
    str.length = result->column_metadata()[index].name_size;
  } else {
    str.data = "";
    str.length = 0;
  }
  return str;
}

CassValueType cass_result_column_type(const CassResult* result, size_t index) {
  if (result->kind() == CASS_RESULT_KIND_ROWS &&
      index < result->column_metadata().size()) {
    return static_cast<CassValueType>(result->column_metadata()[index].type);
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

size_t ResultResponse::find_column_indices(const std::string& name,
                                           size_t* result,
                                           size_t result_size) const {
  bool is_case_sensitive = false;
  ColumnMetadataIndex::const_iterator index_it;

  if (name.size() > 0 && name.front() == '"' && name.back() == '"') {
    is_case_sensitive = true;
    std::string sub = name.substr(1, name.size() - 2);
    index_it = column_metadata_index_.find(sub);
  } else {
    index_it = column_metadata_index_.find(name);
  }

  const ColumnIndexVec& indices = index_it->second;
  if (!is_case_sensitive || index_it == column_metadata_index_.end()) {
    std::copy_n(indices.begin(), std::min(indices.size(), result_size), result);
    return index_it->second.size();
  }

  size_t num_matches = 0;
  const size_t name_size = name.size() - 2;

  for (ColumnIndexVec::const_iterator it = indices.begin(),
       end = indices.end(); it != end; ++it) {
    const ColumnMetadata& cm = column_metadata_[*it];
    if (name.compare(1, name_size, cm.name, cm.class_name_size)) {
      if (num_matches < result_size) {
        result[num_matches] = *it;
      }
      num_matches++;
    }
  }

  return num_matches;
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
      return decode_prepared(buffer);
      break;
    case CASS_RESULT_KIND_SCHEMA_CHANGE:
      return decode_schema_change(buffer);
      break;
    default:
      assert(false);
  }
  return false;
}

char* ResultResponse::decode_metadata(char* input) {
  int32_t flags = 0;
  char* buffer = decode_int32(input, flags);
  buffer = decode_int32(buffer, column_count_);

  if (flags & CASS_RESULT_FLAG_HAS_MORE_PAGES) {
    has_more_pages_ = true;
    buffer = decode_bytes(buffer, &paging_state_, paging_state_size_);
  } else {
    has_more_pages_ = false;
  }

  if (flags & CASS_RESULT_FLAG_GLOBAL_TABLESPEC) {
    global_table_spec_ = true;
    buffer = decode_string(buffer, &keyspace_, keyspace_size_);
    buffer = decode_string(buffer, &table_, table_size_);
  } else {
    global_table_spec_ = false;
  }

  if (flags & CASS_RESULT_FLAG_NO_METADATA) {
    no_metadata_ = true;
  } else {
    no_metadata_ = false;
    column_metadata_.reserve(column_count_);

    for (int i = 0; i < column_count_; ++i) {
      ColumnMetadata meta;

      if (!global_table_spec_) {
        buffer = decode_string(buffer, &meta.keyspace, meta.keyspace_size);
        buffer = decode_string(buffer, &meta.table, meta.table_size);
      }

      buffer = decode_string(buffer, &meta.name, meta.name_size);
      buffer = decode_option(buffer, meta.type, &meta.class_name,
                             meta.class_name_size);

      if (meta.type == CASS_VALUE_TYPE_SET ||
          meta.type == CASS_VALUE_TYPE_LIST ||
          meta.type == CASS_VALUE_TYPE_MAP) {
        buffer = decode_option(buffer, meta.collection_primary_type,
                               &meta.collection_primary_class,
                               meta.collection_primary_class_size);
      }

      if (meta.type == CASS_VALUE_TYPE_MAP) {
        buffer = decode_option(buffer, meta.collection_secondary_type,
                               &meta.collection_secondary_class,
                               meta.collection_secondary_class_size);
      }

      column_metadata_.push_back(meta);
    }

    // Build index by column name
    for (size_t i = 0; i < column_metadata_.size(); ++i) {
      ColumnMetadata& metadata = column_metadata_[i];
      std::string name(metadata.name, metadata.name_size);
      column_metadata_index_[name].push_back(i);
    }
  }
  return buffer;
}

bool ResultResponse::decode_rows(char* input) {
  char* buffer = decode_metadata(input);
  rows_ = decode_int32(buffer, row_count_);
  if (row_count_ > 0) {
    first_row_.values.reserve(column_count_);
    rows_ = decode_row(rows_, this, first_row_.values);
  }
  return true;
}

bool ResultResponse::decode_set_keyspace(char* input) {
  decode_string(input, &keyspace_, keyspace_size_);
  return true;
}

bool ResultResponse::decode_prepared(char* input) {
  char* buffer = decode_string(input, &prepared_, prepared_size_);
  decode_metadata(buffer);
  return true;
}

bool ResultResponse::decode_schema_change(char* input) {
  char* buffer = decode_string(input, &change_, change_size_);
  buffer = decode_string(buffer, &keyspace_, keyspace_size_);
  buffer = decode_string(buffer, &table_, table_size_);
  return true;
}

} // namespace cass
