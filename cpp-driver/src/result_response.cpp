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

#include "result_response.hpp"

#include "external.hpp"
#include "result_metadata.hpp"
#include "serialization.hpp"

extern "C" {

void cass_result_free(const CassResult* result) {
  result->dec_ref();
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

CassError cass_result_column_name(const CassResult* result,
                                  size_t index,
                                  const char** name,
                                  size_t* name_length) {
  const cass::SharedRefPtr<cass::ResultMetadata>& metadata(result->metadata());
  if (index >= metadata->column_count()) {
    return CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS;
  }
  if (result->kind() != CASS_RESULT_KIND_ROWS) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  const cass::ColumnDefinition def = metadata->get_column_definition(index);
  *name = def.name.data();
  *name_length = def.name.size();
  return CASS_OK;
}

CassValueType cass_result_column_type(const CassResult* result, size_t index) {
  const cass::SharedRefPtr<cass::ResultMetadata>& metadata(result->metadata());
  if (result->kind() == CASS_RESULT_KIND_ROWS &&
      index < metadata->column_count()) {
    return metadata->get_column_definition(index).data_type->value_type();
  }
  return CASS_VALUE_TYPE_UNKNOWN;
}

const CassDataType* cass_result_column_data_type(const CassResult* result, size_t index) {
  const cass::SharedRefPtr<cass::ResultMetadata>& metadata(result->metadata());
  if (result->kind() == CASS_RESULT_KIND_ROWS &&
      index < metadata->column_count()) {
    return CassDataType::to(metadata->get_column_definition(index).data_type.get());
  }
  return NULL;
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

CassError cass_result_paging_state_token(const CassResult* result,
                                   const char** paging_state,
                                   size_t* paging_state_size) {
  if (!result->has_more_pages()) {
    return CASS_ERROR_LIB_NO_PAGING_STATE;
  }
  *paging_state = result->paging_state().data();
  *paging_state_size = result->paging_state().size();
  return CASS_OK;
}

} // extern "C"

namespace cass {

class DataTypeDecoder {
public:
  DataTypeDecoder(char* input, SimpleDataTypeCache& cache)
    : buffer_(input)
    , cache_(cache) { }

  char* buffer() const { return buffer_; }

  DataType::ConstPtr decode() {
    uint16_t value_type;
    buffer_ = decode_uint16(buffer_, value_type);

    switch (value_type) {
      case CASS_VALUE_TYPE_CUSTOM:
        return decode_custom();

      case CASS_VALUE_TYPE_LIST:
      case CASS_VALUE_TYPE_SET:
      case CASS_VALUE_TYPE_MAP:
        return decode_collection(static_cast<CassValueType>(value_type));

      case CASS_VALUE_TYPE_UDT:
        return decode_user_type();

      case CASS_VALUE_TYPE_TUPLE:
        return decode_tuple();

      default:
        return cache_.by_value_type(value_type);
    }

    return DataType::NIL;
  }

private:
  DataType::ConstPtr decode_custom() {
    StringRef class_name;
    buffer_ = decode_string(buffer_, &class_name);

    DataType::ConstPtr type = cache_.by_class(class_name);
    if (type) return type;

    // If no mapping exists, return an actual custom type.
    return DataType::ConstPtr(new CustomType(class_name.to_string()));
  }

  DataType::ConstPtr decode_collection(CassValueType collection_type) {
    DataType::Vec types;
    types.push_back(decode());
    if (collection_type == CASS_VALUE_TYPE_MAP) {
      types.push_back(decode());
    }
    return DataType::ConstPtr(new CollectionType(collection_type, types, false));
  }

  DataType::ConstPtr decode_user_type() {
    StringRef keyspace;
    buffer_ = decode_string(buffer_, &keyspace);

    StringRef type_name;
    buffer_ = decode_string(buffer_, &type_name);

    uint16_t n;
    buffer_ = decode_uint16(buffer_, n);

    UserType::FieldVec fields;
    for (uint16_t i = 0; i < n; ++i) {
      StringRef field_name;
      buffer_ = decode_string(buffer_, &field_name);
      fields.push_back(UserType::Field(field_name.to_string(), decode()));
    }
    return DataType::ConstPtr(new UserType(keyspace.to_string(),
                                           type_name.to_string(),
                                           fields,
                                           false));
  }

  DataType::ConstPtr decode_tuple() {
    uint16_t n;
    buffer_ = decode_uint16(buffer_, n);

    DataType::Vec types;
    for (uint16_t i = 0; i < n; ++i) {
      types.push_back(decode());
    }
    return DataType::ConstPtr(new TupleType(types, false));
  }

private:
  char* buffer_;
  SimpleDataTypeCache& cache_;
};

bool ResultResponse::decode(int version, char* input, size_t size) {
  protocol_version_ = version;

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

char* ResultResponse::decode_metadata(char* input, ResultMetadata::Ptr* metadata,
                                      bool has_pk_indices) {
  int32_t flags = 0;
  char* buffer = decode_int32(input, flags);

  int32_t column_count = 0;
  buffer = decode_int32(buffer, column_count);

  if (has_pk_indices) {
    int32_t pk_count = 0;
    buffer = decode_int32(buffer, pk_count);
    for (int i = 0; i < pk_count; ++i) {
      uint16_t pk_index = 0;
      buffer = decode_uint16(buffer, pk_index);
      pk_indices_.push_back(pk_index);
    }
  }

  if (flags & CASS_RESULT_FLAG_HAS_MORE_PAGES) {
    has_more_pages_ = true;
    buffer = decode_bytes(buffer, &paging_state_);
  } else {
    has_more_pages_ = false;
  }

  if (!(flags & CASS_RESULT_FLAG_NO_METADATA)) {
    bool global_table_spec = flags & CASS_RESULT_FLAG_GLOBAL_TABLESPEC;

    if (global_table_spec) {
      buffer = decode_string(buffer, &keyspace_);
      buffer = decode_string(buffer, &table_);
    }

    metadata->reset(new ResultMetadata(column_count));

    SimpleDataTypeCache cache;

    for (int i = 0; i < column_count; ++i) {
      ColumnDefinition def;

      def.index = i;

      if (!global_table_spec) {
        buffer = decode_string(buffer, &def.keyspace);
        buffer = decode_string(buffer, &def.table);
      }

      buffer = decode_string(buffer, &def.name);

      DataTypeDecoder type_decoder(buffer, cache);
      def.data_type = DataType::ConstPtr(type_decoder.decode());
      buffer = type_decoder.buffer();

      (*metadata)->add(def);
    }
  }
  return buffer;
}

void ResultResponse::decode_first_row() {
  if (row_count_ > 0 &&
      metadata_ && // Valid metadata required for column count
      first_row_.values.empty()) { // Only decode the first row once
    first_row_.values.reserve(column_count());
    rows_ = decode_row(rows_, this, first_row_.values);
  }
}

bool ResultResponse::decode_rows(char* input) {
  char* buffer = decode_metadata(input, &metadata_);
  rows_ = decode_int32(buffer, row_count_);
  decode_first_row();
  return true;
}

bool ResultResponse::decode_set_keyspace(char* input) {
  decode_string(input, &keyspace_);
  return true;
}

bool ResultResponse::decode_prepared(int version, char* input) {
  char* buffer = decode_string(input, &prepared_);
  buffer = decode_metadata(buffer, &metadata_, version >= 4);
  if (version > 1) {
    decode_metadata(buffer, &result_metadata_);
  }
  return true;
}

bool ResultResponse::decode_schema_change(char* input) {
  char* buffer = decode_string(input, &change_);
  buffer = decode_string(buffer, &keyspace_);
  buffer = decode_string(buffer, &table_);
  return true;
}

} // namespace cass
