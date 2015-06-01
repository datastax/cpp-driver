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

#include "result_response.hpp"

#include "external_types.hpp"
#include "result_metadata.hpp"
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

CassError cass_result_column_name(const CassResult* result,
                                  size_t index,
                                  const char** name,
                                  size_t* name_length) {
  if (result->kind() == CASS_RESULT_KIND_ROWS &&
      index < result->metadata()->column_count()) {
    const cass::ColumnDefinition def = result->metadata()->get_column_definition(index);
    *name = def.name.data();
    *name_length = def.name.size();
    return CASS_OK;
  }
  return CASS_ERROR_LIB_BAD_PARAMS;
}

CassValueType cass_result_column_type(const CassResult* result, size_t index) {
  if (result->kind() == CASS_RESULT_KIND_ROWS &&
      index < result->metadata()->column_count()) {
    return static_cast<CassValueType>(result->metadata()->get_column_definition(index).data_type->value_type());
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

class DataTypeDecoder {
public:
  DataTypeDecoder(char* input)
    : buffer_(input) { }

  char* buffer() const { return buffer_; }

  SharedRefPtr<DataType> decode() {
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
        if (value_type < CASS_VALUE_TYPE_LAST_ENTRY) {
          if (data_type_cache_[value_type]) {
            return data_type_cache_[value_type];
          } else {
            SharedRefPtr<DataType> data_type(
                  new DataType(static_cast<CassValueType>(value_type)));
            data_type_cache_[value_type] = data_type;
            return data_type;
          }
        }
        break;
    }

    return SharedRefPtr<DataType>();
  }

private:
  SharedRefPtr<DataType> decode_custom() {
    StringRef class_name;
    buffer_ = decode_string_ref(buffer_, &class_name);
    return SharedRefPtr<DataType>(new CustomType(class_name.to_string()));
  }

  SharedRefPtr<DataType> decode_collection(CassValueType collection_type) {
    DataTypeVec types;
    types.push_back(decode());
    if (collection_type == CASS_VALUE_TYPE_MAP) {
      types.push_back(decode());
    }
    return SharedRefPtr<DataType>(new CollectionType(collection_type, types));
  }

  SharedRefPtr<DataType> decode_user_type() {
    StringRef keyspace;
    buffer_ = decode_string_ref(buffer_, &keyspace);

    StringRef type_name;
    buffer_ = decode_string_ref(buffer_, &type_name);

    uint16_t n;
    buffer_ = decode_uint16(buffer_, n);

    UserType::FieldVec fields;
    for (uint16_t i = 0; i < n; ++i) {
      StringRef field_name;
      buffer_ = decode_string_ref(buffer_, &field_name);
      fields.push_back(UserType::Field(field_name.to_string(), decode()));
    }
    return SharedRefPtr<DataType>(new UserType(keyspace.to_string(),
                                               type_name.to_string(),
                                               fields));
  }

  SharedRefPtr<DataType> decode_tuple() {
    uint16_t n;
    buffer_ = decode_uint16(buffer_, n);

    DataTypeVec types;
    for (uint16_t i = 0; i < n; ++i) {
      types.push_back(decode());
    }
    return CollectionType::tuple(types);
  }

private:
  char* buffer_;
  SharedRefPtr<DataType> data_type_cache_[CASS_VALUE_TYPE_LAST_ENTRY];
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

char* ResultResponse::decode_metadata(char* input, SharedRefPtr<ResultMetadata>* metadata) {
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

    metadata->reset(new ResultMetadata(column_count));

    for (int i = 0; i < column_count; ++i) {
      ColumnDefinition def;

      def.index = i;

      if (!global_table_spec) {
        buffer = decode_string_ref(buffer, &def.keyspace);
        buffer = decode_string_ref(buffer, &def.table);
      }

      buffer = decode_string_ref(buffer, &def.name);

      DataTypeDecoder type_decoder(buffer);
      def.data_type = type_decoder.decode();
      buffer = type_decoder.buffer();

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
