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

#include "statement.hpp"

#include "execute_request.hpp"
#include "result_metadata.hpp"
#include "prepared.hpp"
#include "query_request.hpp"
#include "scoped_ptr.hpp"
#include "types.hpp"

#include <boost/cstdint.hpp>

namespace cass {

  template<class T>
  struct IsValidValueType;

  template<>
  struct IsValidValueType<CassNull> {
    bool operator()(uint16_t type) const {
      return true;
    }
  };

  template<>
  struct IsValidValueType<cass_int32_t> {
    bool operator()(uint16_t type) const {
      return type == CASS_VALUE_TYPE_INT;
    }
  };

  template<>
  struct IsValidValueType<cass_int64_t> {
    bool operator()(uint16_t type) const {
      return type == CASS_VALUE_TYPE_BIGINT ||
          type == CASS_VALUE_TYPE_COUNTER ||
          type == CASS_VALUE_TYPE_TIMESTAMP;
    }
  };

  template<>
  struct IsValidValueType<cass_float_t> {
    bool operator()(uint16_t type) const {
      return type == CASS_VALUE_TYPE_FLOAT;
    }
  };

  template<>
  struct IsValidValueType<cass_double_t> {
    bool operator()(uint16_t type) const {
      return type == CASS_VALUE_TYPE_DOUBLE;
    }
  };

  template<>
  struct IsValidValueType<bool> {
    bool operator()(uint16_t type) const {
      return type == CASS_VALUE_TYPE_BOOLEAN;
    }
  };

  template<>
  struct IsValidValueType<CassString> {
    bool operator()(uint16_t type) const {
      return type == CASS_VALUE_TYPE_ASCII ||
          type == CASS_VALUE_TYPE_TEXT ||
          type == CASS_VALUE_TYPE_VARCHAR;
    }
  };

  template<>
  struct IsValidValueType<CassBytes> {
    bool operator()(uint16_t type) const {
      return type == CASS_VALUE_TYPE_BLOB;
    }
  };

  template<>
  struct IsValidValueType<CassUuid> {
    bool operator()(uint16_t type) const {
      return type == CASS_VALUE_TYPE_TIMEUUID ||
          type == CASS_VALUE_TYPE_UUID;
    }
  };

  template<>
  struct IsValidValueType<CassInet> {
    bool operator()(uint16_t type) const {
      return type == CASS_VALUE_TYPE_INET;
    }
  };

  template<>
  struct IsValidValueType<CassDecimal> {
    bool operator()(uint16_t type) const {
      return type == CASS_VALUE_TYPE_DECIMAL;
    }
  };

  template<>
  struct IsValidValueType<const CassCollection*> {
    bool operator()(uint16_t type) const {
      // TODO(mpenick): Check actual type against collection
      return type == CASS_VALUE_TYPE_LIST ||
          type == CASS_VALUE_TYPE_MAP ||
          type == CASS_VALUE_TYPE_SET;
    }
  };

  template<>
  struct IsValidValueType<CassCustom> {
    bool operator()(uint16_t type) const {
      return true;
    }
  };

  template<class T>
  CassError bind_by_name(cass::Statement* statement,
                         boost::string_ref name,
                         T value) {
    if (statement->opcode() != CQL_OPCODE_EXECUTE) {
      return CASS_ERROR_LIB_INVALID_STATEMENT_TYPE;
    }

    const cass::ResultResponse* result
        = static_cast<cass::ExecuteRequest*>(statement)->prepared()->result().get();

    cass::ResultMetadata::IndexVec indices;
    result->find_column_indices(name, &indices);
    IsValidValueType<T> is_valid_type;

    if (indices.empty()) {
      return CASS_ERROR_LIB_NAME_DOES_NOT_EXIST;
    }

    for (cass::ResultMetadata::IndexVec::const_iterator it = indices.begin(),
         end = indices.end(); it != end; ++it) {
      size_t index = *it;
      if (!is_valid_type(result->metadata()->get(index).type)) {
        return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
      }
      statement->bind(index, value);
    }

    return CASS_OK;
  }

} // namespace cass

extern "C" {

CassStatement* cass_statement_new(const char* query, size_t parameter_count) {
  return cass_statement_new_n(query, strlen(query), parameter_count);
}

CassStatement* cass_statement_new_n(const char* query,
                                    size_t query_length,
                                    size_t parameter_count) {
  cass::QueryRequest* query_request = new cass::QueryRequest(parameter_count);
  query_request->inc_ref();
  query_request->set_query(query, query_length);
  return CassStatement::to(query_request);
}


CassError cass_statement_add_key_index(CassStatement* statement, size_t index) {
  if (statement->kind() != CASS_BATCH_KIND_QUERY) return CASS_ERROR_LIB_BAD_PARAMS;
  if (index >= statement->values_count()) return CASS_ERROR_LIB_BAD_PARAMS;
  statement->add_key_index(index);
  return CASS_OK;
}

CassError cass_statement_set_keyspace(CassStatement* statement, const char* keyspace) {
  return cass_statement_set_keyspace_n(statement, keyspace, strlen(keyspace));
}

CassError cass_statement_set_keyspace_n(CassStatement* statement,
                                        const char* keyspace,
                                        size_t keyspace_length) {
  if (statement->kind() != CASS_BATCH_KIND_QUERY) return CASS_ERROR_LIB_BAD_PARAMS;
  statement->set_keyspace(std::string(keyspace, keyspace_length));
  return CASS_OK;
}

void cass_statement_free(CassStatement* statement) {
  statement->dec_ref();
}

CassError cass_statement_set_consistency(CassStatement* statement,
                                         CassConsistency consistency) {
  statement->set_consistency(consistency);
  return CASS_OK;
}

CassError cass_statement_set_serial_consistency(CassStatement* statement,
                                                CassConsistency serial_consistency) {
  statement->set_serial_consistency(serial_consistency);
  return CASS_OK;
}

CassError cass_statement_set_paging_size(CassStatement* statement,
                                         int page_size) {
  statement->set_page_size(page_size);
  return CASS_OK;
}

CassError cass_statement_set_paging_state(CassStatement* statement,
                                          const CassResult* result) {
  statement->set_paging_state(result->paging_state());
  return CASS_OK;
}

CassError cass_statement_bind_null(CassStatement* statement,
                                   size_t index) {
  return statement->bind(index, cass::CassNull());
}

CassError cass_statement_bind_int32(CassStatement* statement, size_t index,
                                    cass_int32_t value) {
  return statement->bind(index, value);
}

CassError cass_statement_bind_int64(CassStatement* statement, size_t index,
                                    cass_int64_t value) {
  return statement->bind(index, value);
}

CassError cass_statement_bind_float(CassStatement* statement, size_t index,
                                    cass_float_t value) {
  return statement->bind(index, value);
}

CassError cass_statement_bind_double(CassStatement* statement, size_t index,
                                     cass_double_t value) {
  return statement->bind(index, value);
}

CassError cass_statement_bind_bool(CassStatement* statement, size_t index,
                                   cass_bool_t value) {
  return statement->bind(index, value == cass_true);
}

CassError cass_statement_bind_string(CassStatement* statement, size_t index,
                                     const char* value) {
  return cass_statement_bind_string_n(statement, index,
                                      value, strlen(value));
}

CassError cass_statement_bind_string_n(CassStatement* statement, size_t index,
                                       const char* value, size_t value_length) {
  cass::CassString s = { value, value_length };
  return statement->bind(index, s);
}

CassError cass_statement_bind_bytes(CassStatement* statement, size_t index,
                                    const cass_byte_t* value,
                                    size_t value_size) {
  cass::CassBytes b = { value, value_size };
  return statement->bind(index, b);
}

CassError cass_statement_bind_uuid(CassStatement* statement, size_t index,
                                   CassUuid value) {
  return statement->bind(index, value);
}

CassError cass_statement_bind_inet(CassStatement* statement, size_t index,
                                   CassInet value) {
  return statement->bind(index, value);
}

CassError cass_statement_bind_decimal(CassStatement* statement,
                                      size_t index,
                                      const cass_byte_t* varint,
                                      size_t varint_size,
                                      cass_int32_t scale) {
  return statement->bind(index, varint, varint_size, scale);
}

CassError cass_statement_bind_collection(CassStatement* statement, size_t index,
                                         const CassCollection* collection) {
  return statement->bind(index, collection->from());
}

CassError cass_statement_bind_custom(CassStatement* statement,
                                     size_t index, size_t size,
                                     cass_byte_t** output) {
  cass::CassCustom c = { output, size };
  return statement->bind(index, c);
}

CassError cass_statement_bind_null_by_name(CassStatement* statement,
                                           const char* name) {
  return cass_statement_bind_null_by_name_n(statement,
                                            name, strlen(name));
}

CassError cass_statement_bind_null_by_name_n(CassStatement* statement,
                                             const char* name,
                                             size_t name_length) {
  return cass::bind_by_name<cass::CassNull>(statement,
                                      boost::string_ref(name, name_length),
                                      cass::CassNull());
}

CassError cass_statement_bind_int32_by_name(CassStatement* statement,
                                            const char* name,
                                            cass_int32_t value) {
  return cass_statement_bind_int32_by_name_n(statement,
                                             name, strlen(name),
                                             value);
}

CassError cass_statement_bind_int32_by_name_n(CassStatement* statement,
                                              const char* name,
                                              size_t name_length,
                                              cass_int32_t value) {
  return cass::bind_by_name<cass_int32_t>(statement, boost::string_ref(name, name_length), value);
}

CassError cass_statement_bind_int64_by_name(CassStatement* statement,
                                            const char* name,
                                            cass_int64_t value) {
  return cass_statement_bind_int64_by_name_n(statement,
                                             name, strlen(name),
                                             value);
}

CassError cass_statement_bind_int64_by_name_n(CassStatement* statement,
                                              const char* name,
                                              size_t name_length,
                                              cass_int64_t value) {
  return cass::bind_by_name<cass_int64_t>(statement, boost::string_ref(name, name_length), value);
}

CassError cass_statement_bind_float_by_name(CassStatement* statement,
                                            const char* name,
                                            cass_float_t value) {
  return cass_statement_bind_float_by_name_n(statement,
                                             name, strlen(name),
                                             value);
}

CassError cass_statement_bind_float_by_name_n(CassStatement* statement,
                                              const char* name,
                                              size_t name_length,
                                              cass_float_t value) {
  return cass::bind_by_name<cass_float_t>(statement, boost::string_ref(name, name_length), value);
}

CassError cass_statement_bind_double_by_name(CassStatement* statement,
                                             const char* name,
                                             cass_double_t value) {
  return cass_statement_bind_double_by_name_n(statement,
                                              name, strlen(name),
                                              value);
}

CassError cass_statement_bind_double_by_name_n(CassStatement* statement,
                                               const char* name,
                                               size_t name_length,
                                               cass_double_t value) {
  return cass::bind_by_name<cass_double_t>(statement, boost::string_ref(name, name_length), value);
}

CassError cass_statement_bind_bool_by_name(CassStatement* statement,
                                           const char* name,
                                           cass_bool_t value) {
  return cass_statement_bind_bool_by_name_n(statement,
                                            name, strlen(name),
                                            value);
}

CassError cass_statement_bind_bool_by_name_n(CassStatement* statement,
                                             const char* name,
                                             size_t name_length,
                                             cass_bool_t value) {
  return cass::bind_by_name<bool>(statement, boost::string_ref(name, name_length), value == cass_true);
}


CassError cass_statement_bind_string_by_name(CassStatement* statement,
                                             const char* name,
                                             const char* value) {
  return cass_statement_bind_string_by_name_n(statement,
                                              name, strlen(name),
                                              value, strlen(value));
}

CassError cass_statement_bind_string_by_name_n(CassStatement* statement,
                                               const char* name,
                                               size_t name_length,
                                               const char* value,
                                               size_t value_length) {
  cass::CassString s = { value, value_length };
  return cass::bind_by_name<cass::CassString>(statement, boost::string_ref(name, name_length), s);
}

CassError cass_statement_bind_bytes_by_name(CassStatement* statement,
                                            const char* name,
                                            cass_byte_t* value,
                                            size_t value_size) {
  return cass_statement_bind_bytes_by_name_n(statement,
                                             name, strlen(name),
                                             value, value_size);
}

CassError cass_statement_bind_bytes_by_name_n(CassStatement* statement,
                                              const char* name,
                                              size_t name_length,
                                              cass_byte_t* value,
                                              size_t value_size) {
  cass::CassBytes b = { value, value_size };
  return cass::bind_by_name<cass::CassBytes>(statement, boost::string_ref(name, name_length), b);
}


CassError cass_statement_bind_uuid_by_name(CassStatement* statement,
                                           const char* name,
                                           CassUuid value) {
  return cass_statement_bind_uuid_by_name_n(statement,
                                            name, strlen(name),
                                            value);
}

CassError cass_statement_bind_uuid_by_name_n(CassStatement* statement,
                                             const char* name,
                                             size_t name_length,
                                             CassUuid value) {
  return cass::bind_by_name<CassUuid>(statement, boost::string_ref(name, name_length), value);
}


CassError cass_statement_bind_inet_by_name(CassStatement* statement,
                                           const char* name,
                                           CassInet value) {
  return cass_statement_bind_inet_by_name_n(statement,
                                            name, strlen(name),
                                            value);
}

CassError cass_statement_bind_inet_by_name_n(CassStatement* statement,
                                             const char* name,
                                             size_t name_length,
                                             CassInet value) {
  return cass::bind_by_name<CassInet>(statement, boost::string_ref(name, name_length), value);
}


CassError cass_statement_bind_decimal_by_name(CassStatement* statement,
                                              const char* name,
                                              const cass_byte_t* varint,
                                              size_t varint_size,
                                              cass_int32_t scale) {
  return cass_statement_bind_decimal_by_name_n(statement,
                                               name, strlen(name),
                                               varint, varint_size,
                                               scale);
}

CassError cass_statement_bind_decimal_by_name_n(CassStatement* statement,
                                                const char* name,
                                                size_t name_length,
                                                const cass_byte_t* varint,
                                                size_t varint_size,
                                                cass_int32_t scale) {
  cass::CassDecimal d = { varint, varint_size, scale };
  return cass::bind_by_name<cass::CassDecimal>(statement, boost::string_ref(name, name_length), d);
}

CassError cass_statement_bind_custom_by_name(CassStatement* statement,
                                             const char* name,
                                             size_t size,
                                             cass_byte_t** output) {
  return cass_statement_bind_custom_by_name_n(statement,
                                              name, strlen(name),
                                              size, output);
}

CassError cass_statement_bind_custom_by_name_n(CassStatement* statement,
                                               const char* name,
                                               size_t name_length,
                                               size_t size,
                                               cass_byte_t** output) {
  cass::CassCustom c = { output, size };
  return cass::bind_by_name<cass::CassCustom>(statement, boost::string_ref(name, name_length), c);
}

CassError cass_statement_bind_collection_by_name(CassStatement* statement,
                                                 const char* name,
                                                 const CassCollection* collection) {
  return cass_statement_bind_collection_by_name_n(statement,
                                                  name, strlen(name),
                                                  collection);
}

CassError cass_statement_bind_collection_by_name_n(CassStatement* statement,
                                                   const char* name,
                                                   size_t name_length,
                                                   const CassCollection* collection) {
  return cass::bind_by_name<const CassCollection*>(statement, boost::string_ref(name, name_length), collection);
}

} // extern "C"

namespace cass {

static int32_t decode_buffer_size(const Buffer& buffer) {
  if (!buffer.is_buffer()) {
    LOG_ERROR("Routing key cannot contain an empty value or a collection");
    return -1;
  }
  int32_t size;
  decode_int32(const_cast<char*>(buffer.data()), size);
  if (size < 0) {
    LOG_ERROR("Routing key cannot contain a null value");
  }
  return size;
}

int32_t Statement::encode_values(int version, BufferVec* bufs) const {
  int32_t values_size = 0;
  for (ValueVec::const_iterator it = values_.begin(), end = values_.end();
       it != end; ++it) {
    if (it->is_empty()) {
      Buffer buf(sizeof(int32_t));
      buf.encode_int32(0, -1); // [bytes] "null"
      bufs->push_back(buf);
      values_size += sizeof(int32_t);
    } else if (it->is_collection()) {
      values_size += it->collection()->encode(version, bufs);
    } else {
      bufs->push_back(*it);
      values_size += it->size();
    }
  }
  return values_size;
}

bool Statement::get_routing_key(std::string* routing_key)  const {
  if (key_indices_.empty()) return false;

  if (key_indices_.size() == 1) {
      assert(key_indices_.front() < values_.size());
      const Buffer& buffer = values_[key_indices_.front()];
      int32_t size = decode_buffer_size(buffer);
      if (size < 0) return false;
      routing_key->assign(buffer.data() + sizeof(int32_t), size);
  } else {
    size_t length = 0;

    for (std::vector<size_t>::const_iterator i = key_indices_.begin();
         i != key_indices_.end(); ++i) {
      assert(*i < values_.size());
      const Buffer& buffer = values_[*i];
      int32_t size = decode_buffer_size(buffer);
      if (size < 0) return false;
      length += sizeof(uint16_t) + size + 1;
    }

    routing_key->clear();
    routing_key->reserve(length);

    for (std::vector<size_t>::const_iterator i = key_indices_.begin();
         i != key_indices_.end(); ++i) {
      const Buffer& buffer = values_[*i];
      int32_t size;
      char size_buf[sizeof(uint16_t)];
      decode_int32(const_cast<char*>(buffer.data()), size);
      encode_uint16(size_buf, size);
      routing_key->append(size_buf, sizeof(uint16_t));
      routing_key->append(buffer.data() + sizeof(int32_t), size);
      routing_key->push_back(0);
    }
  }

  return true;
}

} // namespace  cass
