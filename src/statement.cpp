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

#include "statement.hpp"

#include "execute_request.hpp"
#include "result_metadata.hpp"
#include "prepared.hpp"
#include "query_request.hpp"
#include "scoped_ptr.hpp"
#include "types.hpp"

#include "third_party/boost/boost/cstdint.hpp"

namespace {

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
  struct IsValidValueType<const CassUuid> {
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
      // TODO(mpenick): Check acutal type against collection
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
                         const char* name,
                         T value) {
    if (statement->opcode() != CQL_OPCODE_EXECUTE) {
      return CASS_ERROR_INVALID_STATEMENT_TYPE;
    }

    const cass::ResultResponse* result
        = static_cast<cass::ExecuteRequest*>(statement)->prepared()->result().get();

    cass::ResultMetadata::IndexVec indices;
    result->find_column_indices(name, &indices);
    IsValidValueType<T> is_valid_type;

    if (indices.empty()) {
      return CASS_ERROR_NAME_DOES_NOT_EXIST;
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

} // namespace

extern "C" {

CassStatement* cass_statement_new(CassString query, size_t parameter_count) {
  cass::QueryRequest* query_request = new cass::QueryRequest(parameter_count);
  query_request->inc_ref();
  query_request->set_query(query.data, query.length);
  return CassStatement::to(query_request);
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
                                   cass_size_t index) {
  return statement->bind(index, CassNull());
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
                                     CassString value) {
  return statement->bind(index, value);
}

CassError cass_statement_bind_bytes(CassStatement* statement, size_t index,
                                    CassBytes value) {
  return statement->bind(index, value);
}

CassError cass_statement_bind_uuid(CassStatement* statement, size_t index,
                                   const CassUuid value) {
  return statement->bind(index, value);
}

CassError cass_statement_bind_inet(CassStatement* statement, size_t index,
                                   CassInet value) {
  return statement->bind(index, value);
}

CassError cass_statement_bind_decimal(CassStatement* statement,
                                      cass_size_t index, CassDecimal value) {
  return statement->bind(index, value);
}

CassError cass_statement_bind_collection(CassStatement* statement, size_t index,
                                         const CassCollection* collection) {
  return statement->bind(index, collection->from());
}

CassError cass_statement_bind_custom(CassStatement* statement,
                                     cass_size_t index, cass_size_t size,
                                     cass_byte_t** output) {
  CassCustom custom;
  custom.output = output;
  custom.output_size = size;
  return statement->bind(index, custom);
}

CassError cass_statement_bind_null_by_name(CassStatement* statement,
                                           const char* name) {
  return bind_by_name<CassNull>(statement, name, CassNull());
}

CassError cass_statement_bind_int32_by_name(CassStatement* statement,
                                            const char* name,
                                            cass_int32_t value) {
  return bind_by_name<cass_int32_t>(statement, name, value);
}

CassError cass_statement_bind_int64_by_name(CassStatement* statement,
                                            const char* name,
                                            cass_int64_t value) {
  return bind_by_name<cass_int64_t>(statement, name, value);
}

CassError cass_statement_bind_float_by_name(CassStatement* statement,
                                            const char* name,
                                            cass_float_t value) {
  return bind_by_name<cass_float_t>(statement, name, value);
}

CassError cass_statement_bind_double_by_name(CassStatement* statement,
                                             const char* name,
                                             cass_double_t value) {
  return bind_by_name<cass_double_t>(statement, name, value);
}

CassError cass_statement_bind_bool_by_name(CassStatement* statement,
                                           const char* name,
                                           cass_bool_t value) {
  return bind_by_name<bool>(statement, name, value == cass_true);
}

CassError cass_statement_bind_string_by_name(CassStatement* statement,
                                             const char* name,
                                             CassString value) {
  return bind_by_name<CassString>(statement, name, value);
}

CassError cass_statement_bind_bytes_by_name(CassStatement* statement,
                                            const char* name,
                                            CassBytes value) {
  return bind_by_name<CassBytes>(statement, name, value);
}

CassError cass_statement_bind_uuid_by_name(CassStatement* statement,
                                           const char* name,
                                           const CassUuid value) {
  return bind_by_name<const CassUuid>(statement, name, value);
}

CassError cass_statement_bind_inet_by_name(CassStatement* statement,
                                           const char* name,
                                           CassInet value) {
  return bind_by_name<CassInet>(statement, name, value);
}

CassError cass_statement_bind_decimal_by_name(CassStatement* statement,
                                              const char* name,
                                              CassDecimal value) {
  return bind_by_name<CassDecimal>(statement, name, value);
}

CassError cass_statement_bind_custom_by_name(CassStatement* statement,
                                             const char* name,
                                             cass_size_t size,
                                             cass_byte_t** output) {
  CassCustom custom;
  custom.output = output;
  custom.output_size = size;
  return bind_by_name<CassCustom>(statement, name, custom);
}

CassError cass_statement_bind_collection_by_name(CassStatement* statement,
                                                 const char* name,
                                                 const CassCollection* collection) {
  return bind_by_name<const CassCollection*>(statement, name, collection);
}

} // extern "C"

namespace cass {

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

} // namespace  cass
