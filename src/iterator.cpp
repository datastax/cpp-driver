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

#include "iterator.hpp"

#include "collection_iterator.hpp"
#include "external.hpp"
#include "map_iterator.hpp"
#include "result_iterator.hpp"
#include "row_iterator.hpp"
#include "user_type_field_iterator.hpp"

using namespace datastax;
using namespace datastax::internal::core;

extern "C" {

void cass_iterator_free(CassIterator* iterator) { delete iterator->from(); }

cass_bool_t cass_iterator_next(CassIterator* iterator) {
  return static_cast<cass_bool_t>(iterator->from()->next());
}

CassIteratorType cass_iterator_type(CassIterator* iterator) { return iterator->type(); }

CassIterator* cass_iterator_from_result(const CassResult* result) {
  return CassIterator::to(new ResultIterator(result));
}

CassIterator* cass_iterator_from_row(const CassRow* row) {
  return CassIterator::to(new RowIterator(row));
}

CassIterator* cass_iterator_from_collection(const CassValue* value) {
  if (value->is_null() || !value->is_collection()) {
    return NULL;
  }
  return CassIterator::to(new CollectionIterator(value));
}

CassIterator* cass_iterator_from_tuple(const CassValue* value) {
  if (value->is_null() || !value->is_tuple()) {
    return NULL;
  }
  return CassIterator::to(new TupleIterator(value));
}

CassIterator* cass_iterator_from_map(const CassValue* value) {
  if (value->is_null() || !value->is_map()) {
    return NULL;
  }
  return CassIterator::to(new MapIterator(value));
}

CassIterator* cass_iterator_fields_from_user_type(const CassValue* value) {
  if (value->is_null() || !value->is_user_type()) {
    return NULL;
  }
  return CassIterator::to(new UserTypeFieldIterator(value));
}

CassError cass_iterator_get_user_type_field_name(const CassIterator* iterator, const char** name,
                                                 size_t* name_length) {
  if (iterator->type() != CASS_ITERATOR_TYPE_USER_TYPE_FIELD) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  StringRef field_name = static_cast<const UserTypeFieldIterator*>(iterator->from())->field_name();
  *name = field_name.data();
  *name_length = field_name.size();
  return CASS_OK;
}

const CassValue* cass_iterator_get_user_type_field_value(const CassIterator* iterator) {
  if (iterator->type() != CASS_ITERATOR_TYPE_USER_TYPE_FIELD) {
    return NULL;
  }
  return CassValue::to(static_cast<const UserTypeFieldIterator*>(iterator->from())->field_value());
}

const CassRow* cass_iterator_get_row(const CassIterator* iterator) {
  if (iterator->type() != CASS_ITERATOR_TYPE_RESULT) {
    return NULL;
  }
  return CassRow::to(static_cast<const ResultIterator*>(iterator->from())->row());
}

const CassValue* cass_iterator_get_column(const CassIterator* iterator) {
  if (iterator->type() != CASS_ITERATOR_TYPE_ROW) {
    return NULL;
  }
  return CassValue::to(static_cast<const RowIterator*>(iterator->from())->column());
}

const CassValue* cass_iterator_get_value(const CassIterator* iterator) {
  if (iterator->type() != CASS_ITERATOR_TYPE_COLLECTION &&
      iterator->type() != CASS_ITERATOR_TYPE_TUPLE) {
    return NULL;
  }
  return CassValue::to(static_cast<const ValueIterator*>(iterator->from())->value());
}

const CassValue* cass_iterator_get_map_key(const CassIterator* iterator) {
  if (iterator->type() != CASS_ITERATOR_TYPE_MAP) {
    return NULL;
  }
  return CassValue::to(static_cast<const MapIterator*>(iterator->from())->key());
}

const CassValue* cass_iterator_get_map_value(const CassIterator* iterator) {
  if (iterator->type() != CASS_ITERATOR_TYPE_MAP) {
    return NULL;
  }
  return CassValue::to(static_cast<const MapIterator*>(iterator->from())->value());
}

} // extern "C"
