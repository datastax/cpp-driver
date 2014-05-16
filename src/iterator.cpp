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

#include "cassandra.hpp"

#include "iterator.hpp"
#include "result_iterator.hpp"
#include "collection_iterator.hpp"
#include "row_iterator.hpp"

extern "C" {

cass_iterator_t*
cass_iterator_from_result(const cass_result_t* result) {
  return cass_iterator_t::to(new cass::ResultIterator(result));
}

cass_iterator_t*
cass_iterator_from_row(const cass_row_t* row) {
  return cass_iterator_t::to(new cass::RowIterator(row));
}

cass_iterator_t*
cass_iterator_from_collection(const cass_value_t* value) {
  if(cass_value_is_collection(value)) {
    return cass_iterator_t::to(new cass::CollectionIterator(value));
  }
  return nullptr;
}

void
cass_iterator_free(
    cass_iterator_t* iterator) {
  delete iterator->from();
}

cass_bool_t
cass_iterator_next(
    cass_iterator_t* iterator) {
  return static_cast<bool>(iterator->from()->next());
}

const cass_row_t*
cass_iterator_get_row(
    cass_iterator_t *iterator) {
    cass::Iterator* internal_it = iterator->from();
    if(internal_it->type != cass::CASS_ITERATOR_TYPE_RESULT) {
      return nullptr;
    }
    cass::ResultIterator* result_it = static_cast<cass::ResultIterator*>(internal_it);
    return cass_row_t::to(&result_it->row);
}

const cass_value_t*
cass_iterator_get_column(
    cass_iterator_t *iterator) {
  cass::Iterator* internal_it = iterator->from();
  if(internal_it->type != cass::CASS_ITERATOR_TYPE_ROW) {
    return nullptr;
  }
  cass::RowIterator* row_it = static_cast<cass::RowIterator*>(internal_it);
  return cass_value_t::to(row_it->column());
}

const cass_value_t*
cass_iterator_get_value(
    cass_iterator_t *iterator) {
  cass::Iterator* internal_it = iterator->from();
  if(internal_it->type != cass::CASS_ITERATOR_COLLECTION) {
    return nullptr;
  }
  cass::CollectionIterator* collection_it = static_cast<cass::CollectionIterator*>(internal_it);
  return cass_value_t::to(collection_it->value());
}

} // extern "C"
