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

#ifndef __BODY_RESULT_HPP_INCLUDED__
#define __BODY_RESULT_HPP_INCLUDED__

#include <list>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "cql_body.hpp"
#include "cql_iterable.hpp"

#define CQL_RESULT_KIND_VOID          1
#define CQL_RESULT_KIND_ROWS          2
#define CQL_RESULT_KIND_SET_KEYSPACE  3
#define CQL_RESULT_KIND_PREPARED      4
#define CQL_RESULT_KIND_SCHEMA_CHANGE 5

#define CQL_RESULT_FLAG_GLOBAL_TABLESPEC 1
#define CQL_RESULT_FLAG_HAS_MORE_PAGES   2
#define CQL_RESULT_FLAG_NO_METADATA      4

namespace cql {

struct ResultIterator;

struct BodyResult
    : public Body {

  struct ColumnMetaData {
    ColumnMetaData() :
        type(CQL_COLUMN_TYPE_UNKNOWN),
        keyspace(NULL),
        keyspace_size(0),
        table(NULL),
        table_size(0),
        name(NULL),
        name_size(0),
        class_name(NULL),
        class_name_size(0),
        collection_primary_type(CQL_COLUMN_TYPE_UNKNOWN),
        collection_primary_class(NULL),
        collection_primary_class_size(0),
        collection_secondary_type(CQL_COLUMN_TYPE_UNKNOWN),
        collection_secondary_class(NULL),
        collection_secondary_class_size(0)
    {}

    int16_t type;
    char*   keyspace;
    size_t  keyspace_size;

    char*   table;
    size_t  table_size;

    char*   name;
    size_t  name_size;

    char*   class_name;
    size_t  class_name_size;

    int16_t collection_primary_type;
    char*   collection_primary_class;

    size_t  collection_primary_class_size;
    int16_t collection_secondary_type;

    char*   collection_secondary_class;
    size_t  collection_secondary_class_size;
  };

  typedef std::vector<ColumnMetaData>             MetaDataCollection;
  typedef std::unordered_map<std::string, size_t> MetaDataIndex;

  int32_t            kind;
  bool               more_pages;        // row data
  bool               no_metadata;
  bool               global_table_spec;
  int32_t            column_count;
  MetaDataCollection column_metadata;
  MetaDataIndex      column_index;
  char*              page_state;        // row paging
  size_t             page_state_size;
  char*              prepared;          // prepared result
  size_t             prepared_size;
  char*              change;            // schema change
  size_t             change_size;
  char*              keyspace;          // rows, set keyspace, and schema change
  size_t             keyspace_size;
  char*              table;             // rows, and schema change
  size_t             table_size;
  int32_t            row_count;
  char*              rows;

  BodyResult() :
      kind(0),
      more_pages(false),
      no_metadata(false),
      global_table_spec(true),
      column_count(0),
      page_state(NULL),
      page_state_size(0),
      prepared(NULL),
      prepared_size(0),
      change(NULL),
      change_size(0),
      keyspace(NULL),
      keyspace_size(0),
      table(NULL),
      table_size(0),
      row_count(0),
      rows(NULL)
  {}

  uint8_t
  opcode() {
    return CQL_OPCODE_RESULT;
  }

  bool
  consume(
      char* input,
      size_t size) {
    (void) size;

    char* buffer = decode_int(input, kind);

    switch (kind) {
      case CQL_RESULT_KIND_VOID:
        return true;
        break;
      case CQL_RESULT_KIND_ROWS:
        return parse_rows(buffer);
        break;
      case CQL_RESULT_KIND_SET_KEYSPACE:
        return parse_set_keyspace(buffer);
        break;
      case CQL_RESULT_KIND_PREPARED:
        return parse_prepared(buffer);
        break;
      case CQL_RESULT_KIND_SCHEMA_CHANGE:
        return parse_schema_change(buffer);
        break;
      default:
        assert(false);
    }
    return false;
  }

  char*
  parse_metadata(
      char* input) {
    int32_t flags  = 0;
    char*   buffer = decode_int(input, flags);
    buffer         = decode_int(buffer, column_count);

    if (flags & CQL_RESULT_FLAG_HAS_MORE_PAGES) {
      more_pages = true;
      buffer     = decode_long_string(buffer, &page_state, page_state_size);
    } else {
      more_pages = false;
    }

    if (flags & CQL_RESULT_FLAG_GLOBAL_TABLESPEC) {
      global_table_spec = true;
      buffer            = decode_string(buffer, &keyspace, keyspace_size);
      buffer            = decode_string(buffer, &table, table_size);
    } else {
      global_table_spec = false;
    }

    if (flags & CQL_RESULT_FLAG_NO_METADATA) {
      no_metadata = true;
    } else {
      no_metadata = false;
      column_metadata.resize(column_count);

      for (int i = 0; i < column_count; ++i) {
        ColumnMetaData meta;

        if (!global_table_spec) {
          buffer = decode_string(buffer, &meta.keyspace, meta.keyspace_size);
          buffer = decode_string(buffer, &meta.table, meta.table_size);
        }

        buffer = decode_string(buffer, &meta.name, meta.name_size);
        buffer = decode_option(
            buffer,
            meta.type,
            &meta.class_name,
            meta.class_name_size);

        if (meta.type == CQL_COLUMN_TYPE_SET
            || meta.type == CQL_COLUMN_TYPE_LIST
            || meta.type == CQL_COLUMN_TYPE_MAP) {
          buffer = decode_option(
              buffer,
              meta.collection_primary_type,
              &meta.collection_primary_class,
              meta.collection_primary_class_size);
        }

        if (meta.type == CQL_COLUMN_TYPE_MAP) {
          buffer = decode_option(
              buffer,
              meta.collection_secondary_type,
              &meta.collection_secondary_class,
              meta.collection_secondary_class_size);
        }

        column_metadata.push_back(meta);
        column_index.insert(
            std::make_pair(std::string(meta.name, meta.name_size), i));
      }
    }
    return buffer;
  }

  bool
  parse_rows(
      char* input) {
    char* buffer = parse_metadata(input);
    rows = decode_int(buffer, row_count);
    return true;
  }

  bool
  parse_set_keyspace(
      char* input) {
    decode_string(input, &keyspace, keyspace_size);
    return true;
  }

  bool
  parse_prepared(
      char* input) {
    char* buffer = decode_string(input, &prepared, prepared_size);
    parse_metadata(buffer);
    return true;
  }

  bool
  parse_schema_change(
      char* input) {
    char* buffer = decode_string(input, &change, change_size);
    buffer       = decode_string(buffer, &keyspace, keyspace_size);
    buffer       = decode_string(buffer, &table, table_size);
    return true;
  }

  bool
  prepare(
      size_t  reserved,
      char**  output,
      size_t& size) {
    (void) reserved;
    (void) output;
    (void) size;
    return false;
  }

 private:
  BodyResult(const BodyResult&) {}
  void operator=(const BodyResult&) {}
};

struct ResultIterator : Iterable {
  typedef std::pair<char*, size_t> Column;

  BodyResult*         result;
  int32_t             row_position;
  char*               position;
  char*               position_next;
  std::vector<Column> row;

  ResultIterator(
      BodyResult* result) :
      Iterable(CQL_ITERABLE_TYPE_RESULT),
      result(result),
      row_position(0),
      position(result->rows),
      row(result->column_count) {
    position_next = parse_row(position, row);
  }

  char*
  parse_row(
      char* row,
      std::vector<Column>& output) {
    char* buffer = row;
    output.clear();

    for (int i = 0; i < result->column_count; ++i) {
      int32_t size  = 0;
      buffer        = decode_int(buffer, size);
      output.push_back(std::make_pair(buffer, size));
      buffer       += size;
    }
    return buffer;
  }

  bool
  next() {
    ++row_position;
    if (row_position >= result->row_count) {
      return false;
    }
    position_next = parse_row(position_next, row);
    return true;
  }
};

}
#endif
