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

#include "metadata.hpp"

#include "buffer.hpp"
#include "collection.hpp"
#include "collection_iterator.hpp"
#include "external.hpp"
#include "iterator.hpp"
#include "logger.hpp"
#include "map_iterator.hpp"
#include "result_iterator.hpp"
#include "row.hpp"
#include "row_iterator.hpp"
#include "scoped_lock.hpp"
#include "data_type_parser.hpp"
#include "utils.hpp"
#include "value.hpp"

#include "third_party/rapidjson/rapidjson/document.h"

#include <algorithm>
#include <cmath>
#include <ctype.h>

static std::string& append_arguments(std::string& full_name, const std::string& arguments) {
  full_name.push_back('(');
  bool first = true;
  std::istringstream stream(arguments);
  while (!stream.eof()) {
    std::string argument;
    std::getline(stream, argument, ',');
    // Remove white-space
    argument.erase(std::remove_if(argument.begin(), argument.end(), ::isspace),
                   argument.end());
    if (!argument.empty()) {
      if (!first) full_name.push_back(',');
      full_name.append(argument);
      first = false;
    }
  }
  full_name.push_back(')');
  return full_name;
}

extern "C" {

void cass_schema_meta_free(const CassSchemaMeta* schema_meta) {
  delete schema_meta->from();
}

cass_uint32_t cass_schema_meta_snapshot_version(const CassSchemaMeta* schema_meta) {
  return schema_meta->version();
}

CassVersion cass_schema_meta_version(const CassSchemaMeta* schema_meta) {
  CassVersion version;
  version.major_version = schema_meta->cassandra_version().major_version();
  version.minor_version = schema_meta->cassandra_version().minor_version();
  version.patch_version = schema_meta->cassandra_version().patch_version();
  return version;
}

const CassKeyspaceMeta* cass_schema_meta_keyspace_by_name(const CassSchemaMeta* schema_meta,
                                               const char* keyspace) {
  return CassKeyspaceMeta::to(schema_meta->get_keyspace(keyspace));
}

const CassKeyspaceMeta* cass_schema_meta_keyspace_by_name_n(const CassSchemaMeta* schema_meta,
                                                            const char* keyspace,
                                                            size_t keyspace_length) {
  return CassKeyspaceMeta::to(schema_meta->get_keyspace(std::string(keyspace, keyspace_length)));
}

void cass_keyspace_meta_name(const CassKeyspaceMeta* keyspace_meta,
                             const char** name, size_t* name_length) {
  *name = keyspace_meta->name().data();
  *name_length = keyspace_meta->name().size();
}

const CassTableMeta* cass_keyspace_meta_table_by_name(const CassKeyspaceMeta* keyspace_meta,
                                                      const char* table) {
  return CassTableMeta::to(keyspace_meta->get_table(table));
}

const CassTableMeta* cass_keyspace_meta_table_by_name_n(const CassKeyspaceMeta* keyspace_meta,
                                                        const char* table,
                                                        size_t table_length) {

  return CassTableMeta::to(keyspace_meta->get_table(std::string(table, table_length)));
}

const CassMaterializedViewMeta* cass_keyspace_meta_materialized_view_by_name(const CassKeyspaceMeta* keyspace_meta,
                                                                             const char* view) {
  return CassMaterializedViewMeta::to(keyspace_meta->get_view(view));
}

const CassMaterializedViewMeta* cass_keyspace_meta_materialized_view_by_name_n(const CassKeyspaceMeta* keyspace_meta,
                                                                               const char* view,
                                                                               size_t view_length) {

  return CassMaterializedViewMeta::to(keyspace_meta->get_view(std::string(view, view_length)));
}

const CassDataType* cass_keyspace_meta_user_type_by_name(const CassKeyspaceMeta* keyspace_meta,
                                                         const char* type) {
  return CassDataType::to(keyspace_meta->get_user_type(type));
}

const CassDataType* cass_keyspace_meta_user_type_by_name_n(const CassKeyspaceMeta* keyspace_meta,
                                                  const char* type,
                                                  size_t type_length) {
  return CassDataType::to(keyspace_meta->get_user_type(std::string(type, type_length)));
}

const CassFunctionMeta* cass_keyspace_meta_function_by_name(const CassKeyspaceMeta* keyspace_meta,
                                                            const char* name,
                                                            const char* arguments) {
  std::string full_function_name(name);
  return CassFunctionMeta::to(keyspace_meta->get_function(
                                append_arguments(full_function_name, std::string(arguments))));
}


const CassFunctionMeta* cass_keyspace_meta_function_by_name_n(const CassKeyspaceMeta* keyspace_meta,
                                                              const char* name,
                                                              size_t name_length,
                                                              const char* arguments,
                                                              size_t arguments_length) {
  std::string full_function_name(name, name_length);
  return CassFunctionMeta::to(keyspace_meta->get_function(
                                append_arguments(full_function_name, std::string(arguments, arguments_length))));
}

const CassAggregateMeta* cass_keyspace_meta_aggregate_by_name(const CassKeyspaceMeta* keyspace_meta,
                                                              const char* name,
                                                              const char* arguments) {
  std::string full_aggregate_name(name);
  return CassAggregateMeta::to(keyspace_meta->get_aggregate(
                                 append_arguments(full_aggregate_name, std::string(arguments))));
}

const CassAggregateMeta* cass_keyspace_meta_aggregate_by_name_n(const CassKeyspaceMeta* keyspace_meta,
                                                                const char* name,
                                                                size_t name_length,
                                                                const char* arguments,
                                                                size_t arguments_length) {
  std::string full_aggregate_name(name, name_length);
  return CassAggregateMeta::to(keyspace_meta->get_aggregate(
                                 append_arguments(full_aggregate_name, std::string(arguments, arguments_length))));
}

const CassValue* cass_keyspace_meta_field_by_name(const CassKeyspaceMeta* keyspace_meta,
                                               const char* name) {
  return CassValue::to(keyspace_meta->get_field(name));
}

const CassValue* cass_keyspace_meta_field_by_name_n(const CassKeyspaceMeta* keyspace_meta,
                                                 const char* name, size_t name_length) {
  return CassValue::to(keyspace_meta->get_field(std::string(name, name_length)));
}

void cass_table_meta_name(const CassTableMeta* table_meta,
                          const char** name, size_t* name_length) {
  *name = table_meta->name().data();
  *name_length = table_meta->name().size();
}

const CassColumnMeta* cass_table_meta_column_by_name(const CassTableMeta* table_meta,
                                                 const char* column) {
  return CassColumnMeta::to(table_meta->get_column(column));
}

const CassColumnMeta* cass_table_meta_column_by_name_n(const CassTableMeta* table_meta,
                                                   const char* column,
                                                   size_t column_length) {
  return CassColumnMeta::to(table_meta->get_column(std::string(column, column_length)));
}

size_t cass_table_meta_column_count(const CassTableMeta* table_meta) {
  return table_meta->columns().size();
}

const CassColumnMeta* cass_table_meta_column(const CassTableMeta* table_meta,
                                             size_t index) {
  if (index >= table_meta->columns().size()) {
    return NULL;
  }
  return CassColumnMeta::to(table_meta->columns()[index].get());
}

const CassIndexMeta* cass_table_meta_index_by_name(const CassTableMeta* table_meta,
                                                 const char* index) {
  return CassIndexMeta::to(table_meta->get_index(index));
}

const CassIndexMeta* cass_table_meta_index_by_name_n(const CassTableMeta* table_meta,
                                                   const char* index,
                                                   size_t index_length) {
  return CassIndexMeta::to(table_meta->get_index(std::string(index, index_length)));
}

size_t cass_table_meta_index_count(const CassTableMeta* table_meta) {
  return table_meta->indexes().size();
}

const CassIndexMeta* cass_table_meta_index(const CassTableMeta* table_meta,
                                             size_t index) {
  if (index >= table_meta->indexes().size()) {
    return NULL;
  }
  return CassIndexMeta::to(table_meta->indexes()[index].get());
}

const CassMaterializedViewMeta* cass_table_meta_materialized_view_by_name(const CassTableMeta* table_meta,
                                                                          const char* view) {
  return CassMaterializedViewMeta::to(table_meta->get_view(view));
}

const CassMaterializedViewMeta* cass_table_meta_materialized_view_by_name_n(const CassTableMeta* table_meta,
                                                                            const char* view,
                                                                            size_t view_length) {
  return CassMaterializedViewMeta::to(table_meta->get_view(std::string(view, view_length)));
}

size_t cass_table_meta_materialized_view_count(const CassTableMeta* table_meta) {
  return table_meta->views().size();
}

const CassMaterializedViewMeta* cass_table_meta_materialized_view(const CassTableMeta* table_meta,
                                                                  size_t index) {
  if (index >= table_meta->views().size()) {
    return NULL;
  }
  return CassMaterializedViewMeta::to(table_meta->views()[index].get());
}

size_t cass_table_meta_partition_key_count(const CassTableMeta* table_meta) {
  return table_meta->partition_key().size();
}

const CassColumnMeta* cass_table_meta_partition_key(const CassTableMeta* table_meta,
                                                    size_t index) {
  if (index >= table_meta->partition_key().size()) {
    return NULL;
  }
  return CassColumnMeta::to(table_meta->partition_key()[index].get());
}

size_t cass_table_meta_clustering_key_count(const CassTableMeta* table_meta) {
  return table_meta->clustering_key().size();
}

const CassColumnMeta* cass_table_meta_clustering_key(const CassTableMeta* table_meta,
                                                     size_t index) {
  if (index >= table_meta->clustering_key().size()) {
    return NULL;
  }
  return CassColumnMeta::to(table_meta->clustering_key()[index].get());
}

CassClusteringOrder cass_table_meta_clustering_key_order(const CassTableMeta* table_meta,
                                                         size_t index) {
  if (index >= table_meta->clustering_key_order().size()) {
    return CASS_CLUSTERING_ORDER_NONE;
  }
  return table_meta->clustering_key_order()[index];
}

const CassValue* cass_table_meta_field_by_name(const CassTableMeta* table_meta,
                                               const char* name) {
  return CassValue::to(table_meta->get_field(name));
}

const CassValue* cass_table_meta_field_by_name_n(const CassTableMeta* table_meta,
                                                 const char* name, size_t name_length) {
  return CassValue::to(table_meta->get_field(std::string(name, name_length)));
}

const CassColumnMeta* cass_materialized_view_meta_column_by_name(const CassMaterializedViewMeta* view_meta,
                                                                 const char* column) {
  return CassColumnMeta::to(view_meta->get_column(column));
}

const CassColumnMeta* cass_materialized_view_meta_column_by_name_n(const CassMaterializedViewMeta* view_meta,
                                                                   const char* column,
                                                                   size_t column_length) {
  return CassColumnMeta::to(view_meta->get_column(std::string(column, column_length)));
}

void cass_materialized_view_meta_name(const CassMaterializedViewMeta* view_meta,
                                      const char** name, size_t* name_length) {
  *name = view_meta->name().data();
  *name_length = view_meta->name().size();
}

const CassTableMeta* cass_materialized_view_meta_base_table(const CassMaterializedViewMeta* view_meta) {
  return CassTableMeta::to(view_meta->base_table());
}

const CassValue* cass_materialized_view_meta_field_by_name(const CassMaterializedViewMeta* view_meta,
                                                           const char* name) {
  return CassValue::to(view_meta->get_field(name));
}

const CassValue* cass_materialized_view_meta_field_by_name_n(const CassMaterializedViewMeta* view_meta,
                                                             const char* name, size_t name_length) {
  return CassValue::to(view_meta->get_field(std::string(name, name_length)));
}

size_t cass_materialized_view_meta_column_count(const CassMaterializedViewMeta* view_meta) {
  return view_meta->columns().size();
}

const CassColumnMeta* cass_materialized_view_meta_column(const CassMaterializedViewMeta* view_meta,
                                                         size_t index) {
  if (index >= view_meta->columns().size()) {
    return NULL;
  }
  return CassColumnMeta::to(view_meta->columns()[index].get());
}

size_t cass_materialized_view_meta_partition_key_count(const CassMaterializedViewMeta* view_meta) {
  return view_meta->partition_key().size();
}

const CassColumnMeta* cass_materialized_view_meta_partition_key(const CassMaterializedViewMeta* view_meta,
                                                                size_t index) {
  if (index >= view_meta->partition_key().size()) {
    return NULL;
  }
  return CassColumnMeta::to(view_meta->partition_key()[index].get());
}

size_t cass_materialized_view_meta_clustering_key_count(const CassMaterializedViewMeta* view_meta) {
  return view_meta->clustering_key().size();
}

const CassColumnMeta* cass_materialized_view_meta_clustering_key(const CassMaterializedViewMeta* view_meta,
                                                                 size_t index) {
  if (index >= view_meta->clustering_key().size()) {
    return NULL;
  }
  return CassColumnMeta::to(view_meta->clustering_key()[index].get());
}

CassClusteringOrder cass_materialized_view_meta_clustering_key_order(const CassMaterializedViewMeta* view_meta,
                                                                     size_t index) {
  if (index >= view_meta->clustering_key_order().size()) {
    return CASS_CLUSTERING_ORDER_NONE;
  }
  return view_meta->clustering_key_order()[index];
}

void cass_column_meta_name(const CassColumnMeta* column_meta,
                           const char** name, size_t* name_length) {
  *name = column_meta->name().data();
  *name_length = column_meta->name().size();
}

CassColumnType cass_column_meta_type(const CassColumnMeta* column_meta) {
  return column_meta->type();
}

const CassDataType* cass_column_meta_data_type(const CassColumnMeta* column_meta) {
  return CassDataType::to(column_meta->data_type().get());
}

const CassValue*
cass_column_meta_field_by_name(const CassColumnMeta* column_meta,
                           const char* name) {
  return CassValue::to(column_meta->get_field(name));
}

const CassValue*
cass_column_meta_field_by_name_n(const CassColumnMeta* column_meta,
                             const char* name, size_t name_length) {
  return CassValue::to(column_meta->get_field(std::string(name, name_length)));
}

void cass_index_meta_name(const CassIndexMeta* index_meta,
                           const char** name, size_t* name_length) {
  *name = index_meta->name().data();
  *name_length = index_meta->name().size();
}

CassIndexType cass_index_meta_type(const CassIndexMeta* index_meta) {
  return index_meta->type();
}

void cass_index_meta_target(const CassIndexMeta* index_meta,
                            const char** target, size_t* target_length) {
  *target = index_meta->target().data();
  *target_length = index_meta->target().size();
}

const CassValue* cass_index_meta_options(const CassIndexMeta* index_meta) {
  return CassValue::to(index_meta->options());
}

const CassValue*
cass_index_meta_field_by_name(const CassIndexMeta* index_meta,
                           const char* name) {
  return CassValue::to(index_meta->get_field(name));
}

const CassValue*
cass_index_meta_field_by_name_n(const CassIndexMeta* index_meta,
                             const char* name, size_t name_length) {
  return CassValue::to(index_meta->get_field(std::string(name, name_length)));
}

void cass_function_meta_name(const CassFunctionMeta* function_meta,
                        const char** name,
                        size_t* name_length) {
  *name = function_meta->simple_name().data();
  *name_length = function_meta->simple_name().size();
}

void cass_function_meta_full_name(const CassFunctionMeta* function_meta,
                             const char** full_name,
                             size_t* full_name_length) {
  *full_name = function_meta->name().data();
  *full_name_length = function_meta->name().size();
}

void cass_function_meta_body(const CassFunctionMeta* function_meta,
                        const char** body,
                        size_t* body_length) {
  *body = function_meta->body().data();
  *body_length = function_meta->body().size();
}

void cass_function_meta_language(const CassFunctionMeta* function_meta,
                            const char** language,
                            size_t* language_length) {
  *language = function_meta->language().data();
  *language_length = function_meta->language().size();
}

cass_bool_t cass_function_meta_called_on_null_input(const CassFunctionMeta* function_meta) {
  return function_meta->called_on_null_input() ? cass_true : cass_false;
}

size_t cass_function_meta_argument_count(const CassFunctionMeta* function_meta) {
  return function_meta->args().size();
}

CassError cass_function_meta_argument(const CassFunctionMeta* function_meta,
                                      size_t index,
                                      const char** name,
                                      size_t* name_length,
                                      const CassDataType** type) {
  if (index >= function_meta->args().size()) {
    return CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS;
  }
  const cass::FunctionMetadata::Argument& arg = function_meta->args()[index];
  *name = arg.name.data();
  *name_length = arg.name.size();
  *type = CassDataType::to(arg.type.get());
  return CASS_OK;
}

const CassDataType* cass_function_meta_argument_type_by_name(const CassFunctionMeta* function_meta,
                                                             const char* name) {
  return CassDataType::to(function_meta->get_arg_type(name));
}

const CassDataType* cass_function_meta_argument_type_by_name_n(const CassFunctionMeta* function_meta,
                                                               const char* name,
                                                               size_t name_length) {
  return CassDataType::to(function_meta->get_arg_type(cass::StringRef(name, name_length)));
}

const CassDataType* cass_function_meta_return_type(const CassFunctionMeta* function_meta) {
  return CassDataType::to(function_meta->return_type().get());
}

const CassValue* cass_function_meta_field_by_name(const CassFunctionMeta* function_meta,
                                                  const char* name) {
  return CassValue::to(function_meta->get_field(name));
}

const CassValue* cass_function_meta_field_by_name_n(const CassFunctionMeta* function_meta,
                                                    const char* name,
                                                    size_t name_length) {
  return CassValue::to(function_meta->get_field(std::string(name, name_length)));
}

void cass_aggregate_meta_name(const CassAggregateMeta* aggregate_meta,
                              const char** name,
                              size_t* name_length) {
  *name = aggregate_meta->simple_name().data();
  *name_length = aggregate_meta->simple_name().size();
}

void cass_aggregate_meta_full_name(const CassAggregateMeta* aggregate_meta,
                                   const char** full_name,
                                   size_t* full_name_length) {
  *full_name = aggregate_meta->name().data();
  *full_name_length = aggregate_meta->name().size();
}

size_t cass_aggregate_meta_argument_count(const CassAggregateMeta* aggregate_meta) {
  return aggregate_meta->arg_types().size();
}

const CassDataType* cass_aggregate_meta_argument_type(const CassAggregateMeta* aggregate_meta,
                                                      size_t index) {
  if (index >= aggregate_meta->arg_types().size()) {
    return NULL;
  }
  return CassDataType::to(aggregate_meta->arg_types()[index].get());
}

const CassDataType* cass_aggregate_meta_return_type(const CassAggregateMeta* aggregate_meta) {
  return CassDataType::to(aggregate_meta->return_type().get());
}

const CassDataType* cass_aggregate_meta_state_type(const CassAggregateMeta* aggregate_meta) {
  return CassDataType::to(aggregate_meta->state_type().get());
}

const CassFunctionMeta* cass_aggregate_meta_state_func(const CassAggregateMeta* aggregate_meta) {
  return CassFunctionMeta::to(aggregate_meta->state_func().get());
}

const CassFunctionMeta* cass_aggregate_meta_final_func(const CassAggregateMeta* aggregate_meta) {
  return CassFunctionMeta::to(aggregate_meta->final_func().get());
}

const CassValue* cass_aggregate_meta_init_cond(const CassAggregateMeta* aggregate_meta) {
  return CassValue::to(&aggregate_meta->init_cond());
}

const CassValue* cass_aggregate_meta_field_by_name(const CassAggregateMeta* aggregate_meta,
                                                   const char* name) {
  return CassValue::to(aggregate_meta->get_field(name));
}

const CassValue* cass_aggregate_meta_field_by_name_n(const CassAggregateMeta* aggregate_meta,
                                                     const char* name,
                                                     size_t name_length) {
  return CassValue::to(aggregate_meta->get_field(std::string(name, name_length)));
}

CassIterator* cass_iterator_keyspaces_from_schema_meta(const CassSchemaMeta* schema_meta) {
  return CassIterator::to(schema_meta->iterator_keyspaces());
}

CassIterator* cass_iterator_tables_from_keyspace_meta(const CassKeyspaceMeta* keyspace_meta) {
  return CassIterator::to(keyspace_meta->iterator_tables());
}

CassIterator* cass_iterator_materialized_views_from_keyspace_meta(const CassKeyspaceMeta* keyspace_meta) {
  return CassIterator::to(keyspace_meta->iterator_views());
}

CassIterator* cass_iterator_user_types_from_keyspace_meta(const CassKeyspaceMeta* keyspace_meta) {
  return CassIterator::to(keyspace_meta->iterator_user_types());
}

CassIterator* cass_iterator_functions_from_keyspace_meta(const CassKeyspaceMeta* keyspace_meta) {
  return CassIterator::to(keyspace_meta->iterator_functions());
}

CassIterator* cass_iterator_aggregates_from_keyspace_meta(const CassKeyspaceMeta* keyspace_meta) {
  return CassIterator::to(keyspace_meta->iterator_aggregates());
}

CassIterator* cass_iterator_fields_from_keyspace_meta(const CassKeyspaceMeta* keyspace_meta) {
  return CassIterator::to(keyspace_meta->iterator_fields());
}

CassIterator* cass_iterator_columns_from_table_meta(const CassTableMeta* table_meta) {
  return CassIterator::to(table_meta->iterator_columns());
}

CassIterator* cass_iterator_materialized_views_from_table_meta(const CassTableMeta* table_meta) {
  return CassIterator::to(table_meta->iterator_views());
}

CassIterator* cass_iterator_indexes_from_table_meta(const CassTableMeta* table_meta) {
  return CassIterator::to(table_meta->iterator_indexes());
}

CassIterator* cass_iterator_fields_from_table_meta(const CassTableMeta* table_meta) {
  return CassIterator::to(table_meta->iterator_fields());
}

CassIterator* cass_iterator_columns_from_materialized_view_meta(const CassMaterializedViewMeta* view_meta) {
  return CassIterator::to(view_meta->iterator_columns());
}

CassIterator* cass_iterator_fields_from_materialized_view_meta(const CassMaterializedViewMeta* view_meta) {
  return CassIterator::to(view_meta->iterator_fields());
}

CassIterator* cass_iterator_fields_from_column_meta(const CassColumnMeta* column_meta) {
  return CassIterator::to(column_meta->iterator_fields());
}

CassIterator* cass_iterator_fields_from_index_meta(const CassIndexMeta* index_meta) {
  return CassIterator::to(index_meta->iterator_fields());
}

CassIterator* cass_iterator_fields_from_function_meta(const CassFunctionMeta* function_meta) {
  return CassIterator::to(function_meta->iterator_fields());
}

CassIterator* cass_iterator_fields_from_aggregate_meta(const CassAggregateMeta* aggregate_meta) {
  return CassIterator::to(aggregate_meta->iterator_fields());
}

const CassKeyspaceMeta* cass_iterator_get_keyspace_meta(const CassIterator* iterator) {
  if (iterator->type() != CASS_ITERATOR_TYPE_KEYSPACE_META) {
    return NULL;
  }
  return CassKeyspaceMeta::to(
        static_cast<const cass::Metadata::KeyspaceIterator*>(
          iterator->from())->keyspace());
}

const CassTableMeta* cass_iterator_get_table_meta(const CassIterator* iterator) {
  if (iterator->type() != CASS_ITERATOR_TYPE_TABLE_META) {
    return NULL;
  }
  return CassTableMeta::to(
        static_cast<const cass::KeyspaceMetadata::TableIterator*>(
          iterator->from())->table());
}

const CassMaterializedViewMeta* cass_iterator_get_materialized_view_meta(const CassIterator* iterator) {
  if (iterator->type() != CASS_ITERATOR_TYPE_MATERIALIZED_VIEW_META) {
    return NULL;
  }
  return CassMaterializedViewMeta::to(
        static_cast<const cass::ViewIteratorBase*>(
          iterator->from())->view());
}

const CassDataType* cass_iterator_get_user_type(const CassIterator* iterator) {
  if (iterator->type() != CASS_ITERATOR_TYPE_TYPE_META) {
    return NULL;
  }
  return CassDataType::to(
        static_cast<const cass::KeyspaceMetadata::TypeIterator*>(
          iterator->from())->type());
}

const CassFunctionMeta* cass_iterator_get_function_meta(const CassIterator* iterator) {
  if (iterator->type() != CASS_ITERATOR_TYPE_FUNCTION_META) {
    return NULL;
  }
  return CassFunctionMeta::to(
        static_cast<const cass::KeyspaceMetadata::FunctionIterator*>(
          iterator->from())->function());
}

const CassAggregateMeta* cass_iterator_get_aggregate_meta(const CassIterator* iterator){
  if (iterator->type() != CASS_ITERATOR_TYPE_AGGREGATE_META) {
    return NULL;
  }
  return CassAggregateMeta::to(
        static_cast<const cass::KeyspaceMetadata::AggregateIterator*>(
          iterator->from())->aggregate());
}

const CassColumnMeta* cass_iterator_get_column_meta(const CassIterator* iterator) {
  if (iterator->type() != CASS_ITERATOR_TYPE_COLUMN_META) {
    return NULL;
  }
  return CassColumnMeta::to(
        static_cast<const cass::TableMetadata::ColumnIterator*>(
          iterator->from())->column());
}

const CassIndexMeta* cass_iterator_get_index_meta(const CassIterator* iterator) {
  if (iterator->type() != CASS_ITERATOR_TYPE_INDEX_META) {
    return NULL;
  }
  return CassIndexMeta::to(
        static_cast<const cass::TableMetadata::IndexIterator*>(
          iterator->from())->index());
}

CassError cass_iterator_get_meta_field_name(const CassIterator* iterator,
                                       const char** name,
                                       size_t* name_length) {
  if (iterator->type() != CASS_ITERATOR_TYPE_META_FIELD) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  const cass::MetadataField* field =
      static_cast<const cass::MetadataFieldIterator*>(iterator->from())->field();
  *name = field->name().data();
  *name_length = field->name().size();
  return CASS_OK;
}

const CassValue* cass_iterator_get_meta_field_value(const CassIterator* iterator) {
  if (iterator->type() != CASS_ITERATOR_TYPE_META_FIELD) {
    return NULL;
  }
  return CassValue::to(
        static_cast<const cass::MetadataFieldIterator*>(
          iterator->from())->field()->value());
}

} // extern "C"

namespace cass {

static const char* table_column_name(const cass::VersionNumber& cassandra_version) {
  return cassandra_version >= VersionNumber(3, 0, 0) ? "table_name" : "columnfamily_name";
}

static const char* signature_column_name(const cass::VersionNumber& cassandra_version) {
  return cassandra_version >= VersionNumber(3, 0, 0) ? "argument_types" : "signature";
}

template <class T>
const T& as_const(const T& x) { return x; }

struct ColumnCompare {
  typedef CassColumnMeta::Ptr argument_type;
  bool operator()(const CassColumnMeta::Ptr& a, const CassColumnMeta::Ptr& b) const {
    bool result;
    if (a->type() == b->type()) {
      result = (a->type() == CASS_COLUMN_TYPE_PARTITION_KEY ||
                a->type() == CASS_COLUMN_TYPE_CLUSTERING_KEY) &&
               a->position() < b->position();
    } else {
      result = a->type() == CASS_COLUMN_TYPE_PARTITION_KEY ||
               (a->type() == CASS_COLUMN_TYPE_CLUSTERING_KEY &&
                b->type() != CASS_COLUMN_TYPE_PARTITION_KEY);
    }
    return result;
  }
};

const KeyspaceMetadata* Metadata::SchemaSnapshot::get_keyspace(const std::string& name) const {
  KeyspaceMetadata::Map::const_iterator i = keyspaces_->find(name);
  if (i == keyspaces_->end()) return NULL;
  return &i->second;
}

const UserType* Metadata::SchemaSnapshot::get_user_type(const std::string& keyspace_name,
                                                        const std::string& type_name) const
{
  KeyspaceMetadata::Map::const_iterator i = keyspaces_->find(keyspace_name);
  if (i == keyspaces_->end()) {
    return NULL;
  }
  return i->second.get_user_type(type_name);
}

std::string Metadata::full_function_name(const std::string& name, const StringVec& signature) {
  std::string full_function_name(name);
  full_function_name.push_back('(');
  for (StringVec::const_iterator i = signature.begin(),
       end = signature.end(); i != end; ++i) {
    std::string argument(*i);
    // Remove white-space
    argument.erase(std::remove_if(argument.begin(), argument.end(), ::isspace),
                   argument.end());
    if (!argument.empty()) {
      if (i != signature.begin()) full_function_name.push_back(',');
      full_function_name.append(argument);
    }
  }
  full_function_name.push_back(')');
  return full_function_name;
}

Metadata::SchemaSnapshot Metadata::schema_snapshot(int protocol_version, const VersionNumber& cassandra_version) const {
  ScopedMutex l(&mutex_);
  return SchemaSnapshot(schema_snapshot_version_,
                        protocol_version,
                        cassandra_version,
                        front_.keyspaces());
}

void Metadata::update_keyspaces(int protocol_version, const VersionNumber& cassandra_version, ResultResponse* result) {
  schema_snapshot_version_++;

  if (is_front_buffer()) {
    ScopedMutex l(&mutex_);
    updating_->update_keyspaces(protocol_version, cassandra_version, result);
  } else {
    updating_->update_keyspaces(protocol_version, cassandra_version, result);
  }
}

void Metadata::update_tables(int protocol_version, const VersionNumber& cassandra_version, ResultResponse* result) {
  schema_snapshot_version_++;

  if (is_front_buffer()) {
    ScopedMutex l(&mutex_);
    updating_->update_tables(protocol_version, cassandra_version, result);
  } else {
    updating_->update_tables(protocol_version, cassandra_version, result);
  }
}

void Metadata::update_views(int protocol_version, const VersionNumber& cassandra_version, ResultResponse* result) {
  schema_snapshot_version_++;

  if (is_front_buffer()) {
    ScopedMutex l(&mutex_);
    updating_->update_views(protocol_version, cassandra_version, result);
  } else {
    updating_->update_views(protocol_version, cassandra_version, result);
  }
}

void Metadata::update_columns(int protocol_version, const VersionNumber& cassandra_version, ResultResponse* result) {
  schema_snapshot_version_++;

  if (is_front_buffer()) {
    ScopedMutex l(&mutex_);
    updating_->update_columns(protocol_version, cassandra_version, cache_, result);
    if (cassandra_version < VersionNumber(3, 0, 0)) {
      updating_->update_legacy_indexes(protocol_version, cassandra_version, result);
    }
  } else {
    updating_->update_columns(protocol_version, cassandra_version, cache_, result);
    if (cassandra_version < VersionNumber(3, 0, 0)) {
      updating_->update_legacy_indexes(protocol_version, cassandra_version, result);
    }
  }
}

void Metadata::update_indexes(int protocol_version, const VersionNumber& cassandra_version, ResultResponse* result) {
  schema_snapshot_version_++;

  if (is_front_buffer()) {
    ScopedMutex l(&mutex_);
    updating_->update_indexes(protocol_version, cassandra_version, result);
  } else {
    updating_->update_indexes(protocol_version, cassandra_version, result);
  }
}

void Metadata::update_user_types(int protocol_version, const VersionNumber& cassandra_version, ResultResponse* result) {
  schema_snapshot_version_++;

  if (is_front_buffer()) {
    ScopedMutex l(&mutex_);
    updating_->update_user_types(protocol_version, cassandra_version, cache_, result);
  } else {
    updating_->update_user_types(protocol_version, cassandra_version, cache_, result);
  }
}

void Metadata::update_functions(int protocol_version, const VersionNumber& cassandra_version, ResultResponse* result) {
  schema_snapshot_version_++;

  if (is_front_buffer()) {
    ScopedMutex l(&mutex_);
    updating_->update_functions(protocol_version, cassandra_version, cache_, result);
  } else {
    updating_->update_functions(protocol_version, cassandra_version, cache_, result);
  }
}

void Metadata::update_aggregates(int protocol_version, const VersionNumber& cassandra_version, ResultResponse* result) {
  schema_snapshot_version_++;

  if (is_front_buffer()) {
    ScopedMutex l(&mutex_);
    updating_->update_aggregates(protocol_version, cassandra_version, cache_, result);
  } else {
    updating_->update_aggregates(protocol_version, cassandra_version, cache_, result);
  }
}

void Metadata::drop_keyspace(const std::string& keyspace_name) {
  schema_snapshot_version_++;

  if (is_front_buffer()) {
    ScopedMutex l(&mutex_);
    updating_->drop_keyspace(keyspace_name);
  } else {
    updating_->drop_keyspace(keyspace_name);
  }
}

void Metadata::drop_table_or_view(const std::string& keyspace_name, const std::string& table_or_view_name) {
  schema_snapshot_version_++;

  if (is_front_buffer()) {
    ScopedMutex l(&mutex_);
    updating_->drop_table_or_view(keyspace_name, table_or_view_name);
  } else {
    updating_->drop_table_or_view(keyspace_name, table_or_view_name);
  }
}

void Metadata::drop_user_type(const std::string& keyspace_name, const std::string& type_name) {
  schema_snapshot_version_++;

  if (is_front_buffer()) {
    ScopedMutex l(&mutex_);
    updating_->drop_user_type(keyspace_name, type_name);
  } else {
    updating_->drop_user_type(keyspace_name, type_name);
  }
}

void Metadata::drop_function(const std::string& keyspace_name, const std::string& full_function_name) {
  schema_snapshot_version_++;

  if (is_front_buffer()) {
    ScopedMutex l(&mutex_);
    updating_->drop_function(keyspace_name, full_function_name);
  } else {
    updating_->drop_function(keyspace_name, full_function_name);
  }
}

void Metadata::drop_aggregate(const std::string& keyspace_name, const std::string& full_aggregate_name) {
  schema_snapshot_version_++;

  if (is_front_buffer()) {
    ScopedMutex l(&mutex_);
    updating_->drop_aggregate(keyspace_name, full_aggregate_name);
  } else {
    updating_->drop_aggregate(keyspace_name, full_aggregate_name);
  }
}

void Metadata::clear_and_update_back(const VersionNumber& cassandra_version) {
  back_.clear();
  updating_ = &back_;
}

void Metadata::swap_to_back_and_update_front() {
  {
    ScopedMutex l(&mutex_);
    schema_snapshot_version_++;
    front_.swap(back_);
  }
  back_.clear();
  updating_ = &front_;
}

void Metadata::clear() {
  {
    ScopedMutex l(&mutex_);
    schema_snapshot_version_ = 0;
    front_.clear();
  }
  back_.clear();
}

const Value* MetadataBase::get_field(const std::string& name) const {
  MetadataField::Map::const_iterator it = fields_.find(name);
  if (it == fields_.end()) return NULL;
  return it->second.value();
}

std::string MetadataBase::get_string_field(const std::string& name) const {
  const Value* value = get_field(name);
  if (value == NULL) return std::string();
  return value->to_string();
}

const Value* MetadataBase::add_field(const RefBuffer::Ptr& buffer, const Row* row, const std::string& name) {
  const Value* value = row->get_by_name(name);
  if (value == NULL) return NULL;
  if (value->size() <= 0) {
    fields_[name] = MetadataField(name);
    return NULL; // Return NULL for "null" columns
  } else {
    fields_[name] = MetadataField(name, *value, buffer);
    return value;
  }
}

void MetadataBase::add_field(const RefBuffer::Ptr& buffer, const Value& value, const std::string& name) {
  fields_[name] = MetadataField(name, value, buffer);
}

void MetadataBase::add_json_list_field(int protocol_version, const Row* row, const std::string& name) {
  const Value* value = row->get_by_name(name);
  if (value == NULL) return;
  if (value->size() <= 0) {
    fields_[name] = MetadataField(name);
    return;
  }

  int32_t buffer_size = value->size();
  ScopedPtr<char[]> buf(new char[buffer_size + 1]);
  memcpy(buf.get(), value->data(), buffer_size);
  buf[buffer_size] = '\0';

  rapidjson::Document d;
  d.ParseInsitu(buf.get());

  if (d.HasParseError()) {
    LOG_ERROR("Unable to parse JSON (array) for column '%s'", name.c_str());
    return;
  }

  if (!d.IsArray()) {
    LOG_DEBUG("Expected JSON array for column '%s' (probably null or empty)", name.c_str());
    fields_[name] = MetadataField(name);
    return;
  }

  Collection collection(CollectionType::list(DataType::Ptr(new DataType(CASS_VALUE_TYPE_TEXT)), false),
                        d.Size());
  for (rapidjson::Value::ConstValueIterator i = d.Begin(); i != d.End(); ++i) {
    collection.append(cass::CassString(i->GetString(), i->GetStringLength()));
  }

  size_t encoded_size = collection.get_items_size(protocol_version);
  RefBuffer::Ptr encoded(RefBuffer::create(encoded_size));

  collection.encode_items(protocol_version, encoded->data());

  Value list(protocol_version,
             collection.data_type(),
             d.Size(),
             encoded->data(),
             encoded_size);
  fields_[name] = MetadataField(name, list, encoded);
}

const Value* MetadataBase::add_json_map_field(int protocol_version, const Row* row, const std::string& name) {
  const Value* value = row->get_by_name(name);
  if (value == NULL) return NULL;
  if (value->size() <= 0) {
    return (fields_[name] = MetadataField(name)).value();
  }

  int32_t buffer_size = value->size();
  ScopedPtr<char[]> buf(new char[buffer_size + 1]);
  memcpy(buf.get(), value->data(), buffer_size);
  buf[buffer_size] = '\0';

  rapidjson::Document d;
  d.ParseInsitu(buf.get());

  if (d.HasParseError()) {
    LOG_ERROR("Unable to parse JSON (object) for column '%s'", name.c_str());
    return (fields_[name] = MetadataField(name)).value();
  }

  if (!d.IsObject()) {
    LOG_DEBUG("Expected JSON object for column '%s' (probably null or empty)", name.c_str());
    fields_[name] = MetadataField(name);
    return (fields_[name] = MetadataField(name)).value();
  }

  Collection collection(CollectionType::map(DataType::Ptr(new DataType(CASS_VALUE_TYPE_TEXT)),
                                            DataType::Ptr(new DataType(CASS_VALUE_TYPE_TEXT)),
                                            false),
                        2 * d.MemberCount());
  for (rapidjson::Value::ConstMemberIterator i = d.MemberBegin(); i != d.MemberEnd(); ++i) {
    collection.append(CassString(i->name.GetString(), i->name.GetStringLength()));
    collection.append(CassString(i->value.GetString(), i->value.GetStringLength()));
  }

  size_t encoded_size = collection.get_items_size(protocol_version);
  RefBuffer::Ptr encoded(RefBuffer::create(encoded_size));

  collection.encode_items(protocol_version, encoded->data());

  Value map(protocol_version,
            collection.data_type(),
            d.MemberCount(),
            encoded->data(),
            encoded_size);

  return (fields_[name] = MetadataField(name, map, encoded)).value();
}

const TableMetadata* KeyspaceMetadata::get_table(const std::string& name) const {
  TableMetadata::Map::const_iterator i = tables_->find(name);
  if (i == tables_->end()) return NULL;
  return i->second.get();
}

const TableMetadata::Ptr& KeyspaceMetadata::get_table(const std::string& name) {
  TableMetadata::Map::iterator i = tables_->find(name);
  if (i == tables_->end()) return TableMetadata::NIL;
  return i->second;
}

void KeyspaceMetadata::add_table(const TableMetadata::Ptr& table) {
  (*tables_)[table->name()] = table;
}

const ViewMetadata* KeyspaceMetadata::get_view(const std::string& name) const {
  ViewMetadata::Map::const_iterator i = views_->find(name);
  if (i == views_->end()) return NULL;
  return i->second.get();
}

const ViewMetadata::Ptr& KeyspaceMetadata::get_view(const std::string& name) {
  ViewMetadata::Map::iterator i = views_->find(name);
  if (i == views_->end()) return ViewMetadata::NIL;
  return i->second;
}

void KeyspaceMetadata::add_view(const ViewMetadata::Ptr& view) {
  (*views_)[view->name()] = view;
}

void KeyspaceMetadata::drop_table_or_view(const std::string& table_or_view_name) {
  TableMetadata::Map::iterator table_it = tables_->find(table_or_view_name);
  if (table_it != tables_->end()) { // The name is for a table, remove the
                                    // table and views from keyspace
    TableMetadata::Ptr table(table_it->second);
    // Cassandra doesn't allow for tables to be dropped while it has active
    // views, but it could be possible for the drop events to arrive out of
    // order.
    for (ViewMetadata::Vec::const_iterator i = table->views().begin(),
         end = table->views().end(); i != end; ++i) {
      views_->erase((*i)->name());
    }
    tables_->erase(table_it);
  } else { // The name is for a view, remove the view from the table and keyspace
    ViewMetadata::Map::iterator view_it = views_->find(table_or_view_name);
    if (view_it != views_->end()) {
      ViewMetadata::Ptr view(view_it->second);
      view->base_table()->drop_view(table_or_view_name);
      views_->erase(view_it);
    }
  }
}

const UserType::Ptr& KeyspaceMetadata::get_or_create_user_type(const std::string& name, bool is_frozen) {
  UserType::Map::iterator i = user_types_->find(name);
  if (i == user_types_->end()) {
    i = user_types_->insert(std::make_pair(name,
                                           UserType::Ptr(new UserType(MetadataBase::name(), name,
                                                                      is_frozen)))).first;
  }
  return i->second;
}

const UserType* KeyspaceMetadata::get_user_type(const std::string& name) const {
  UserType::Map::const_iterator i = user_types_->find(name);
  if (i == user_types_->end()) return NULL;
  return i->second.get();
}

void KeyspaceMetadata::update(int protocol_version, const VersionNumber& cassandra_version, const RefBuffer::Ptr& buffer, const Row* row) {
  add_field(buffer, row, "keyspace_name");
  add_field(buffer, row, "durable_writes");
  if (cassandra_version >= VersionNumber(3, 0, 0))  {
    const Value* map = add_field(buffer, row, "replication");
    if (map != NULL &&
        map->value_type() == CASS_VALUE_TYPE_MAP &&
        is_string_type(map->primary_value_type()) &&
        is_string_type(map->secondary_value_type())) {
      MapIterator iterator(map);
      while (iterator.next()) {
        const Value* key = iterator.key();
        const Value* value  = iterator.value();
        if (key->to_string_ref() == "class") {
          strategy_class_ = value->to_string_ref();
        }
      }
      strategy_options_ = *map;
    }
  } else {
    const Value* value = add_field(buffer, row, "strategy_class");
    if (value != NULL &&
        is_string_type(value->value_type())) {
      strategy_class_ = value->to_string_ref();
    }
    const Value* map = add_json_map_field(protocol_version, row, "strategy_options");
    if (map != NULL) {
      strategy_options_ = *map;
    }
  }
}

void KeyspaceMetadata::drop_user_type(const std::string& type_name) {
  user_types_->erase(type_name);
}

void KeyspaceMetadata::add_function(const FunctionMetadata::Ptr& function) {
  (*functions_)[function->name()] = function;
}

const FunctionMetadata* KeyspaceMetadata::get_function(const std::string& full_function_name) const {
  FunctionMetadata::Map::const_iterator i = functions_->find(full_function_name);
  if (i == functions_->end()) return NULL;
  return i->second.get();
}

void KeyspaceMetadata::drop_function(const std::string& full_function_name) {
  functions_->erase(full_function_name);
}

const AggregateMetadata* KeyspaceMetadata::get_aggregate(const std::string& full_aggregate_name) const {
  AggregateMetadata::Map::const_iterator i = aggregates_->find(full_aggregate_name);
  if (i == aggregates_->end()) return NULL;
  return i->second.get();
}

void KeyspaceMetadata::add_aggregate(const AggregateMetadata::Ptr& aggregate) {
  (*aggregates_)[aggregate->name()] = aggregate;
}

void KeyspaceMetadata::drop_aggregate(const std::string& full_aggregate_name) {
  aggregates_->erase(full_aggregate_name);
}

TableMetadataBase::TableMetadataBase(int protocol_version, const VersionNumber& cassandra_version,
                                     const std::string& name, const RefBuffer::Ptr& buffer, const Row* row)
  : MetadataBase(name) {
  add_field(buffer, row, "keyspace_name");
  add_field(buffer, row, "bloom_filter_fp_chance");
  add_field(buffer, row, "caching");
  add_field(buffer, row, "comment");
  add_field(buffer, row, "default_time_to_live");
  add_field(buffer, row, "gc_grace_seconds");
  add_field(buffer, row, "id");
  add_field(buffer, row, "speculative_retry");
  add_field(buffer, row, "max_index_interval");
  add_field(buffer, row, "min_index_interval");
  add_field(buffer, row, "memtable_flush_period_in_ms");
  add_field(buffer, row, "read_repair_chance");

  if (cassandra_version >= VersionNumber(3, 0, 0)) {
    add_field(buffer, row, "dclocal_read_repair_chance");
    add_field(buffer, row, "crc_check_chance");
    add_field(buffer, row, "compaction");
    add_field(buffer, row, "compression");
    add_field(buffer, row, "extensions");
  } else {
    add_field(buffer, row, "cf_id");
    add_field(buffer, row, "local_read_repair_chance");

    add_field(buffer, row, "compaction_strategy_class");
    add_json_map_field(protocol_version, row, "compaction_strategy_options");
    add_json_map_field(protocol_version, row, "compression_parameters");

    add_json_list_field(protocol_version, row, "column_aliases");
    add_field(buffer, row, "comparator");
    add_field(buffer, row, "subcomparator");
    add_field(buffer, row, "default_validator");
    add_field(buffer, row, "key_alias");
    add_json_list_field(protocol_version, row, "key_aliases");
    add_field(buffer, row, "value_alias");
    add_field(buffer, row, "key_validator");
    add_field(buffer, row, "type");

    add_field(buffer, row, "dropped_columns");
    add_field(buffer, row, "index_interval");
    add_field(buffer, row, "is_dense");
    add_field(buffer, row, "max_compaction_threshold");
    add_field(buffer, row, "min_compaction_threshold");
    add_field(buffer, row, "populate_io_cache_on_flush");
    add_field(buffer, row, "replicate_on_write");
  }
}

const ColumnMetadata* TableMetadataBase::get_column(const std::string& name) const {
  ColumnMetadata::Map::const_iterator i = columns_by_name_.find(name);
  if (i == columns_by_name_.end()) return NULL;
  return i->second.get();
}

void TableMetadataBase::add_column(const ColumnMetadata::Ptr& column) {
  if (columns_by_name_.insert(std::make_pair(column->name(), column)).second) {
    columns_.push_back(column);
  }
}

void TableMetadataBase::clear_columns() {
  columns_.clear();
  columns_by_name_.clear();
  partition_key_.clear();
  clustering_key_.clear();
}

size_t get_column_count(const ColumnMetadata::Vec& columns, CassColumnType type) {
  size_t count = 0;
  for (ColumnMetadata::Vec::const_iterator i = columns.begin(),
       end = columns.end(); i != end; ++i) {
    if ((*i)->type() == type) count++;
  }
  return count;
}

void TableMetadataBase::build_keys_and_sort(int protocol_version, const VersionNumber& cassandra_version, SimpleDataTypeCache& cache) {
  // Also, Reorders columns so that the order is:
  // 1) Parition key
  // 2) Clustering keys
  // 3) Other columns

  if (cassandra_version.major_version() >= 2) {
    partition_key_.resize(get_column_count(columns_, CASS_COLUMN_TYPE_PARTITION_KEY));
    clustering_key_.resize(get_column_count(columns_, CASS_COLUMN_TYPE_CLUSTERING_KEY));
    clustering_key_order_.resize(clustering_key_.size(), CASS_CLUSTERING_ORDER_NONE);
    for (ColumnMetadata::Vec::const_iterator i = columns_.begin(),
         end = columns_.end(); i != end; ++i) {
      ColumnMetadata::Ptr column(*i);
      if (column->type() == CASS_COLUMN_TYPE_PARTITION_KEY &&
          column->position() >= 0 &&
          static_cast<size_t>(column->position()) < partition_key_.size()) {
        partition_key_[column->position()] = column;
      } else if (column->type() == CASS_COLUMN_TYPE_CLUSTERING_KEY &&
                 column->position() >= 0 &&
                 static_cast<size_t>(column->position()) < clustering_key_.size()) {
        clustering_key_[column->position()] = column;
        clustering_key_order_[column->position()] = column->is_reversed() ? CASS_CLUSTERING_ORDER_DESC
                                                                          : CASS_CLUSTERING_ORDER_ASC;
      }
    }

    std::stable_sort(columns_.begin(), columns_.end(), ColumnCompare());
  } else {
    // Cassandra 1.2 requires a lot more work because "system.schema_columns" only
    // contains regular columns.

    // Partition key
    {
      StringRefVec key_aliases;
      const Value* key_aliases_value = get_field("key_aliases");
      if (key_aliases_value != NULL) {
        CollectionIterator iterator(key_aliases_value);
        while (iterator.next()) {
          key_aliases.push_back(iterator.value()->to_string_ref());
        }
      }

      ParseResult::Ptr key_validator
          = DataTypeClassNameParser::parse_with_composite(get_string_field("key_validator"), cache);
      size_t size = key_validator->types().size();
      partition_key_.reserve(size);
      for (size_t i = 0; i < size; ++i) {
        std::string key_alias;
        if (i < key_aliases.size()) {
          key_alias = key_aliases[i].to_string();
        } else {
          std::ostringstream ss("key");
          if (i > 0) {
            ss << i + 1;
          }
          key_alias = ss.str();
        }
        partition_key_.push_back(ColumnMetadata::Ptr(new ColumnMetadata(key_alias,
                                                                        partition_key_.size(),
                                                                        CASS_COLUMN_TYPE_PARTITION_KEY,
                                                                        key_validator->types()[i])));
      }
    }

    // Clustering key
    {
      StringRefVec column_aliases;
      const Value* column_aliases_value = get_field("column_aliases");
      if (column_aliases_value != NULL) {
        CollectionIterator iterator(column_aliases_value);
        while (iterator.next()) {
          column_aliases.push_back(iterator.value()->to_string_ref());
        }
      }

      // TODO: Figure out how to test these special cases and properly document them here
      ParseResult::Ptr comparator
          = DataTypeClassNameParser::parse_with_composite(get_string_field("comparator"), cache);
      size_t size = comparator->types().size();
      if (comparator->is_composite()) {
        if (!comparator->collections().empty() ||
            (column_aliases.size() == size - 1 && comparator->types().back()->value_type() == CASS_VALUE_TYPE_TEXT)) {
          size = size - 1;
        }
      } else {
        size = !column_aliases.empty() || columns_.empty() ? size : 0;
      }
      clustering_key_.reserve(size);
      for (size_t i = 0; i < size; ++i) {
        std::string column_alias;
        if (i < column_aliases.size()) {
          column_alias = column_aliases[i].to_string();
        } else {
          std::ostringstream ss("column");
          if (i > 0) {
            ss << i + 1;
          }
          column_alias = ss.str();
        }
        clustering_key_.push_back(ColumnMetadata::Ptr(new ColumnMetadata(column_alias,
                                                                         clustering_key_.size(),
                                                                         CASS_COLUMN_TYPE_CLUSTERING_KEY,
                                                                         comparator->types()[i])));
        clustering_key_order_.push_back(comparator->reversed()[i] ? CASS_CLUSTERING_ORDER_DESC
                                                                  : CASS_CLUSTERING_ORDER_ASC);
      }
    }

    // TODO: Handle value alias column

    ColumnMetadata::Vec columns(partition_key_.size() + clustering_key_.size() + columns_.size());

    ColumnMetadata::Vec::iterator pos = columns.begin();
    pos = std::copy(partition_key_.begin(), partition_key_.end(), pos);
    pos = std::copy(clustering_key_.begin(), clustering_key_.end(), pos);
    std::copy(columns_.begin(), columns_.end(), pos);

    columns_.swap(columns);
  }
}

const TableMetadata::Ptr TableMetadata::NIL;

TableMetadata::TableMetadata(int protocol_version, const VersionNumber& cassandra_version,
                             const std::string& name, const RefBuffer::Ptr& buffer, const Row* row)
  : TableMetadataBase(protocol_version, cassandra_version, name, buffer, row) {
  add_field(buffer, row, table_column_name(cassandra_version));
  if (cassandra_version >= VersionNumber(3, 0, 0)) {
    add_field(buffer, row, "flags");
  }
}

const ViewMetadata* TableMetadata::get_view(const std::string& name) const {
 ViewMetadata::Vec::const_iterator i = std::lower_bound(views_.begin(), views_.end(), name);
  if (i == views_.end() || (*i)->name() != name) return NULL;
  return i->get();
}

void TableMetadata::add_view(const ViewMetadata::Ptr& view) {
  views_.push_back(view);
}

void TableMetadata::drop_view(const std::string& name) {
 ViewMetadata::Vec::iterator i = std::lower_bound(views_.begin(), views_.end(), name);
  if (i != views_.end() &&  (*i)->name() == name) {
    views_.erase(i);
  }
}

void TableMetadata::sort_views() {
  std::sort(views_.begin(), views_.end());
}

void TableMetadata::key_aliases(SimpleDataTypeCache& cache, KeyAliases* output) const {
  const Value* aliases = get_field("key_aliases");
  if (aliases != NULL) {
    output->reserve(aliases->count());
    CollectionIterator itr(aliases);
    while (itr.next()) {
      output->push_back(itr.value()->to_string());
    }
  }
  if (output->empty()) {// C* 1.2 tables created via CQL2 or thrift don't have col meta or key aliases
    ParseResult::Ptr key_validator_type
        = DataTypeClassNameParser::parse_with_composite(get_string_field("key_validator"), cache);
    const size_t count = key_validator_type->types().size();
    std::ostringstream ss("key");
    for (size_t i = 0; i < count; ++i) {
      if (i > 0) {
        ss.seekp(3);// position after "key"
        ss << i + 1;
      }
      output->push_back(ss.str());
    }
  }
}

const ViewMetadata::Ptr ViewMetadata::NIL;

ViewMetadata::ViewMetadata(int protocol_version, const VersionNumber& cassandra_version,
                           TableMetadata* table,
                           const std::string& name, const RefBuffer::Ptr& buffer, const Row* row)
  : TableMetadataBase(protocol_version, cassandra_version, name, buffer, row)
  , base_table_(table) {
  add_field(buffer, row, "keyspace_name");
  add_field(buffer, row, "view_name");
  add_field(buffer, row, "base_table_name");
  add_field(buffer, row, "base_table_id");
  add_field(buffer, row, "include_all_columns");
  add_field(buffer, row, "where_clause");
}

const IndexMetadata* TableMetadata::get_index(const std::string& name) const {
  IndexMetadata::Map::const_iterator i = indexes_by_name_.find(name);
  if (i == indexes_by_name_.end()) return NULL;
  return i->second.get();
}

void TableMetadata::add_index(const IndexMetadata::Ptr& index) {
  if (indexes_by_name_.insert(std::make_pair(index->name(), index)).second) {
    indexes_.push_back(index);
  }
}

void TableMetadata::clear_indexes() {
  indexes_.clear();
  indexes_by_name_.clear();
}

FunctionMetadata::FunctionMetadata(int protocol_version, const VersionNumber& cassandra_version, SimpleDataTypeCache& cache,
                                   const std::string& name, const Value* signature,
                                   KeyspaceMetadata* keyspace,
                                   const RefBuffer::Ptr& buffer, const Row* row)
  : MetadataBase(Metadata::full_function_name(name, signature->as_stringlist()))
  , simple_name_(name) {
  const Value* value1;
  const Value* value2;

  add_field(buffer, row, "keyspace_name");
  add_field(buffer, row, "function_name");

  value1 = add_field(buffer, row, "argument_names");
  value2 = add_field(buffer, row, "argument_types");
  if (value1 != NULL &&
      value1->value_type() == CASS_VALUE_TYPE_LIST &&
      value1->primary_value_type() == CASS_VALUE_TYPE_VARCHAR &&
      value2 != NULL &&
      value2->value_type() == CASS_VALUE_TYPE_LIST &&
      value2->primary_value_type() == CASS_VALUE_TYPE_VARCHAR) {
    CollectionIterator iterator1(value1);
    CollectionIterator iterator2(value2);
    if (cassandra_version >= VersionNumber(3, 0, 0)) {
      while (iterator1.next() && iterator2.next()) {
        StringRef arg_name(iterator1.value()->to_string_ref());
        DataType::ConstPtr arg_type(DataTypeCqlNameParser::parse(iterator2.value()->to_string(), cache, keyspace));
        args_.push_back(Argument(arg_name, arg_type));
      }
    } else {
      while (iterator1.next() && iterator2.next()) {
        StringRef arg_name(iterator1.value()->to_string_ref());
        DataType::ConstPtr arg_type(DataTypeClassNameParser::parse_one(iterator2.value()->to_string(), cache));
        args_.push_back(Argument(arg_name, arg_type));
      }
    }
  }

  value1 = add_field(buffer, row, "return_type");
  if (value1 != NULL &&
      value1->value_type() == CASS_VALUE_TYPE_VARCHAR) {
    if (cassandra_version >= VersionNumber(3, 0, 0)) {
      return_type_ = DataTypeCqlNameParser::parse(value1->to_string(), cache, keyspace);
    } else {
      return_type_ = DataTypeClassNameParser::parse_one(value1->to_string(), cache);
    }
  }

  value1 = add_field(buffer, row, "body");
  if (value1 != NULL &&
      value1->value_type() == CASS_VALUE_TYPE_VARCHAR) {
     body_ = value1->to_string_ref();
  }

  value1 = add_field(buffer, row, "language");
  if (value1 != NULL &&
      value1->value_type() == CASS_VALUE_TYPE_VARCHAR) {
     language_ = value1->to_string_ref();
  }

  value1 = add_field(buffer, row, "called_on_null_input");
  if (value1 != NULL &&
      value1->value_type() == CASS_VALUE_TYPE_BOOLEAN) {
    called_on_null_input_ = value1->as_bool();
  }
}

const DataType* FunctionMetadata::get_arg_type(StringRef name) const {
  Argument::Vec::const_iterator i = std::find(args_.begin(), args_.end(), name);
  if (i == args_.end()) return NULL;
  return i->type.get();
}

AggregateMetadata::AggregateMetadata(int protocol_version, const VersionNumber& cassandra_version, SimpleDataTypeCache& cache,
                                     const std::string& name, const Value* signature,
                                     KeyspaceMetadata* keyspace,
                                     const RefBuffer::Ptr& buffer, const Row* row)
  : MetadataBase(Metadata::full_function_name(name, signature->as_stringlist()))
  , simple_name_(name) {
  const Value* value;
  const FunctionMetadata::Map& functions = keyspace->functions();

  add_field(buffer, row, "keyspace_name");
  add_field(buffer, row, "aggregate_name");

  value = add_field(buffer, row, "argument_types");
  if (value != NULL &&
      value->value_type() == CASS_VALUE_TYPE_LIST &&
      value->primary_value_type() == CASS_VALUE_TYPE_VARCHAR) {
    CollectionIterator iterator(value);
    if (cassandra_version >= VersionNumber(3, 0, 0)) {
      while (iterator.next()) {
        arg_types_.push_back(DataTypeCqlNameParser::parse(iterator.value()->to_string(), cache, keyspace));
      }
    } else {
      while (iterator.next()) {
        arg_types_.push_back(DataTypeClassNameParser::parse_one(iterator.value()->to_string(), cache));
      }
    }
  }

  value = add_field(buffer, row, "return_type");
  if (value != NULL &&
      value->value_type() == CASS_VALUE_TYPE_VARCHAR) {
    if (cassandra_version >= VersionNumber(3, 0, 0)) {
      return_type_ = DataTypeCqlNameParser::parse(value->to_string(), cache, keyspace);
    } else {
      return_type_ = DataTypeClassNameParser::parse_one(value->to_string(), cache);
    }
  }

  value = add_field(buffer, row, "state_type");
  if (value != NULL &&
      value->value_type() == CASS_VALUE_TYPE_VARCHAR) {
    if (cassandra_version >= VersionNumber(3, 0, 0)) {
      state_type_ = DataTypeCqlNameParser::parse(value->to_string(), cache, keyspace);
    } else {
      state_type_ = DataTypeClassNameParser::parse_one(value->to_string(), cache);
    }
  }

  value = add_field(buffer, row, "final_func");
  if (value != NULL &&
      value->value_type() == CASS_VALUE_TYPE_VARCHAR) {
    StringVec final_func_signature;
    final_func_signature.push_back(state_type_->to_string());
    std::string full_final_func_name(Metadata::full_function_name(value->to_string(), final_func_signature));
    FunctionMetadata::Map::const_iterator i = functions.find(full_final_func_name);
    if (i != functions.end()) final_func_ = i->second;
  }

  value = add_field(buffer, row, "state_func");
  if (value != NULL &&
      value->value_type() == CASS_VALUE_TYPE_VARCHAR) {
    StringVec state_func_signature;
    state_func_signature.push_back(state_type_->to_string());
    CollectionIterator iterator(signature);
    while (iterator.next()) {
      state_func_signature.push_back(iterator.value()->to_string());
    }
    std::string full_state_func_name(Metadata::full_function_name(value->to_string(), state_func_signature));
    FunctionMetadata::Map::const_iterator i = functions.find(full_state_func_name);
    if (i != functions.end()) state_func_ = i->second;
  }

  value = add_field(buffer, row, "initcond");
  if (value != NULL) {
    if (value->value_type() == CASS_VALUE_TYPE_BLOB) {
      init_cond_ = Value(protocol_version, state_type_, value->data(), value->size());
    } else if (cassandra_version >= VersionNumber(3, 0, 0) &&
               value->value_type() == CASS_VALUE_TYPE_VARCHAR) {
      init_cond_ = Value(protocol_version,
                         cache.by_value_type(CASS_VALUE_TYPE_VARCHAR),
                         value->data(), value->size());
    }
  }
}

IndexMetadata::Ptr IndexMetadata::from_row(const std::string& index_name,
                                           const RefBuffer::Ptr& buffer, const Row* row) {
  IndexMetadata::Ptr index(new IndexMetadata(index_name));

  StringRef kind;
  const Value* value = index->add_field(buffer, row, "kind");
  if (value != NULL &&
      value->value_type() == CASS_VALUE_TYPE_VARCHAR) {
    kind = value->to_string_ref();
  }

  const Value* options = index->add_field(buffer, row, "options");
  index->update(kind, options);

  return index;
}

void IndexMetadata::update(StringRef kind, const Value* options) {
  type_ = index_type_from_string(kind);

  if (options != NULL &&
      options->value_type() == CASS_VALUE_TYPE_MAP) {
    MapIterator iterator(options);
    while(iterator.next()) {
      if (iterator.key()->to_string_ref() == "target") {
        target_ = iterator.value()->to_string();
      }
    }
  }

  options_ = *options;
}

IndexMetadata::Ptr IndexMetadata::from_legacy(int protocol_version,
                                              const std::string& index_name, const ColumnMetadata* column,
                                              const RefBuffer::Ptr& buffer, const Row* row) {
  IndexMetadata::Ptr index(new IndexMetadata(index_name));

  index->add_field(buffer, row, "index_name");

  StringRef index_type;
  const Value* value = index->add_field(buffer, row, "index_type");
  if (value != NULL &&
      value->value_type() == CASS_VALUE_TYPE_VARCHAR) {
    index_type = value->to_string_ref();
  }

  const Value* options = index->add_json_map_field(protocol_version, row, "index_options");
  index->update_legacy(index_type, column, options);

  return index;
}

void IndexMetadata::update_legacy(StringRef index_type, const ColumnMetadata* column, const Value* options) {
  type_ = index_type_from_string(index_type);
  target_ = target_from_legacy(column, options);
  options_ = *options;
}

std::string IndexMetadata::target_from_legacy(const ColumnMetadata* column,
                                              const Value* options) {
  std::string column_name(column->name());

  escape_id(column_name);

  if (options != NULL &&
      options->value_type() == CASS_VALUE_TYPE_MAP) {
    MapIterator iterator(options);

    while (iterator.next()) {
      std::string key(iterator.key()->to_string());
      if (key.find("index_keys") != std::string::npos) {
        return "keys("  + column_name + ")";
      } else if (key.find("index_keys_and_values") != std::string::npos) {
        return "entries("  + column_name + ")";
      } else  if (column->data_type()->is_collection()) { // TODO(mpenick): && is_frozen()
        return "full("  + column_name + ")";
      }
    }
  }

  return column_name;
}

CassIndexType IndexMetadata::index_type_from_string(StringRef index_type) {
  if (index_type.iequals("keys")) {
    return CASS_INDEX_TYPE_KEYS;
  } else if (index_type.iequals("custom")) {
    return CASS_INDEX_TYPE_CUSTOM;
  } else if (index_type.iequals("composites")) {
    return CASS_INDEX_TYPE_COMPOSITES;
  }
  return CASS_INDEX_TYPE_UNKNOWN;
}

ColumnMetadata::ColumnMetadata(int protocol_version, const VersionNumber& cassandra_version, SimpleDataTypeCache& cache,
                               const std::string& name,
                               KeyspaceMetadata* keyspace,
                               const RefBuffer::Ptr& buffer, const Row* row)
  : MetadataBase(name)
  , type_(CASS_COLUMN_TYPE_REGULAR)
  , position_(0)
  , is_reversed_(false) {
  const Value* value;

  add_field(buffer, row, "keyspace_name");
  add_field(buffer, row, table_column_name(cassandra_version));
  add_field(buffer, row, "column_name");

  if (cassandra_version >= VersionNumber(3, 0, 0)) {
    value = add_field(buffer, row, "clustering_order");
    if (value != NULL &&
        value->value_type() == CASS_VALUE_TYPE_VARCHAR &&
        value->to_string_ref().iequals("desc")) {
      is_reversed_ = true;
    }

    add_field(buffer, row, "column_name_bytes");

    value = add_field(buffer, row, "kind");
    if (value != NULL &&
        value->value_type() == CASS_VALUE_TYPE_VARCHAR) {
      StringRef type = value->to_string_ref();
      if (type == "partition_key") {
        type_ = CASS_COLUMN_TYPE_PARTITION_KEY;
      } else  if (type == "clustering") {
        type_ = CASS_COLUMN_TYPE_CLUSTERING_KEY;
      } else  if (type == "static") {
        type_ = CASS_COLUMN_TYPE_STATIC;
      } else {
        type_ = CASS_COLUMN_TYPE_REGULAR;
      }
    }

    value = add_field(buffer, row, "position");
    if (value != NULL &&
        value->value_type() == CASS_VALUE_TYPE_INT) {
      position_ = value->as_int32();
      if (position_ < 0) position_ = 0;
    }

    value = add_field(buffer, row, "type");
    if (value != NULL &&
        value->value_type() == CASS_VALUE_TYPE_VARCHAR) {
      std::string type(value->to_string());
      data_type_ = DataTypeCqlNameParser::parse(type, cache, keyspace);
    }
  } else {
    value = add_field(buffer, row, "type");
    if (value != NULL &&
        value->value_type() == CASS_VALUE_TYPE_VARCHAR) {
      StringRef type = value->to_string_ref();
      if (type == "partition_key") {
        type_ = CASS_COLUMN_TYPE_PARTITION_KEY;
      } else  if (type == "clustering_key") {
        type_ = CASS_COLUMN_TYPE_CLUSTERING_KEY;
      } else  if (type == "static") {
        type_ = CASS_COLUMN_TYPE_STATIC;
      } else  if (type == "compact_value") {
        type_ = CASS_COLUMN_TYPE_COMPACT_VALUE;
      } else {
        type_ = CASS_COLUMN_TYPE_REGULAR;
      }
    }

    value = add_field(buffer, row, "component_index");
    // For C* 2.0 to 2.2 this is "null" for single component partition keys
    // so the default position of 0 works. C* 1.2 and below don't use this.
    if (value != NULL &&
        value->value_type() == CASS_VALUE_TYPE_INT) {
      position_ = value->as_int32();
    }

    value = add_field(buffer, row, "validator");
    if (value != NULL &&
        value->value_type() == CASS_VALUE_TYPE_VARCHAR) {
      std::string validator(value->to_string());
      data_type_ = DataTypeClassNameParser::parse_one(validator, cache);
      is_reversed_ = DataTypeClassNameParser::is_reversed(validator);
    }

    add_field(buffer, row, "index_type");
    add_field(buffer, row, "index_name");
    add_json_map_field(protocol_version, row, "index_options");
  }
}

void Metadata::InternalData::update_keyspaces(int protocol_version, const VersionNumber& cassandra_version,
                                              ResultResponse* result) {
  RefBuffer::Ptr buffer = result->buffer();
  ResultIterator rows(result);

  while (rows.next()) {
    std::string keyspace_name;
    const Row* row = rows.row();

    if (!row->get_string_by_name("keyspace_name", &keyspace_name)) {
      LOG_ERROR("Unable to get column value for 'keyspace_name'");
      continue;
    }

    KeyspaceMetadata* keyspace = get_or_create_keyspace(keyspace_name);
    keyspace->update(protocol_version, cassandra_version, buffer, row);
  }
}

void Metadata::InternalData::update_tables(int protocol_version, const VersionNumber& cassandra_version,
                                           ResultResponse* result) {
  RefBuffer::Ptr buffer = result->buffer();

  ResultIterator rows(result);

  std::string keyspace_name;
  std::string table_name;
  KeyspaceMetadata* keyspace = NULL;

  while (rows.next()) {
    std::string temp_keyspace_name;
    const Row* row = rows.row();

    if (!row->get_string_by_name("keyspace_name", &temp_keyspace_name) ||
        !row->get_string_by_name(table_column_name(cassandra_version), &table_name)) {
      LOG_ERROR("Unable to get column value for 'keyspace_name' or '%s'", table_column_name(cassandra_version));
      continue;
    }

    if (keyspace_name != temp_keyspace_name) {
      keyspace_name = temp_keyspace_name;
      keyspace = get_or_create_keyspace(keyspace_name);
    }

    keyspace->add_table(TableMetadata::Ptr(new TableMetadata(protocol_version, cassandra_version, table_name, buffer, row)));
  }
}

void Metadata::InternalData::update_views(int protocol_version, const VersionNumber& cassandra_version,
                                          ResultResponse* result) {
  RefBuffer::Ptr buffer = result->buffer();

  ResultIterator rows(result);

  std::string keyspace_name;
  std::string view_name;
  KeyspaceMetadata* keyspace = NULL;

  TableMetadata::Vec updated_tables;

  while (rows.next()) {
    std::string temp_keyspace_name;
    std::string base_table_name;
    const Row* row = rows.row();

    if (!row->get_string_by_name("keyspace_name", &temp_keyspace_name) ||
        !row->get_string_by_name("view_name", &view_name)) {
      LOG_ERROR("Unable to get column value for 'keyspace_name' and 'view_name'");
      continue;
    }

    if (keyspace_name != temp_keyspace_name) {
      keyspace_name = temp_keyspace_name;
      keyspace = get_or_create_keyspace(keyspace_name);
    }

    if (!row->get_string_by_name("base_table_name", &base_table_name)) {
      LOG_ERROR("Unable to get column value for 'base_table_name'");
      continue;
    }

    TableMetadata::Ptr table(keyspace->get_table(base_table_name));
    if (!table) {
      LOG_ERROR("No table metadata for view with base table name '%s'", base_table_name.c_str());
      continue;
    }

    ViewMetadata::Ptr view(new ViewMetadata(protocol_version, cassandra_version, table.get(), view_name, buffer, row));
    keyspace->add_view(view);
    table->add_view(view);
    updated_tables.push_back(table);
  }

  for (TableMetadata::Vec::iterator i = updated_tables.begin(),
       end = updated_tables.end(); i != end; ++i) {
    (*i)->sort_views();
  }
}

void Metadata::InternalData::update_user_types(int protocol_version, const VersionNumber& cassandra_version, SimpleDataTypeCache& cache, ResultResponse* result) {
  ResultIterator rows(result);

  std::string keyspace_name;
  KeyspaceMetadata* keyspace = NULL;

  while (rows.next()) {
    std::string temp_keyspace_name;
    std::string type_name;
    const Row* row = rows.row();

    if (!row->get_string_by_name("keyspace_name", &temp_keyspace_name) ||
        !row->get_string_by_name("type_name", &type_name)) {
      LOG_ERROR("Unable to get column value for 'keyspace_name' or 'type_name'");
      continue;
    }

    if (keyspace_name != temp_keyspace_name) {
      keyspace_name = temp_keyspace_name;
      keyspace = get_or_create_keyspace(keyspace_name);
    }

    const Value* names_value = row->get_by_name("field_names");
    if (names_value == NULL || names_value->is_null()) {
      LOG_ERROR("'field_name's column for keyspace \"%s\" and type \"%s\" is null",
                keyspace_name.c_str(), type_name.c_str());
      continue;
    }

    const Value* types_value = row->get_by_name("field_types");
    if (types_value == NULL || types_value->is_null()) {
      LOG_ERROR("'field_type's column for keyspace '%s' and type '%s' is null",
                keyspace_name.c_str(), type_name.c_str());
      continue;
    }

    CollectionIterator names(names_value);
    CollectionIterator types(types_value);

    UserType::FieldVec fields;

    while (names.next()) {
      if(!types.next()) {
        LOG_ERROR("The number of 'field_type's doesn\"t match the number of field_names for keyspace \"%s\" and type \"%s\"",
                  keyspace_name.c_str(), type_name.c_str());
        break;
      }

      const cass::Value* name = names.value();
      const cass::Value* type = types.value();

      if (name->is_null() || type->is_null()) {
        LOG_ERROR("'field_name' or 'field_type' is null for keyspace \"%s\" and type \"%s\"",
                  keyspace_name.c_str(), type_name.c_str());
        break;
      }

      std::string field_name(name->to_string());

      DataType::ConstPtr data_type;

      if (cassandra_version >= VersionNumber(3, 0, 0)) {
        data_type = DataTypeCqlNameParser::parse(type->to_string(), cache, keyspace);
      } else {
        data_type = DataTypeClassNameParser::parse_one(type->to_string(), cache);
      }

      if (!data_type) {
        LOG_ERROR("Invalid 'field_type' for field \"%s\", keyspace \"%s\" and type \"%s\"",
                  field_name.c_str(),
                  keyspace_name.c_str(),
                  type_name.c_str());
        break;
      }

      fields.push_back(UserType::Field(field_name, data_type));
    }

    keyspace->get_or_create_user_type(type_name, false)->set_fields(fields);
  }
}

void Metadata::InternalData::update_functions(int protocol_version, const VersionNumber& cassandra_version, SimpleDataTypeCache& cache,
                                              ResultResponse* result) {
  RefBuffer::Ptr buffer = result->buffer();

  ResultIterator rows(result);

  std::string keyspace_name;
  KeyspaceMetadata* keyspace = NULL;

  while (rows.next()) {
    std::string temp_keyspace_name;
    std::string function_name;
    const Row* row = rows.row();

    const Value* signature = row->get_by_name(signature_column_name(cassandra_version));
    if (!row->get_string_by_name("keyspace_name", &temp_keyspace_name) ||
        !row->get_string_by_name("function_name", &function_name) ||
        signature == NULL) {
      LOG_ERROR("Unable to get column value for 'keyspace_name', 'function_name' or 'signature'");
      continue;
    }

    if (keyspace_name != temp_keyspace_name) {
      keyspace_name = temp_keyspace_name;
      keyspace = get_or_create_keyspace(keyspace_name);
    }

    keyspace->add_function(FunctionMetadata::Ptr(new FunctionMetadata(protocol_version, cassandra_version, cache,
                                                                      function_name, signature,
                                                                      keyspace,
                                                                      buffer, row)));

  }
}

void Metadata::InternalData::update_aggregates(int protocol_version, const VersionNumber& cassandra_version, SimpleDataTypeCache& cache, ResultResponse* result) {
  RefBuffer::Ptr buffer = result->buffer();

  ResultIterator rows(result);

  std::string keyspace_name;
  KeyspaceMetadata* keyspace = NULL;

  while (rows.next()) {
    std::string temp_keyspace_name;
    std::string aggregate_name;
    const Row* row = rows.row();

    const Value* signature = row->get_by_name(signature_column_name(cassandra_version));
    if (!row->get_string_by_name("keyspace_name", &temp_keyspace_name) ||
        !row->get_string_by_name("aggregate_name", &aggregate_name) ||
        signature == NULL) {
      LOG_ERROR("Unable to get column value for 'keyspace_name', 'aggregate_name' or 'signature'");
      continue;
    }

    if (keyspace_name != temp_keyspace_name) {
      keyspace_name = temp_keyspace_name;
      keyspace = get_or_create_keyspace(keyspace_name);
    }

    keyspace->add_aggregate(AggregateMetadata::Ptr(new AggregateMetadata(protocol_version, cassandra_version, cache,
                                                                         aggregate_name, signature,
                                                                         keyspace,
                                                                         buffer, row)));
  }
}

void Metadata::InternalData::drop_keyspace(const std::string& keyspace_name) {
  keyspaces_->erase(keyspace_name);
}

void Metadata::InternalData::drop_table_or_view(const std::string& keyspace_name,
                                                const std::string& table_or_view_name) {
  KeyspaceMetadata::Map::iterator i = keyspaces_->find(keyspace_name);
  if (i == keyspaces_->end()) return;
  i->second.drop_table_or_view(table_or_view_name);
}

void Metadata::InternalData::drop_user_type(const std::string& keyspace_name, const std::string& type_name) {
  KeyspaceMetadata::Map::iterator i = keyspaces_->find(keyspace_name);
  if (i == keyspaces_->end()) return;
  i->second.drop_user_type(type_name);
}

void Metadata::InternalData::drop_function(const std::string& keyspace_name, const std::string& full_function_name) {
  KeyspaceMetadata::Map::iterator i = keyspaces_->find(keyspace_name);
  if (i == keyspaces_->end()) return;
  i->second.drop_function(full_function_name);
}

void Metadata::InternalData::drop_aggregate(const std::string& keyspace_name, const std::string& full_aggregate_name) {
  KeyspaceMetadata::Map::iterator i = keyspaces_->find(keyspace_name);
  if (i == keyspaces_->end()) return;
  i->second.drop_aggregate(full_aggregate_name);
}

void Metadata::InternalData::update_columns(int protocol_version, const VersionNumber& cassandra_version, SimpleDataTypeCache& cache, ResultResponse* result) {
  RefBuffer::Ptr buffer = result->buffer();

  ResultIterator rows(result);

  std::string keyspace_name;
  std::string table_or_view_name;
  std::string column_name;

  KeyspaceMetadata* keyspace = NULL;
  TableMetadataBase::Ptr table_or_view;

  while (rows.next()) {
    std::string temp_keyspace_name;
    std::string temp_table_or_view_name;
    const Row* row = rows.row();

    if (!row->get_string_by_name("keyspace_name", &temp_keyspace_name) ||
        !row->get_string_by_name(table_column_name(cassandra_version), &temp_table_or_view_name) ||
        !row->get_string_by_name("column_name", &column_name)) {
      LOG_ERROR("Unable to get column value for 'keyspace_name', '%s' or 'column_name'",
                table_column_name(cassandra_version));
      continue;
    }

    if (keyspace_name != temp_keyspace_name) {
      keyspace_name = temp_keyspace_name;
      keyspace = get_or_create_keyspace(keyspace_name);
      table_or_view_name.clear();
    }

    if (table_or_view_name != temp_table_or_view_name) {
      // Build keys for the previous table
      if (table_or_view) {
        table_or_view->build_keys_and_sort(protocol_version, cassandra_version, cache);
      }
      table_or_view_name = temp_table_or_view_name;
      table_or_view = TableMetadataBase::Ptr(keyspace->get_table(table_or_view_name));
      if (!table_or_view)  {
        table_or_view = TableMetadataBase::Ptr(keyspace->get_view(table_or_view_name));
        if (!table_or_view) continue;
      }
      table_or_view->clear_columns();
    }

    if (table_or_view) {
      table_or_view->add_column(ColumnMetadata::Ptr(new ColumnMetadata(protocol_version, cassandra_version, cache, column_name,
                                                                       keyspace, buffer, row)));
    }
  }

  // Build keys for the last table
  if (table_or_view) {
    table_or_view->build_keys_and_sort(protocol_version, cassandra_version, cache);
  }
}

void Metadata::InternalData::update_legacy_indexes(int protocol_version, const VersionNumber& cassandra_version, ResultResponse* result) {
  RefBuffer::Ptr buffer = result->buffer();

  ResultIterator rows(result);

  std::string keyspace_name;
  std::string table_name;
  std::string column_name;

  KeyspaceMetadata* keyspace = NULL;
  TableMetadata::Ptr table;

  while (rows.next()) {
    std::string temp_keyspace_name;
    std::string temp_table_name;
    const Row* row = rows.row();

    if (!row->get_string_by_name("keyspace_name", &temp_keyspace_name) ||
        !row->get_string_by_name(table_column_name(cassandra_version), &temp_table_name) ||
        !row->get_string_by_name("column_name", &column_name)) {
      LOG_ERROR("Unable to get column value for 'keyspace_name', '%s' or 'column_name'",
                table_column_name(cassandra_version));
      continue;
    }

    if (keyspace_name != temp_keyspace_name) {
      keyspace_name = temp_keyspace_name;
      keyspace = get_or_create_keyspace(keyspace_name);
      table_name.clear();
    }

    if (table_name != temp_table_name) {
      table_name = temp_table_name;
      table = keyspace->get_table(table_name);
      if (!table) continue;
      table->clear_indexes();
    }

    if (table) {
      const ColumnMetadata* column = table->get_column(column_name);
      if (column != NULL) {
        const Value* index_type = column->get_field("index_type");
        if (index_type != NULL &&
            index_type->value_type() == CASS_VALUE_TYPE_VARCHAR) {
          std::string index_name = column->get_string_field("index_name");
          table->add_index(IndexMetadata::from_legacy(protocol_version, index_name, column, buffer, row));
        }
      }
    }
  }
}

void Metadata::InternalData::update_indexes(int protocol_version, const VersionNumber& cassandra_version, ResultResponse* result) {
  RefBuffer::Ptr buffer = result->buffer();

  ResultIterator rows(result);

  std::string keyspace_name;
  std::string table_name;
  std::string index_name;

  KeyspaceMetadata* keyspace = NULL;
  TableMetadata::Ptr table;

  while (rows.next()) {
    std::string temp_keyspace_name;
    std::string temp_table_name;
    const Row* row = rows.row();

    if (!row->get_string_by_name("keyspace_name", &temp_keyspace_name) ||
        !row->get_string_by_name("table_name", &temp_table_name) ||
        !row->get_string_by_name("index_name", &index_name)) {
      LOG_ERROR("Unable to get column value for 'keyspace_name', 'table_name' or 'index_name'");
      continue;
    }

    if (keyspace_name != temp_keyspace_name) {
      keyspace_name = temp_keyspace_name;
      keyspace = get_or_create_keyspace(keyspace_name);
      table_name.clear();
    }

    if (table_name != temp_table_name) {
      table_name = temp_table_name;
      table = keyspace->get_table(table_name);
      if (!table) continue;
      table->clear_indexes();
    }

    table->add_index(IndexMetadata::from_row(index_name, buffer, row));
  }
}

KeyspaceMetadata* Metadata::InternalData::get_or_create_keyspace(const std::string& name) {
  KeyspaceMetadata::Map::iterator i = keyspaces_->find(name);
  if (i == keyspaces_->end()) {
    i = keyspaces_->insert(std::make_pair(name, KeyspaceMetadata(name))).first;
  }
  return &i->second;
}

} // namespace cass

