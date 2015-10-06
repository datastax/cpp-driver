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

#include "metadata.hpp"

#include "buffer.hpp"
#include "collection_iterator.hpp"
#include "external_types.hpp"
#include "iterator.hpp"
#include "logger.hpp"
#include "map_iterator.hpp"
#include "result_iterator.hpp"
#include "row.hpp"
#include "row_iterator.hpp"
#include "scoped_lock.hpp"
#include "value.hpp"

#include "third_party/rapidjson/rapidjson/document.h"

#include <cmath>

extern "C" {

void cass_schema_free(const CassSchema* schema) {
  delete schema->from();
}

const CassSchemaMeta* cass_schema_get_keyspace(const CassSchema* schema,
                                               const char* keyspace) {
  return cass_schema_get_keyspace_n(schema, keyspace, strlen(keyspace));
}

const CassSchemaMeta* cass_schema_get_keyspace_n(const CassSchema* schema,
                                                 const char* keyspace,
                                                 size_t keyspace_length) {
  return CassSchemaMeta::to(schema->get_keyspace(std::string(keyspace, keyspace_length)));
}

CassSchemaMetaType cass_schema_meta_type(const CassSchemaMeta* meta) {
  return meta->type();
}

const CassDataType* cass_schema_get_udt(const CassSchema* schema,
                                        const char* keyspace,
                                        const char* type_name) {
  return cass_schema_get_udt_n(schema,
                               keyspace, strlen(keyspace),
                               type_name, strlen(type_name));
}

const CassDataType* cass_schema_get_udt_n(const CassSchema* schema,
                                          const char* keyspace,
                                          size_t keyspace_length,
                                          const char* type_name,
                                          size_t type_name_length) {
  std::string keyspace_id(keyspace, keyspace_length);
  std::string type_name_id(type_name, type_name_length);
  cass::SharedRefPtr<cass::UserType> user_type
      = schema->get_type(cass::to_cql_id(keyspace_id),
                              cass::to_cql_id(type_name_id));
  return CassDataType::to(user_type.get());
}

const CassSchemaMeta* cass_schema_meta_get_entry(const CassSchemaMeta* meta,
                                                 const char* name) {
  return cass_schema_meta_get_entry_n(meta, name, strlen(name));
}

const CassSchemaMeta* cass_schema_meta_get_entry_n(const CassSchemaMeta* meta,
                                                   const char* name,
                                                   size_t name_length) {
  return CassSchemaMeta::to(meta->get_entry(std::string(name, name_length)));
}

const CassSchemaMetaField* cass_schema_meta_get_field(const CassSchemaMeta* meta,
                                                      const char* name) {
  return cass_schema_meta_get_field_n(meta, name, strlen(name));
}

const CassSchemaMetaField* cass_schema_meta_get_field_n(const CassSchemaMeta* meta,
                                                        const char* name,
                                                        size_t name_length) {
  return CassSchemaMetaField::to(meta->get_field(std::string(name, name_length)));
}

void cass_schema_meta_field_name(const CassSchemaMetaField* field,
                                 const char** name,
                                 size_t* name_length) {
  const std::string& n = field->name();
  *name = n.data();
  *name_length = n.length();
}

const CassValue* cass_schema_meta_field_value(const CassSchemaMetaField* field) {
  return CassValue::to(field->value());
}

CassIterator* cass_iterator_from_schema(const CassSchema* schema) {
  return CassIterator::to(schema->iterator());
}

CassIterator* cass_iterator_from_schema_meta(const CassSchemaMeta* meta) {
  return CassIterator::to(meta->iterator());
}

const CassSchemaMeta* cass_iterator_get_schema_meta(CassIterator* iterator) {
  if (iterator->type() != CASS_ITERATOR_TYPE_SCHEMA_META) {
    return NULL;
  }
  return CassSchemaMeta::to(
        static_cast<cass::SchemaMetadataIterator*>(
          iterator->from())->meta());
}

CassIterator* cass_iterator_fields_from_schema_meta(const CassSchemaMeta* meta) {
  return CassIterator::to(meta->iterator_fields());
}

const CassSchemaMetaField* cass_iterator_get_schema_meta_field(CassIterator* iterator) {
  if (iterator->type() != CASS_ITERATOR_TYPE_SCHEMA_META_FIELD) {
    return NULL;
  }
  return CassSchemaMetaField::to(
        static_cast<cass::SchemaMetadataFieldIterator*>(
          iterator->from())->field());
}

} // extern "C"

namespace cass {

template <class T>
const T& as_const(const T& x) { return x; }

template <class T>
const T* find_by_name(const std::map<std::string, T>& map, const std::string& name) {
  typename std::map<std::string, T>::const_iterator it = map.find(name);
  if (it == map.end()) return NULL;
  return &it->second;
}

const SchemaMetadata* Metadata::Snapshot::get_keyspace(const std::string& name) const {
  KeyspaceMetadata::Map::const_iterator i = keyspaces_->find(name);
  if (i == keyspaces_->end()) return NULL;
  return i->second.get();
}

SharedRefPtr<UserType> Metadata::Snapshot::get_type(const std::string& keyspace_name,
                                                    const std::string& type_name) const
{
 KeyspaceMetadata::Map::const_iterator i = as_const(keyspaces_)->find(keyspace_name);
 if (i == as_const(keyspaces_)->end()) {
   return SharedRefPtr<UserType>();
 }
 return i->second->get_type(type_name);
}

Metadata::Snapshot* Metadata::snapshot() const {
  ScopedMutex l(&mutex_);
  return new Snapshot(snapshot_);
}

void Metadata::update_keyspaces(ResultResponse* result) {
  ScopedMutex l(&mutex_);
  KeyspacePointerMap updates;

  SharedRefPtr<RefBuffer> buffer = result->buffer();
  result->decode_first_row();
  ResultIterator rows(result);

  while (rows.next()) {
    std::string keyspace_name;
    const Row* row = rows.row();

    if (!row->get_string_by_name("keyspace_name", &keyspace_name)) {
      LOG_ERROR("Unable to column value for 'keyspace_name'");
      continue;
    }

    KeyspaceMetadata::Ptr keyspace = snapshot_.get_or_create_keyspace(keyspace_name);
    keyspace->update(protocol_version_, buffer, row);
    updates.insert(std::make_pair(keyspace_name, keyspace));
  }

  for (KeyspacePointerMap::const_iterator i = updates.begin(); i != updates.end(); ++i) {
    token_map_.update_keyspace(i->first, *i->second);
  }
}

void Metadata::update_tables(ResultResponse* tables_result, ResultResponse* columns_result) {
  ScopedMutex l(&mutex_);
  SharedRefPtr<RefBuffer> buffer = tables_result->buffer();

  tables_result->decode_first_row();
  ResultIterator rows(tables_result);

  std::string keyspace_name;
  std::string columnfamily_name;
  KeyspaceMetadata::Ptr keyspace;

  while (rows.next()) {
    std::string temp_keyspace_name;
    const Row* row = rows.row();

    if (!row->get_string_by_name("keyspace_name", &temp_keyspace_name) ||
        !row->get_string_by_name("columnfamily_name", &columnfamily_name)) {
      LOG_ERROR("Unable to column value for 'keyspace_name' or 'columnfamily_name'");
      continue;
    }

    if (keyspace_name != temp_keyspace_name) {
      keyspace_name = temp_keyspace_name;
      keyspace = snapshot_.get_or_create_keyspace(keyspace_name);
    }

    keyspace->get_or_create_table(columnfamily_name)->update(protocol_version_, buffer, row);
  }

  update_columns(columns_result);
}

void Metadata::update_types(ResultResponse* result) {
  ScopedMutex l(&mutex_);
  result->decode_first_row();
  ResultIterator rows(result);

  while (rows.next()) {
    std::string keyspace_name;
    std::string type_name;
    const Row* row = rows.row();

    if (!row->get_string_by_name("keyspace_name", &keyspace_name) ||
        !row->get_string_by_name("type_name", &type_name)) {
      LOG_ERROR("Unable to column value for 'keyspace_name' or 'type_name'");
      continue;
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

      SharedRefPtr<DataType> data_type = TypeParser::parse_one(type->to_string());
      if (!data_type) {
        LOG_ERROR("Invalid 'field_type' for field \"%s\", keyspace \"%s\" and type \"%s\"",
                  field_name.c_str(),
                  keyspace_name.c_str(),
                  type_name.c_str());
        break;
      }

      fields.push_back(UserType::Field(field_name, data_type));
    }

    snapshot_.get_or_create_keyspace(keyspace_name)->add_type(
          SharedRefPtr<UserType>(new UserType(keyspace_name, type_name, fields)));
  }
}

void Metadata::update_columns(ResultResponse* result) {
  // DO NOT LOCK: This is private and called from update_tables().

  SharedRefPtr<RefBuffer> buffer = result->buffer();

  result->decode_first_row();
  ResultIterator rows(result);

  std::string keyspace_name;
  std::string columnfamily_name;
  std::string column_name;
  TableMetadata* table = NULL;
  std::set<TableMetadata*> cleared_tables;
  while (rows.next()) {
    std::string temp_keyspace_name;
    std::string temp_columnfamily_name;
    const Row* row = rows.row();

    if (!row->get_string_by_name("keyspace_name", &temp_keyspace_name) ||
        !row->get_string_by_name("columnfamily_name", &temp_columnfamily_name) ||
        !row->get_string_by_name("column_name", &column_name)) {
      LOG_ERROR("Unable to column value for 'keyspace_name', 'columnfamily_name' or 'column_name'");
      continue;
    }

    if (keyspace_name != temp_keyspace_name ||
        columnfamily_name != temp_columnfamily_name) {
      keyspace_name = temp_keyspace_name;
      columnfamily_name = temp_columnfamily_name;
      table = snapshot_.get_or_create_keyspace(keyspace_name)->get_or_create_table(columnfamily_name);
      if (cleared_tables.insert(table).second) {
        table->clear_columns();
      }
    }

    table->get_or_create(column_name)->update(protocol_version_, buffer, row);
  }
}

void Metadata::drop_keyspace(const std::string& keyspace_name) {
  snapshot_.keyspaces_->erase(keyspace_name);
}

void Metadata::drop_table(const std::string& keyspace_name, const std::string& table_name) {
  KeyspaceMetadata::Map::iterator it = snapshot_.keyspaces_->find(keyspace_name);
  if (it == snapshot_.keyspaces_->end()) return;
  it->second->drop_table(table_name);
}

void Metadata::drop_type(const std::string& keyspace_name, const std::string& type_name) {
  KeyspaceMetadata::Map::iterator it = snapshot_.keyspaces_->find(keyspace_name);
  if (it == snapshot_.keyspaces_->end()) return;
  it->second->drop_type(type_name);
}

void Metadata::clear() {
  snapshot_.keyspaces_->clear();
}

const SchemaMetadataField* SchemaMetadata::get_field(const std::string& name) const {
  return find_by_name<SchemaMetadataField>(fields_, name);
}

std::string SchemaMetadata::get_string_field(const std::string& name) const {
  const SchemaMetadataField* field = get_field(name);
  if (field == NULL) return std::string();
  return field->value()->to_string();
}

void SchemaMetadata::add_field(const SharedRefPtr<RefBuffer>& buffer, const Row* row, const std::string& name) {
  const Value* value = row->get_by_name(name);
  if (value == NULL) return;
  if (value->size() <= 0) {
    fields_[name] = SchemaMetadataField(name);
    return;
  }
  fields_[name] = SchemaMetadataField(name, *value, buffer);
}

void SchemaMetadata::add_json_list_field(int version, const Row* row, const std::string& name) {
  const Value* value = row->get_by_name(name);
  if (value == NULL) return;
  if (value->size() <= 0) {
    fields_[name] = SchemaMetadataField(name);
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
    fields_[name] = SchemaMetadataField(name);
    return;
  }

  Collection collection(CollectionType::list(SharedRefPtr<DataType>(new DataType(CASS_VALUE_TYPE_TEXT))),
                        d.Size());
  for (rapidjson::Value::ConstValueIterator i = d.Begin(); i != d.End(); ++i) {
    collection.append(cass::CassString(i->GetString(), i->GetStringLength()));
  }

  size_t encoded_size = collection.get_items_size(version);
  SharedRefPtr<RefBuffer> encoded(RefBuffer::create(encoded_size));

  collection.encode_items(version, encoded->data());

  Value list(version,
             collection.data_type(),
             d.Size(),
             encoded->data(),
             encoded_size);
  fields_[name] = SchemaMetadataField(name, list, encoded);
}

void SchemaMetadata::add_json_map_field(int version, const Row* row, const std::string& name) {
  const Value* value = row->get_by_name(name);
  if (value == NULL) return;
  if (value->size() <= 0) {
    fields_[name] = SchemaMetadataField(name);
    return;
  }

  int32_t buffer_size = value->size();
  ScopedPtr<char[]> buf(new char[buffer_size + 1]);
  memcpy(buf.get(), value->data(), buffer_size);
  buf[buffer_size] = '\0';

  rapidjson::Document d;
  d.ParseInsitu(buf.get());

  if (d.HasParseError()) {
    LOG_ERROR("Unable to parse JSON (object) for column '%s'", name.c_str());
    return;
  }

  if (!d.IsObject()) {
    LOG_DEBUG("Expected JSON object for column '%s' (probably null or empty)", name.c_str());
    fields_[name] = SchemaMetadataField(name);
    return;
  }



  Collection collection(CollectionType::map(SharedRefPtr<DataType>(new DataType(CASS_VALUE_TYPE_TEXT)),
                                            SharedRefPtr<DataType>(new DataType(CASS_VALUE_TYPE_TEXT))),
                        2 * d.MemberCount());
  for (rapidjson::Value::ConstMemberIterator i = d.MemberBegin(); i != d.MemberEnd(); ++i) {
    collection.append(CassString(i->name.GetString(), i->name.GetStringLength()));
    collection.append(CassString(i->value.GetString(), i->value.GetStringLength()));
  }

  size_t encoded_size = collection.get_items_size(version);
  SharedRefPtr<RefBuffer> encoded(RefBuffer::create(encoded_size));

  collection.encode_items(version, encoded->data());

  Value map(version,
            collection.data_type(),
            d.MemberCount(),
            encoded->data(),
            encoded_size);
  fields_[name] = SchemaMetadataField(name, map, encoded);
}

const SchemaMetadata* KeyspaceMetadata::get_entry(const std::string& name) const {
  return find_by_name<TableMetadata>(tables_, name);
}

SharedRefPtr<UserType> KeyspaceMetadata::get_type(const std::string& type_name) const {
  UserTypeMap::const_iterator i = types_.find(type_name);
  if (i == types_.end()) return SharedRefPtr<UserType>();
  return i->second;
}

void KeyspaceMetadata::update(int version, const SharedRefPtr<RefBuffer>& buffer, const Row* row) {
  add_field(buffer, row, "keyspace_name");
  add_field(buffer, row, "durable_writes");
  add_field(buffer, row, "strategy_class");
  add_json_map_field(version, row, "strategy_options");
}

void KeyspaceMetadata::add_type(const SharedRefPtr<UserType>& user_type) {
  types_[user_type->type_name()] = user_type;
}

void KeyspaceMetadata::drop_table(const std::string& table_name) {
  tables_.erase(table_name);
}

void KeyspaceMetadata::drop_type(const std::string& type_name) {
  types_.erase(type_name);
}

const SchemaMetadata* TableMetadata::get_entry(const std::string& name) const {
  return find_by_name<ColumnMetadata>(columns_, name);
}

void TableMetadata::update(int version, const SharedRefPtr<RefBuffer>& buffer, const Row* row) {
  add_field(buffer, row, "keyspace_name");
  add_field(buffer, row, "columnfamily_name");
  add_field(buffer, row, "bloom_filter_fp_chance");
  add_field(buffer, row, "caching");
  add_field(buffer, row, "cf_id");
  add_json_list_field(version, row, "column_aliases");
  add_field(buffer, row, "comment");
  add_field(buffer, row, "compaction_strategy_class");
  add_json_map_field(version, row, "compaction_strategy_options");
  add_field(buffer, row, "comparator");
  add_json_map_field(version, row, "compression_parameters");
  add_field(buffer, row, "default_time_to_live");
  add_field(buffer, row, "default_validator");
  add_field(buffer, row, "dropped_columns");
  add_field(buffer, row, "gc_grace_seconds");
  add_field(buffer, row, "index_interval");
  add_field(buffer, row, "id");
  add_field(buffer, row, "is_dense");
  add_field(buffer, row, "key_alias");
  add_json_list_field(version, row, "key_aliases");
  add_field(buffer, row, "key_validator");
  add_field(buffer, row, "local_read_repair_chance");
  add_field(buffer, row, "max_compaction_threshold");
  add_field(buffer, row, "max_index_interval");
  add_field(buffer, row, "memtable_flush_period_in_ms");
  add_field(buffer, row, "min_compaction_threshold");
  add_field(buffer, row, "min_index_interval");
  add_field(buffer, row, "populate_io_cache_on_flush");
  add_field(buffer, row, "read_repair_chance");
  add_field(buffer, row, "replicate_on_write");
  add_field(buffer, row, "speculative_retry");
  add_field(buffer, row, "subcomparator");
  add_field(buffer, row, "type");
  add_field(buffer, row, "value_alias");
}

void TableMetadata::key_aliases(KeyAliases* output) const {
  const SchemaMetadataField* aliases = get_field("key_aliases");
  if (aliases != NULL) {
    output->reserve(aliases->value()->count());
    CollectionIterator itr(aliases->value());
    while (itr.next()) {
      output->push_back(itr.value()->to_string());
    }
  }
  if (output->empty()) {// C* 1.2 tables created via CQL2 or thrift don't have col meta or key aliases
    SharedRefPtr<ParseResult> key_validator_type = TypeParser::parse_with_composite(get_string_field("key_validator"));
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

void ColumnMetadata::update(int version, const SharedRefPtr<RefBuffer>& buffer, const Row* row) {
  add_field(buffer, row, "keyspace_name");
  add_field(buffer, row, "columnfamily_name");
  add_field(buffer, row, "column_name");
  add_field(buffer, row, "type");
  add_field(buffer, row, "component_index");
  add_field(buffer, row, "validator");
  add_field(buffer, row, "index_name");
  add_json_map_field(version, row, "index_options");
  add_field(buffer, row, "index_type");
}

void Metadata::Snapshot::get_table_key_columns(const std::string& ks_name,
                                               const std::string& table_name,
                                               std::vector<std::string>* output) const {
  const SchemaMetadata* ks_meta = get_keyspace(ks_name);
  if (ks_meta != NULL) {
    const SchemaMetadata* table_meta = static_cast<const KeyspaceMetadata*>(ks_meta)->get_entry(table_name);
    if (table_meta != NULL) {
      static_cast<const TableMetadata*>(table_meta)->key_aliases(output);
    }
  }
}

} // namespace cass

