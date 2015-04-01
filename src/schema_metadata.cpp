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

#include "schema_metadata.hpp"

#include "buffer.hpp"
#include "buffer_collection.hpp"
#include "collection_iterator.hpp"
#include "iterator.hpp"
#include "logger.hpp"
#include "map_iterator.hpp"
#include "result_iterator.hpp"
#include "row.hpp"
#include "row_iterator.hpp"
#include "types.hpp"
#include "value.hpp"

#include <boost/algorithm/string.hpp>
#include <rapidjson/document.h>

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
  return CassSchemaMeta::to(schema->get(std::string(keyspace, keyspace_length)));
}

CassSchemaMetaType cass_schema_meta_type(const CassSchemaMeta* meta) {
  return meta->type();
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
const T* find_by_name(const std::map<std::string, T>& map, const std::string& name) {
  typename std::map<std::string, T>::const_iterator it = map.find(name);
  if (it == map.end()) return NULL;
  return &it->second;
}

const SchemaMetadata* Schema::get(const std::string& name) const {
  return find_by_name<KeyspaceMetadata>(*keyspaces_, name);
}

Schema::KeyspacePointerMap Schema::update_keyspaces(ResultResponse* result) {
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

    KeyspaceMetadata* ks_meta = get_or_create(keyspace_name);
    ks_meta->update(protocol_version_, buffer, row);
    updates.insert(std::make_pair(keyspace_name, ks_meta));
  }
  return updates;
}

void Schema::update_tables(ResultResponse* table_result, ResultResponse* col_result) {
  SharedRefPtr<RefBuffer> buffer = table_result->buffer();

  table_result->decode_first_row();
  ResultIterator rows(table_result);

  std::string keyspace_name;
  std::string columnfamily_name;
  KeyspaceMetadata* keyspace_metadata = NULL;

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
      keyspace_metadata = get_or_create(keyspace_name);
    }

    keyspace_metadata->get_or_create(columnfamily_name)->update(protocol_version_, buffer, row);
  }
  update_columns(col_result);
}

void Schema::update_columns(ResultResponse* result) {
  SharedRefPtr<RefBuffer> buffer = result->buffer();

  result->decode_first_row();
  ResultIterator rows(result);

  std::string keyspace_name;
  std::string columnfamily_name;
  std::string column_name;
  TableMetadata* table_metadata = NULL;
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
      table_metadata = get_or_create(keyspace_name)->get_or_create(columnfamily_name);
      std::pair<std::set<TableMetadata*>::iterator, bool> pos_success = cleared_tables.insert(table_metadata);
      if (pos_success.second) {
        table_metadata->clear_columns();
      }
    }

    table_metadata->get_or_create(column_name)->update(protocol_version_, buffer, row);
  }
}

void Schema::drop_keyspace(const std::string& keyspace_name) {
  keyspaces_->erase(keyspace_name);
}

void Schema::drop_table(const std::string& keyspace_name, const std::string& table_name) {
  KeyspaceMetadata::Map::iterator it = keyspaces_->find(keyspace_name);
  if (it == keyspaces_->end()) return;
  it->second.drop_table(table_name);
}

void Schema::clear() {
  keyspaces_->clear();
}

const SchemaMetadataField* SchemaMetadata::get_field(const std::string& name) const {
  return find_by_name<SchemaMetadataField>(fields_, name);
}

std::string SchemaMetadata::get_string_field(const std::string& name) const {
  const SchemaMetadataField* field = get_field(name);
  if (field == NULL) return std::string();
  return field->value()->buffer().to_string();
}

void SchemaMetadata::add_field(const SharedRefPtr<RefBuffer>& buffer, const Row* row, const std::string& name) {
  const Value* value = row->get_by_name(name);
  if (value == NULL) return;
  if (value->buffer().size() <= 0) {
    fields_[name] = SchemaMetadataField(name);
    return;
  }
  fields_[name] = SchemaMetadataField(name, *value, buffer);
}

void SchemaMetadata::add_json_list_field(int version, const Row* row, const std::string& name) {
  const Value* value = row->get_by_name(name);
  if (value == NULL) return;
  if (value->buffer().size() <= 0) {
    fields_[name] = SchemaMetadataField(name);
    return;
  }

  int32_t buffer_size = value->buffer().size();
  ScopedPtr<char[]> buf(new char[buffer_size + 1]);
  memcpy(buf.get(), value->buffer().data(), buffer_size);
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

  BufferCollection collection(false, d.Size());
  for (rapidjson::Value::ConstValueIterator i = d.Begin(); i != d.End(); ++i) {
    collection.append(i->GetString(), i->GetStringLength());
  }

  int encoded_size = collection.calculate_size(version);
  SharedRefPtr<RefBuffer> encoded(RefBuffer::create(encoded_size));

  collection.encode(version, encoded->data());

  Value map(CASS_VALUE_TYPE_LIST,
            CASS_VALUE_TYPE_TEXT,
            CASS_VALUE_TYPE_UNKNOWN,
            d.Size(),
            encoded->data(),
            encoded_size);
  fields_[name] = SchemaMetadataField(name, map, encoded);
}

void SchemaMetadata::add_json_map_field(int version, const Row* row, const std::string& name) {
  const Value* value = row->get_by_name(name);
  if (value == NULL) return;
  if (value->buffer().size() <= 0) {
    fields_[name] = SchemaMetadataField(name);
    return;
  }

  int32_t buffer_size = value->buffer().size();
  ScopedPtr<char[]> buf(new char[buffer_size + 1]);
  memcpy(buf.get(), value->buffer().data(), buffer_size);
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

  BufferCollection collection(true, 2 * d.MemberCount());
  for (rapidjson::Value::ConstMemberIterator i = d.MemberBegin(); i != d.MemberEnd(); ++i) {
    collection.append(i->name.GetString(), i->name.GetStringLength());
    collection.append(i->value.GetString(), i->value.GetStringLength());
  }

  int encoded_size = collection.calculate_size(version);
  SharedRefPtr<RefBuffer> encoded(RefBuffer::create(encoded_size));

  collection.encode(version, encoded->data());

  Value map(CASS_VALUE_TYPE_MAP,
            CASS_VALUE_TYPE_TEXT,
            CASS_VALUE_TYPE_TEXT,
            d.MemberCount(),
            encoded->data(),
            encoded_size);
  fields_[name] = SchemaMetadataField(name, map, encoded);
}

const SchemaMetadata* KeyspaceMetadata::get_entry(const std::string& name) const {
  return find_by_name<TableMetadata>(tables_, name);
}

void KeyspaceMetadata::update(int version, const SharedRefPtr<RefBuffer>& buffer, const Row* row) {
  add_field(buffer, row, "keyspace_name");
  add_field(buffer, row, "durable_writes");
  add_field(buffer, row, "strategy_class");
  add_json_map_field(version, row, "strategy_options");
}

void KeyspaceMetadata::drop_table(const std::string& table_name) {
  tables_.erase(table_name);
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

const TableMetadata::KeyAliases& TableMetadata::key_aliases() const {
  if (key_aliases_.empty()) {
    const SchemaMetadataField* aliases = get_field("key_aliases");
    if (aliases != NULL) {
      key_aliases_.resize(aliases->value()->count());
      CollectionIterator itr(aliases->value());
      size_t i = 0;
      while (itr.next()) {
        const BufferPiece& buf = itr.value()->buffer();
        key_aliases_[i++].assign(buf.data(), buf.size());
      }
    }
    if (key_aliases_.empty()) {// C* 1.2 tables created via CQL2 or thrift don't have col meta or key aliases
      TypeDescriptor key_validator_type = TypeParser::parse(get_string_field("key_validator"));
      const size_t count = key_validator_type.component_count();
      std::ostringstream ss("key");
      for (size_t i = 0; i < count; ++i) {
        if (i > 0) {
          ss.seekp(3);// position after "key"
          ss << i + 1;
        }
        key_aliases_.push_back(ss.str());
      }
    }
  }
  return key_aliases_;
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

void Schema::get_table_key_columns(const std::string& ks_name,
                                   const std::string& table_name,
                                   std::vector<std::string>* output) const {
  const SchemaMetadata* ks_meta = get(ks_name);
  if (ks_meta != NULL) {
    const SchemaMetadata* table_meta = static_cast<const KeyspaceMetadata*>(ks_meta)->get_entry(table_name);
    if (table_meta != NULL) {
      *output = static_cast<const TableMetadata*>(table_meta)->key_aliases();
    }
  }
}

} // namespace cass

