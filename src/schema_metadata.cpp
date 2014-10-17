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

#include "schema_metadata.hpp"

#include "buffer.hpp"
#include "cass_type_parser.hpp"
#include "map_iterator.hpp"
#include "row.hpp"
#include "types.hpp"
#include "value.hpp"

#include "third_party/boost/boost/algorithm/string.hpp"

#include <cmath>

extern "C" {

void keyspace_meta_out(const cass::KeyspaceMetadata& ksm,
                       CassKeyspaceMeta* output) {
  cass::cass_string_of_string(ksm.name_, &output->name);
  output->durable_writes = ksm.durable_writes_;
  cass::cass_string_of_string(ksm.strategy_class_, &output->replication_strategy);
  cass::cass_string_of_string(ksm.strategy_options_json_, &output->strategy_options);
}

CassError cass_meta_get_keyspace(CassSchemaMetadata* meta,
                                 CassString keyspace_name,
                                 CassKeyspaceMeta* output) {
  const cass::KeyspaceModel* ks = meta->get_keyspace(std::string(keyspace_name.data, keyspace_name.length));
  if (ks != NULL) {
    keyspace_meta_out(ks->meta(), output);
    return CASS_OK;
  }
  return CASS_ERROR_NAME_DOES_NOT_EXIST;
}

CassError cass_meta_get_keyspace2(CassSchemaMetadata* meta,
                                  const char* keyspace_name,
                                  CassKeyspaceMeta* output) {
  return cass_meta_get_keyspace(meta, cass_string_init(keyspace_name), output);
}

void column_family_meta_out(const cass::ColumnFamilyMetadata& cfm,
                            CassColumnFamilyMeta* output) {
  cass::cass_string_of_string(cfm.name_, &output->name);
  output->bloom_filter_fp_chance = cfm.bloom_filter_fp_chance_;
  cass::cass_string_of_string(cfm.caching_, &output->caching);
  memcpy(output->cf_id, cfm.cf_id_, sizeof(cfm.cf_id_));
  cass::cass_string_of_string(cfm.column_aliases_, &output->column_aliases);
  cass::cass_string_of_string(cfm.comment_, &output->comment);
  cass::cass_string_of_string(cfm.compaction_strategy_class_, &output->compaction_strategy_class);
  cass::cass_string_of_string(cfm.compaction_strategy_options_, &output->compaction_strategy_options);
  cass::cass_string_of_string(cfm.comparator_, &output->comparator);
  cass::cass_string_of_string(cfm.compression_parameters_, &output->compression_parameters);
  output->default_time_to_live = cfm.default_time_to_live_;
  cass::cass_string_of_string(cfm.default_validator_, &output->default_validator);
  // not decoded: dropped_columns (map<string, bigint>)
  // didn't see a lot of value in creating an iterator just to get this out
  // could see encoding json, but not sure what a client would need it for
  output->gc_grace_seconds = cfm.gc_grace_seconds_;
  output->index_interval = cfm.index_interval_;
  output->is_dense = cfm.is_dense_;
  cass::cass_string_of_string(cfm.key_aliases_, &output->key_aliases);
  cass::cass_string_of_string(cfm.key_validator_, &output->key_validator);
  output->local_read_repair_chance = cfm.local_read_repair_chance_;
  output->max_compaction_threshold = cfm.max_compaction_threshold_;
  output->max_index_interval = cfm.max_index_interval_;
  output->memtable_flush_period_in_ms = cfm.memtable_flush_period_in_ms_;
  output->min_compaction_threshold = cfm.min_compaction_threshold_;
  output->min_index_interval = cfm.min_index_interval_;
  output->read_repair_chance = cfm.read_repair_chance_;
  cass::cass_string_of_string(cfm.speculative_retry_, &output->speculative_retry);
  cass::cass_string_of_string(cfm.subcomparator_, &output->subcomparator);
  cass::cass_string_of_string(cfm.type_, &output->type);
  cass::cass_string_of_string(cfm.value_alias_, &output->value_alias);
}

CassError cass_meta_get_column_family(CassSchemaMetadata* schema_meta,
                                      CassString keyspace_name,
                                      CassString column_family_name,
                                      CassColumnFamilyMeta* output) {
  const cass::KeyspaceModel* ks = schema_meta->get_keyspace(std::string(keyspace_name.data, keyspace_name.length));
  if (ks != NULL) {
    const cass::ColumnFamilyModel* cf = ks->get_column_family(std::string(column_family_name.data, column_family_name.length));
    if (cf != NULL) {
      column_family_meta_out(cf->meta(), output);
      return CASS_OK;
    }
  }
  return CASS_ERROR_NAME_DOES_NOT_EXIST;
}

CassError cass_meta_get_column_family2(CassSchemaMetadata* schema_meta,
                                       const char* keyspace_name,
                                       const char* column_family_name,
                                       CassColumnFamilyMeta* output) {
  return cass_meta_get_column_family(schema_meta, cass_string_init(keyspace_name), cass_string_init(column_family_name), output);
}

void column_meta_out(const cass::ColumnMetadata& cm, CassColumnMeta* output) {
  cass::cass_string_of_string(cm.name_, &output->name);
  output->component_index = cm.component_index_;
  cass::cass_string_of_string(cm.index_name_, &output->index_name);
  cass::cass_string_of_string(cm.index_options_, &output->index_options);
  cass::cass_string_of_string(cm.index_type_, &output->index_type);
  output->kind = cm.kind_;
  output->type = cm.type_.type_;
  output->is_reversed = static_cast<cass_bool_t>(cm.type_.is_reversed_);
  cass::cass_string_of_string(cm.validator_, &output->validator);
}

CassError cass_meta_get_column(CassSchemaMetadata* schema_meta,
                               CassString keyspace_name,
                               CassString column_family_name,
                               CassString column_name,
                               CassColumnMeta* output) {
  const cass::KeyspaceModel* ks = schema_meta->get_keyspace(std::string(keyspace_name.data, keyspace_name.length));
  if (ks != NULL) {
    const cass::ColumnFamilyModel* cf = ks->get_column_family(std::string(column_family_name.data, column_family_name.length));
    if (cf != NULL) {
      const cass::ColumnModel* cm = cf->get_column(std::string(column_name.data, column_name.length));
      if (cm != NULL) {
        column_meta_out(cm->meta(), output);
        return CASS_OK;
      }
    }
  }
  return CASS_ERROR_NAME_DOES_NOT_EXIST;
}

CassError cass_meta_get_column2(CassSchemaMetadata* schema_meta,
                                const char* keyspace_name,
                                const char* column_family_name,
                                const char* column_name,
                                CassColumnMeta* output) {
  return cass_meta_get_column(schema_meta, cass_string_init(keyspace_name), cass_string_init(column_family_name), cass_string_init(column_name), output);
}

CassIterator* cass_iterator_keyspaces(CassSchemaMetadata* schema_meta) {
  return CassIterator::to(new cass::KeyspaceIterator(schema_meta->keyspaces()));
}

CassError cass_iterator_get_keyspace_meta(CassIterator* ks_itr,
                                          CassKeyspaceMeta* output) {
  assert(ks_itr->type() == cass::CASS_ITERATOR_TYPE_KS_META);
  keyspace_meta_out(static_cast<cass::KeyspaceIterator*>(ks_itr->from())->meta(), output);
  return CASS_OK;
}

CassIterator* cass_iterator_column_families_from_keyspace(CassIterator* ks_itr) {
  assert(ks_itr->type() == cass::CASS_ITERATOR_TYPE_KS_META);
  const cass::KeyspaceModel& ksm = static_cast<cass::KeyspaceIterator*>(ks_itr->from())->model();
  return CassIterator::to(new cass::ColumnFamilyIterator(ksm.column_families()));
}

CassIterator* cass_iterator_column_families(CassSchemaMetadata* schema_meta,
                                            CassString keyspace_name) {
  const cass::KeyspaceModel* ksm = schema_meta->get_keyspace(std::string(keyspace_name.data, keyspace_name.length));
  if (ksm != NULL) {
    return CassIterator::to(new cass::ColumnFamilyIterator(ksm->column_families()));
  }
  return NULL;
}

CassIterator* cass_iterator_column_families2(CassSchemaMetadata* schema_meta,
                                             const char* keyspace_name) {
  return cass_iterator_column_families(schema_meta, cass_string_init(keyspace_name));
}

CassError cass_iterator_get_column_family_meta(CassIterator* cf_itr,
                                               CassColumnFamilyMeta* output) {
  assert(cf_itr->type() == cass::CASS_ITERATOR_TYPE_CF_META);
  column_family_meta_out(static_cast<cass::ColumnFamilyIterator*>(cf_itr->from())->meta(), output);
  return CASS_OK;
}

CassIterator* cass_iterator_columns(CassSchemaMetadata* schema_meta,
                                    CassString keyspace_name,
                                    CassString column_family_name) {
  const cass::KeyspaceModel* ksm = schema_meta->get_keyspace(std::string(keyspace_name.data, keyspace_name.length));
  if (ksm != NULL) {
    const cass::ColumnFamilyModel* cfm = ksm->get_column_family(std::string(column_family_name.data, column_family_name.length));
    if (cfm != NULL) {
      return CassIterator::to(new cass::ColumnIterator(cfm->columns()));
    }
  }
  return NULL;
}

CassIterator* cass_iterator_column2(CassSchemaMetadata* schema_meta,
                                    const char* keyspace_name,
                                    const char* column_family_name) {
  return cass_iterator_columns(schema_meta, cass_string_init(keyspace_name), cass_string_init(column_family_name));
}

CassIterator* cass_iterator_columns_from_column_family(CassIterator *cf_itr) {
  assert(cf_itr->type() == cass::CASS_ITERATOR_TYPE_CF_META);
  const cass::ColumnFamilyModel& cfm = static_cast<cass::ColumnFamilyIterator*>(cf_itr->from())->model();
  return CassIterator::to(new cass::ColumnIterator(cfm.columns()));
}

CassError cass_iterator_get_column_meta(CassIterator *col_itr,
                                        CassColumnMeta *output) {
  assert(col_itr->type() == cass::CASS_ITERATOR_TYPE_COL_META);
  column_meta_out(static_cast<cass::ColumnIterator*>(col_itr->from())->meta(), output);
  return CASS_OK;
}

void cass_meta_free(CassSchemaMetadata *meta) {
  delete meta->from();
}

} // extern "C"

namespace cass {

void get_optional_string(const Value* v, std::string* output, const char* deflt = NULL) {
  if (v != NULL && !v->is_null()) {
    output->assign(v->buffer().data(), v->buffer().size());
  } else {//TODO: remove?
    if (deflt != NULL) {
      *output = deflt;
    }
  }
}

std::string string_from_column(const Row* row, const boost::string_ref& name) {
  std::string value;
  get_optional_string(row->get_by_name(name), &value);
  return value;
}

ColumnMetadata::ColumnTypeMapper::ColumnTypeMapper() {
  name_type_map_["PARITION_KEY"] = CASS_COLUMN_TYPE_PARITION_KEY;
  name_type_map_["CLUSTERING_KEY"] = CASS_COLUMN_TYPE_CLUSTERING_KEY;
  name_type_map_["REGULAR"] = CASS_COLUMN_TYPE_REGULAR;
  name_type_map_["COMPACT_VALUE"] = CASS_COLUMN_TYPE_COMPACT_VALUE;
  name_type_map_["STATIC"] = CASS_COLUMN_TYPE_STATIC;
}

CassColumnType ColumnMetadata::ColumnTypeMapper::operator [](const std::string& col_type) const {
  NameTypeMap::const_iterator itr = name_type_map_.find(boost::to_upper_copy(col_type));
  if (itr != name_type_map_.end()) {
    return itr->second;
  } else {
    return CASS_COLUMN_TYPE_UNKNOWN;
  }
}

const ColumnMetadata::ColumnTypeMapper ColumnMetadata::type_map_;

ColumnMetadata::ColumnMetadata()
    : component_index_(-1)
    , kind_(CASS_COLUMN_TYPE_UNKNOWN) {}

ColumnMetadata ColumnMetadata::from_schema_row(const Row* schema_row) {
  ColumnMetadata cm;

  get_optional_string(schema_row->get_by_name("column_name"), &cm.name_);

  std::string kind;
  get_optional_string(schema_row->get_by_name("type"), &kind);
  cm.kind_ = type_map_[kind];

  cass_value_get_int32(CassValue::to(schema_row->get_by_name("component_index")), &cm.component_index_);

  get_optional_string(schema_row->get_by_name("validator"), &cm.validator_);
  cm.type_ = CassTypeParser::parse(cm.validator_);

  get_optional_string(schema_row->get_by_name("index_name"), &cm.index_name_);
  get_optional_string(schema_row->get_by_name("index_options"), &cm.index_options_);
  get_optional_string(schema_row->get_by_name("index_type"), &cm.index_type_);

  return cm;
}

std::string ColumnMetadata::keyspace_name_from_row(const Row* schema_row) {
  return string_from_column(schema_row, "keyspace_name");
}

std::string ColumnMetadata::column_family_name_from_row(const Row* schema_row) {
  return string_from_column(schema_row, "columnfamily_name");
}

std::string ColumnMetadata::name_from_row(const Row* schema_row) {
  return string_from_column(schema_row, "column_name");
}

ColumnFamilyMetadata::ColumnFamilyMetadata()
  : bloom_filter_fp_chance_(NAN)
  , default_time_to_live_(-1)
  , gc_grace_seconds_(-1)
  , index_interval_(-1)
  , is_dense_(cass_false)
  , local_read_repair_chance_(NAN)
  , max_compaction_threshold_(-1)
  , max_index_interval_(-1)
  , memtable_flush_period_in_ms_(-1)
  , min_compaction_threshold_(-1)
  , min_index_interval_(-1)
  , read_repair_chance_(NAN) {
  memset(&cf_id_, 0, sizeof(cf_id_));
}

ColumnFamilyMetadata ColumnFamilyMetadata::from_schema_row(const Row* schema_row) {
  ColumnFamilyMetadata cfm;
  get_optional_string(schema_row->get_by_name("columnfamily_name"), &cfm.name_);
  cass_value_get_double(CassValue::to(schema_row->get_by_name("bloom_filter_fp_chance")), &cfm.bloom_filter_fp_chance_);
  get_optional_string(schema_row->get_by_name("caching"), &cfm.caching_);
  cass_value_get_uuid(CassValue::to(schema_row->get_by_name("cf_id")), cfm.cf_id_);
  get_optional_string(schema_row->get_by_name("column_aliases"), &cfm.column_aliases_);
  get_optional_string(schema_row->get_by_name("comment"), &cfm.comment_);
  get_optional_string(schema_row->get_by_name("compaction_strategy_class"), &cfm.compaction_strategy_class_);
  get_optional_string(schema_row->get_by_name("compaction_strategy_options"), &cfm.compaction_strategy_options_);
  get_optional_string(schema_row->get_by_name("comparator"), &cfm.comparator_);
  get_optional_string(schema_row->get_by_name("compression_parameters"), &cfm.compression_parameters_);
  cass_value_get_int32(CassValue::to(schema_row->get_by_name("default_time_to_live")), &cfm.default_time_to_live_);
  get_optional_string(schema_row->get_by_name("default_validator"), &cfm.default_validator_);

  MapIterator itr(schema_row->get_by_name("dropped_columns"));
  while(itr.next()) {
    const Value* key = itr.key();
    cass_int64_t& ts = cfm.dropped_columns_[std::string(key->buffer().data(), key->buffer().size())];
    cass_value_get_int64(CassValue::to(itr.value()), &ts);
  }

  cass_value_get_int32(CassValue::to(schema_row->get_by_name("gc_grace_seconds")), &cfm.gc_grace_seconds_);
  cass_value_get_int32(CassValue::to(schema_row->get_by_name("index_interval")), &cfm.index_interval_);
  cass_value_get_bool(CassValue::to(schema_row->get_by_name("is_dense")), &cfm.is_dense_);
  get_optional_string(schema_row->get_by_name("key_aliases"), &cfm.key_aliases_);
  get_optional_string(schema_row->get_by_name("key_validator"), &cfm.key_validator_);
  cass_value_get_double(CassValue::to(schema_row->get_by_name("local_read_repair_chance")), &cfm.local_read_repair_chance_);
  cass_value_get_int32(CassValue::to(schema_row->get_by_name("max_compaction_threshold")), &cfm.max_compaction_threshold_);
  cass_value_get_int32(CassValue::to(schema_row->get_by_name("max_index_interval")), &cfm.max_index_interval_);
  cass_value_get_int32(CassValue::to(schema_row->get_by_name("memtable_flush_period_in_ms")), &cfm.memtable_flush_period_in_ms_);
  cass_value_get_int32(CassValue::to(schema_row->get_by_name("min_compaction_threshold")), &cfm.min_compaction_threshold_);
  cass_value_get_int32(CassValue::to(schema_row->get_by_name("min_index_interval")), &cfm.min_index_interval_);
  cass_value_get_double(CassValue::to(schema_row->get_by_name("read_repair_chance")), &cfm.read_repair_chance_);
  get_optional_string(schema_row->get_by_name("speculative_retry"), &cfm.speculative_retry_);
  get_optional_string(schema_row->get_by_name("subcomparator"), &cfm.subcomparator_);
  get_optional_string(schema_row->get_by_name("type"), &cfm.type_);
  get_optional_string(schema_row->get_by_name("value_alias"), &cfm.value_alias_);
  return cfm;
}

std::string ColumnFamilyMetadata::keyspace_name_from_row(const Row* schema_row) {
  return string_from_column(schema_row, "keyspace_name");
}

std::string ColumnFamilyMetadata::name_from_row(const Row* schema_row) {
  return string_from_column(schema_row, "columnfamily_name");
}

void ColumnFamilyModel::update_column(const Row* schema_row) {
  columns_[ColumnMetadata::name_from_row(schema_row)]
      .set_meta(ColumnMetadata::from_schema_row(schema_row));
}

const ColumnModel* ColumnFamilyModel::get_column(const std::string& col_name) const {
  const ColumnModel* cm = NULL;
  ColumnModelMap::const_iterator i = columns_.find(col_name);
  if (i != columns_.end()) {
    cm = &i->second;
  }
  return cm;
}

KeyspaceMetadata::KeyspaceMetadata()
  : durable_writes_(cass_false) {}


KeyspaceMetadata KeyspaceMetadata::from_schema_row(const Row* schema_row) {
  KeyspaceMetadata ksm;
  get_optional_string(schema_row->get_by_name("keyspace_name"), &ksm.name_);
  cass_value_get_bool(CassValue::to(schema_row->get_by_name("durable_writes")), &ksm.durable_writes_);
  get_optional_string(schema_row->get_by_name("strategy_class"), &ksm.strategy_class_);

  const Value* v = schema_row->get_by_name("strategy_options");
  if (v != NULL && !v->is_null()) {
    json_to_map(v->buffer(), &ksm.strategy_options_);
    ksm.strategy_options_json_.assign(v->buffer().data(), v->buffer().size());
  }

  return ksm;
}

std::string KeyspaceMetadata::name_from_row(const Row* schema_row) {
  return string_from_column(schema_row, "keyspace_name");
}

void KeyspaceModel::update_column_family(const Row* schema_row) {
  cfs_[ColumnFamilyMetadata::name_from_row(schema_row)]
      .set_meta(ColumnFamilyMetadata::from_schema_row(schema_row));
}

void KeyspaceModel::update_column(const Row* schema_row) {
  cfs_[ColumnMetadata::column_family_name_from_row(schema_row)]
      .update_column(schema_row);
}

const ColumnFamilyModel* KeyspaceModel::get_column_family(const std::string& cf_name) const {
  const ColumnFamilyModel* cfm = NULL;
  ColumnFamilyModelMap::const_iterator i = cfs_.find(cf_name);
  if (i != cfs_.end()) {
    cfm = &i->second;
  }
  return cfm;
}

void SchemaMetadata::update_keyspace(const Row* schema_row) {
 keyspaces_[KeyspaceMetadata::name_from_row(schema_row)]
     .set_meta(KeyspaceMetadata::from_schema_row(schema_row));
}

void SchemaMetadata::update_column_family(const Row* schema_row) {
 keyspaces_[ColumnFamilyMetadata::keyspace_name_from_row(schema_row)]
     .update_column_family(schema_row);
}

void SchemaMetadata::update_column(const Row* schema_row) {
 keyspaces_[ColumnMetadata::keyspace_name_from_row(schema_row)]
     .update_column(schema_row);
}

const KeyspaceModel* SchemaMetadata::get_keyspace(const std::string& ks_name) const {
  const KeyspaceModel* ksm = NULL;
  KeyspaceModelMap::const_iterator i = keyspaces_.find(ks_name);
  if (i != keyspaces_.end()) {
    ksm = &i->second;
  }
  return ksm;
}

} // namespace cass


