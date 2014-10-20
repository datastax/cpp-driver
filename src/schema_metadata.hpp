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

#ifndef __CASS_SCHEMA_METADATA_HPP_INCLUDED__
#define __CASS_SCHEMA_METADATA_HPP_INCLUDED__

#include "cass_type_parser.hpp"
#include "iterator.hpp"
#include "scoped_ptr.hpp"

#include <map>
#include <string>

namespace cass {

class BufferPiece;
class Row;

template<class C, cass::IteratorType I>
class ModelMapIterator : public Iterator {
public:
  ModelMapIterator(const C& model_map)
      : Iterator(I)
      , first_next_(true)
      , itr_(model_map.begin())
      , end_(model_map.end()) {}

  virtual bool next() {
    if (itr_ == end_) {
      return false;
    }
    if (!first_next_) {
      ++itr_;
    }
    first_next_ = false;
    return itr_ != end_;
  }

  const typename C::mapped_type::meta_type& meta() {
    assert(itr_ != end_);
    return itr_->second.meta();
  }

  const typename C::mapped_type& model() {
    assert(itr_ != end_);
    return itr_->second;
  }

private:
  bool first_next_;
  typename C::const_iterator itr_;
  const typename C::const_iterator end_;
};

class ColumnMetadata {
public:
  ColumnMetadata();

  static ColumnMetadata from_schema_row(const Row* schema_row);
  static std::string keyspace_name_from_row(const Row* schema_row);
  static std::string column_family_name_from_row(const Row* schema_row);
  static std::string name_from_row(const Row* schema_row);

  class ColumnTypeMapper {
  public:
    ColumnTypeMapper();
    CassColumnType operator[](const std::string& col_type) const;
  private:
    typedef std::map<std::string, CassColumnType> NameTypeMap;
    NameTypeMap name_type_map_;
  };

  std::string name_;
  int component_index_;
  std::string index_name_;
  std::string index_options_;
  std::string index_type_;
  CassColumnType kind_;
  TypeDescriptor type_;
  std::string validator_;

  const static ColumnTypeMapper type_map_;

private:
  void options_from_json(const BufferPiece& options_json);
};

class ColumnModel {
public:
  typedef ColumnMetadata meta_type;

  void set_meta(const meta_type& cfm) { cf_meta_ = cfm; }
  const meta_type& meta() const { return cf_meta_; }

private:
  meta_type cf_meta_;
};

typedef std::map<std::string, ColumnModel> ColumnModelMap;
typedef ModelMapIterator<ColumnModelMap, CASS_ITERATOR_TYPE_COL_META> ColumnIterator;

class ColumnFamilyMetadata {
public:
  ColumnFamilyMetadata();

  static ColumnFamilyMetadata from_schema_row(const Row* schema_row);
  static std::string keyspace_name_from_row(const Row* schema_row);
  static std::string name_from_row(const Row* schema_row);

  std::string name_;
  double bloom_filter_fp_chance_;
  std::string caching_;
  CassUuid cf_id_;
  std::string column_aliases_;
  std::string comment_;
  std::string compaction_strategy_class_;
  std::string compaction_strategy_options_;
  std::string comparator_;
  std::string compression_parameters_;
  int default_time_to_live_;
  std::string default_validator_;
  std::map<std::string, cass_int64_t> dropped_columns_;
  int gc_grace_seconds_;
  int index_interval_;
  cass_bool_t is_dense_;
  std::string key_aliases_;
  std::string key_validator_;
  double local_read_repair_chance_;
  int max_compaction_threshold_;
  int max_index_interval_;
  int memtable_flush_period_in_ms_;
  int min_compaction_threshold_;
  int min_index_interval_;
  double read_repair_chance_;
  std::string speculative_retry_;
  std::string subcomparator_;
  std::string type_;
  std::string value_alias_;
};

class ColumnFamilyModel {
public:
  typedef ColumnFamilyMetadata meta_type;

  void set_meta(const meta_type& cfm) { cf_meta_ = cfm; }
  const meta_type& meta() const { return cf_meta_; }

  void update_column_family(const Row* schema_row);
  void update_column(const Row* schema_row);
  void update_columns(const std::vector<Row> col_rows);
  const ColumnModel* get_column(const std::string& col_name) const;
  const ColumnModelMap& columns() const { return columns_; }

private:
  meta_type cf_meta_;
  ColumnModelMap columns_;
};

typedef std::map<std::string, ColumnFamilyModel> ColumnFamilyModelMap;
typedef ModelMapIterator<ColumnFamilyModelMap, CASS_ITERATOR_TYPE_CF_META> ColumnFamilyIterator;

class KeyspaceMetadata {
public:
  KeyspaceMetadata();

  static KeyspaceMetadata from_schema_row(const Row* schema_row);
  static std::string name_from_row(const Row* schema_row);

  std::string name_;
  cass_bool_t durable_writes_;
  std::string strategy_class_;
  std::map<std::string,std::string> strategy_options_;
  std::string strategy_options_json_;
};

class KeyspaceModel {
public:
  typedef KeyspaceMetadata meta_type;

  void set_meta(const meta_type& ksm) { ks_meta_ = ksm; }
  const meta_type& meta() const { return ks_meta_; }

  void update_column_family(const Row* schema_row);
  void update_column_family(const Row* schema_row, const std::vector<Row> col_rows);
  void update_column(const Row* schema_row);
  void drop_column_family(const std::string& cf_name) { cfs_.erase(cf_name); }
  const ColumnFamilyModel* get_column_family(const std::string& cf_name) const;
  const ColumnFamilyModelMap& column_families() const { return cfs_; }

private:
  meta_type ks_meta_;
  ColumnFamilyModelMap cfs_;
};

typedef std::map<std::string, KeyspaceModel> KeyspaceModelMap;
typedef ModelMapIterator<KeyspaceModelMap, CASS_ITERATOR_TYPE_KS_META> KeyspaceIterator;

class SchemaModel {
public:
  void update_keyspace(const Row* schema_row);
  void update_column_family(const Row* schema_row);
  void update_column_family(const Row* schema_row, const std::vector<Row> col_rows);
  void update_column(const Row* schema_row);
  void drop_keyspace(const std::string& ks_name) { keyspaces_.erase(ks_name); }
  void drop_column_family(const std::string& ks_name, const std::string& cf_name);
  const KeyspaceModel* get_keyspace(const std::string& ks_name) const;
  const KeyspaceModelMap& keyspaces() const { return keyspaces_; }

private:
  KeyspaceModelMap keyspaces_;
};

} // namespace cass

#endif
