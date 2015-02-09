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

#ifndef __CASS_SCHEMA_METADATA_HPP_INCLUDED__
#define __CASS_SCHEMA_METADATA_HPP_INCLUDED__

#include "copy_on_write_ptr.hpp"
#include "iterator.hpp"
#include "scoped_lock.hpp"
#include "scoped_ptr.hpp"
#include "type_parser.hpp"

#include <map>
#include <string>
#include <uv.h>
#include <vector>

namespace cass {

class Row;
class ResultResponse;

template<class T>
class SchemaMapIteratorImpl {
public:
  typedef std::map<std::string, T> Map;

  SchemaMapIteratorImpl(const Map& map)
    : next_(map.begin())
    , end_(map.end()) {}

  bool next() {
    if (next_ == end_) {
      return false;
    }
    current_ = next_++;
    return true;
  }

  const T* item() const {
    return &current_->second;
  }

private:
  typename Map::const_iterator next_;
  typename Map::const_iterator current_;
  typename Map::const_iterator end_;
};

class SchemaMetadataField {
public:
  typedef std::map<std::string, SchemaMetadataField> Map;

  SchemaMetadataField() {}

  SchemaMetadataField(const std::string& name)
    : name_(name) {}

  SchemaMetadataField(const std::string& name,
                      const Value& value,
                      const SharedRefPtr<RefBuffer>& buffer)
    : name_(name)
    , value_(value)
    , buffer_(buffer) {}

  const std::string& name() const {
    return name_;
  }

  const Value* value() const {
    return &value_;
  }

private:
  std::string name_;
  Value value_;
  SharedRefPtr<RefBuffer> buffer_;
};

class SchemaMetadataFieldIterator : public Iterator {
public:
  typedef SchemaMapIteratorImpl<SchemaMetadataField> Map;

  SchemaMetadataFieldIterator(const Map& map)
    : Iterator(CASS_ITERATOR_TYPE_SCHEMA_META_FIELD)
    , impl_(map) {}

  virtual bool next() { return impl_.next(); }
  const SchemaMetadataField* field() const { return impl_.item(); }

private:
  SchemaMapIteratorImpl<SchemaMetadataField> impl_;
};

class SchemaMetadata {
public:
  SchemaMetadata(CassSchemaMetaType type)
    : type_(type) {}

  virtual ~SchemaMetadata() {}

  CassSchemaMetaType type() const { return type_; }

  virtual const SchemaMetadata* get_entry(const std::string& name) const = 0;
  virtual Iterator* iterator() const = 0;

  const SchemaMetadataField* get_field(const std::string& name) const;
  std::string get_string_field(const std::string& name) const;
  Iterator* iterator_fields() const { return new SchemaMetadataFieldIterator(fields_); }

protected:
  void add_field(const SharedRefPtr<RefBuffer>& buffer, const Row* row, const std::string& name);
  void add_json_list_field(int version, const Row* row, const std::string& name);
  void add_json_map_field(int version, const Row* row, const std::string& name);

  SchemaMetadataField::Map fields_;

private:
  const CassSchemaMetaType type_;
};

class SchemaMetadataIterator : public Iterator {
public:
  SchemaMetadataIterator()
    : Iterator(CASS_ITERATOR_TYPE_SCHEMA_META) {}
  virtual const SchemaMetadata* meta() const = 0;
};

template<class T>
class SchemaMetadataIteratorImpl : public SchemaMetadataIterator {
public:
  typedef typename SchemaMapIteratorImpl<T>::Map Map;

  SchemaMetadataIteratorImpl(const Map& map)
    : impl_(map) {}

  virtual bool next() { return impl_.next(); }
  virtual const SchemaMetadata* meta() const { return impl_.item(); }

private:
  SchemaMapIteratorImpl<T> impl_;
};

class ColumnMetadata : public SchemaMetadata {
public:
  typedef std::map<std::string, ColumnMetadata> Map;

  ColumnMetadata()
    : SchemaMetadata(CASS_SCHEMA_META_TYPE_COLUMN) {}

  virtual const SchemaMetadata* get_entry(const std::string& name) const {
    return NULL;
  }
  virtual Iterator* iterator() const { return NULL; }

  void update(int version, const SharedRefPtr<RefBuffer>& buffer, const Row* row);
};

class TableMetadata : public SchemaMetadata {
public:
  typedef std::map<std::string, TableMetadata> Map;
  typedef SchemaMetadataIteratorImpl<ColumnMetadata> ColumnIterator;
  typedef std::vector<std::string> KeyAliases;

  TableMetadata()
    : SchemaMetadata(CASS_SCHEMA_META_TYPE_TABLE) {}

  virtual const SchemaMetadata* get_entry(const std::string& name) const;
  virtual Iterator* iterator() const { return new ColumnIterator(columns_); }

  ColumnMetadata* get_or_create(const std::string& name) { return &columns_[name]; }
  void update(int version, const SharedRefPtr<RefBuffer>& buffer, const Row* row);

  const KeyAliases& key_aliases() const;
  void clear_columns() { columns_.clear(); }

private:
  ColumnMetadata::Map columns_;
  mutable KeyAliases key_aliases_;
};

class KeyspaceMetadata : public SchemaMetadata {
public:
  typedef std::map<std::string, KeyspaceMetadata> Map;
  typedef SchemaMetadataIteratorImpl<TableMetadata> TableIterator;

  KeyspaceMetadata()
    : SchemaMetadata(CASS_SCHEMA_META_TYPE_KEYSPACE) {}

  virtual const SchemaMetadata* get_entry(const std::string& name) const;
  virtual Iterator* iterator() const { return new TableIterator(tables_); }

  TableMetadata* get_or_create(const std::string& name) { return &tables_[name]; }
  void update(int version, const SharedRefPtr<RefBuffer>& buffer, const Row* row);
  void drop_table(const std::string& table_name);

  std::string strategy_class() const { return get_string_field("strategy_class"); }
  const SchemaMetadataField* strategy_options() const { return get_field("strategy_options"); }

private:
  TableMetadata::Map tables_;
};

class Schema {
public:
  typedef SchemaMetadataIteratorImpl<KeyspaceMetadata> KeyspaceIterator;
  typedef std::map<std::string, KeyspaceMetadata*> KeyspacePointerMap;

  Schema()
    : keyspaces_(new KeyspaceMetadata::Map)
    , protocol_version_(0) {}

  void set_protocol_version(int version) {
    protocol_version_ = version;
  }

  const SchemaMetadata* get(const std::string& name) const;
  Iterator* iterator() const { return new KeyspaceIterator(*keyspaces_); }

  KeyspaceMetadata* get_or_create(const std::string& name) { return &(*keyspaces_)[name]; }
  KeyspacePointerMap update_keyspaces(ResultResponse* result);
  void update_tables(ResultResponse* table_result, ResultResponse* col_result);
  void drop_keyspace(const std::string& keyspace_name);
  void drop_table(const std::string& keyspace_name, const std::string& table_name);
  void clear();
  void get_table_key_columns(const std::string& ks_name,
                             const std::string& cf_name,
                             std::vector<std::string>* output) const;

private:
  void update_columns(ResultResponse* result);

private:
  // Really coarse grain copy-on-write. This could be made
  // more fine grain, but it might not be worth the work.
  CopyOnWritePtr<KeyspaceMetadata::Map> keyspaces_;

  // Only used internally on a single thread, there's
  // no need for copy-on-write.
  int protocol_version_;
};

} // namespace cass

#endif
