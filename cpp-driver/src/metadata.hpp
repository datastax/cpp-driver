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

#ifndef DATASTAX_INTERNAL_SCHEMA_METADATA_HPP
#define DATASTAX_INTERNAL_SCHEMA_METADATA_HPP

#include "allocated.hpp"
#include "copy_on_write_ptr.hpp"
#include "data_type.hpp"
#include "external.hpp"
#include "host.hpp"
#include "iterator.hpp"
#include "macros.hpp"
#include "map.hpp"
#include "ref_counted.hpp"
#include "scoped_lock.hpp"
#include "scoped_ptr.hpp"
#include "string.hpp"
#include "value.hpp"
#include "vector.hpp"

#include <uv.h>

namespace datastax { namespace internal { namespace core {

class ColumnMetadata;
class TableMetadata;
class KeyspaceMetadata;
class Row;
class ResultResponse;

template <class T>
class MapIteratorImpl {
public:
  typedef T ItemType;
  typedef internal::Map<String, T> Collection;

  MapIteratorImpl(const Collection& map)
      : next_(map.begin())
      , end_(map.end()) {}

  bool next() {
    if (next_ == end_) {
      return false;
    }
    current_ = next_++;
    return true;
  }

  const T& item() const { return current_->second; }

private:
  typename Collection::const_iterator next_;
  typename Collection::const_iterator current_;
  typename Collection::const_iterator end_;
};

template <class T>
class VecIteratorImpl {
public:
  typedef T ItemType;
  typedef Vector<T> Collection;

  VecIteratorImpl(const Collection& vec)
      : next_(vec.begin())
      , end_(vec.end()) {}

  bool next() {
    if (next_ == end_) {
      return false;
    }
    current_ = next_++;
    return true;
  }

  const T& item() const { return (*current_); }

private:
  typename Collection::const_iterator next_;
  typename Collection::const_iterator current_;
  typename Collection::const_iterator end_;
};

class MetadataField {
public:
  typedef internal::Map<String, MetadataField> Map;

  MetadataField() {}

  MetadataField(const String& name)
      : name_(name) {}

  MetadataField(const String& name, const Value& value, const RefBuffer::Ptr& buffer)
      : name_(name)
      , value_(value)
      , buffer_(buffer) {}

  const String& name() const { return name_; }

  const Value* value() const { return &value_; }

private:
  String name_;
  Value value_;
  RefBuffer::Ptr buffer_;
};

class MetadataFieldIterator : public Iterator {
public:
  typedef MapIteratorImpl<MetadataField>::Collection Map;

  MetadataFieldIterator(const Map& map)
      : Iterator(CASS_ITERATOR_TYPE_META_FIELD)
      , impl_(map) {}

  virtual bool next() { return impl_.next(); }
  const MetadataField* field() const { return &impl_.item(); }

private:
  MapIteratorImpl<MetadataField> impl_;
};

class MetadataBase {
public:
  MetadataBase(const String& name)
      : name_(name) {}

  const String& name() const { return name_; }

  const Value* get_field(const String& name) const;
  String get_string_field(const String& name) const;
  Iterator* iterator_fields() const { return new MetadataFieldIterator(fields_); }

  void swap_fields(MetadataBase& meta) { fields_.swap(meta.fields_); }

protected:
  const Value* add_field(const RefBuffer::Ptr& buffer, const Row* row, const String& name);
  void add_field(const RefBuffer::Ptr& buffer, const Value& value, const String& name);
  void add_json_list_field(const Row* row, const String& name);
  const Value* add_json_map_field(const Row* row, const String& name);

  MetadataField::Map fields_;

private:
  const String name_;
};

template <class IteratorImpl>
class MetadataIteratorImpl : public Iterator {
public:
  typedef typename IteratorImpl::Collection Collection;

  MetadataIteratorImpl(CassIteratorType type, const Collection& colleciton)
      : Iterator(type)
      , impl_(colleciton) {}

  virtual bool next() { return impl_.next(); }

protected:
  IteratorImpl impl_;
};

class FunctionMetadata
    : public MetadataBase
    , public RefCounted<FunctionMetadata> {
public:
  typedef SharedRefPtr<FunctionMetadata> Ptr;
  typedef internal::Map<String, Ptr> Map;
  typedef Vector<Ptr> Vec;

  struct Argument {
    typedef Vector<Argument> Vec;

    Argument(const StringRef& name, const DataType::ConstPtr& type)
        : name(name)
        , type(type) {}
    StringRef name;
    DataType::ConstPtr type;
  };

  FunctionMetadata(const VersionNumber& server_version, SimpleDataTypeCache& cache,
                   const String& name, const Value* signature, KeyspaceMetadata* keyspace,
                   const RefBuffer::Ptr& buffer, const Row* row);

  const String& simple_name() const { return simple_name_; }
  const Argument::Vec& args() const { return args_; }
  const DataType::ConstPtr& return_type() const { return return_type_; }
  StringRef body() const { return body_; }
  StringRef language() const { return language_; }
  bool called_on_null_input() const { return called_on_null_input_; }

  const DataType* get_arg_type(StringRef name) const;

private:
  String simple_name_;
  Argument::Vec args_;
  DataType::ConstPtr return_type_;
  StringRef body_;
  StringRef language_;
  bool called_on_null_input_;
};

inline bool operator==(const FunctionMetadata::Argument& a, StringRef b) { return a.name == b; }

class AggregateMetadata
    : public MetadataBase
    , public RefCounted<AggregateMetadata> {
public:
  typedef SharedRefPtr<AggregateMetadata> Ptr;
  typedef internal::Map<String, Ptr> Map;
  typedef Vector<Ptr> Vec;

  AggregateMetadata(const VersionNumber& server_version, SimpleDataTypeCache& cache,
                    const String& name, const Value* signature, KeyspaceMetadata* keyspace,
                    const RefBuffer::Ptr& buffer, const Row* row);

  const String& simple_name() const { return simple_name_; }
  const DataType::Vec arg_types() const { return arg_types_; }
  const DataType::ConstPtr& return_type() const { return return_type_; }
  const DataType::ConstPtr& state_type() const { return state_type_; }
  const FunctionMetadata::Ptr& state_func() const { return state_func_; }
  const FunctionMetadata::Ptr& final_func() const { return final_func_; }
  const Value& init_cond() const { return init_cond_; }

private:
  String simple_name_;
  DataType::Vec arg_types_;
  DataType::ConstPtr return_type_;
  DataType::ConstPtr state_type_;
  FunctionMetadata::Ptr state_func_;
  FunctionMetadata::Ptr final_func_;
  Value init_cond_;
};

class IndexMetadata
    : public MetadataBase
    , public RefCounted<IndexMetadata> {
public:
  typedef SharedRefPtr<IndexMetadata> Ptr;
  typedef internal::Map<String, Ptr> Map;
  typedef Vector<Ptr> Vec;

  CassIndexType type() const { return type_; }
  const String& target() const { return target_; }
  const Value* options() const { return &options_; }

  IndexMetadata(const String& index_name)
      : MetadataBase(index_name)
      , type_(CASS_INDEX_TYPE_UNKNOWN) {}

  static IndexMetadata::Ptr from_row(const String& index_name, const RefBuffer::Ptr& buffer,
                                     const Row* row);
  void update(StringRef index_type, const Value* options);

  static IndexMetadata::Ptr from_legacy(const String& index_name, const ColumnMetadata* column,
                                        const RefBuffer::Ptr& buffer, const Row* row);
  void update_legacy(StringRef index_type, const ColumnMetadata* column, const Value* options);

private:
  static CassIndexType index_type_from_string(StringRef index_type);
  static String target_from_legacy(const ColumnMetadata* column, const Value* options);

private:
  CassIndexType type_;
  String target_;
  Value options_;

private:
  DISALLOW_COPY_AND_ASSIGN(IndexMetadata);
};

class ColumnMetadata
    : public MetadataBase
    , public RefCounted<ColumnMetadata> {
public:
  typedef SharedRefPtr<ColumnMetadata> Ptr;
  typedef internal::Map<String, Ptr> Map;
  typedef Vector<Ptr> Vec;

  ColumnMetadata(const String& name)
      : MetadataBase(name)
      , type_(CASS_COLUMN_TYPE_REGULAR)
      , position_(0)
      , is_reversed_(false) {}

  ColumnMetadata(const String& name, int32_t position, CassColumnType type,
                 const DataType::ConstPtr& data_type)
      : MetadataBase(name)
      , type_(type)
      , position_(position)
      , data_type_(data_type)
      , is_reversed_(false) {}

  ColumnMetadata(const VersionNumber& server_version, SimpleDataTypeCache& cache,
                 const String& name, KeyspaceMetadata* keyspace, const RefBuffer::Ptr& buffer,
                 const Row* row);

  CassColumnType type() const { return type_; }
  int32_t position() const { return position_; }
  const DataType::ConstPtr& data_type() const { return data_type_; }
  bool is_reversed() const { return is_reversed_; }

private:
  CassColumnType type_;
  int32_t position_;
  DataType::ConstPtr data_type_;
  bool is_reversed_;

private:
  DISALLOW_COPY_AND_ASSIGN(ColumnMetadata);
};

inline bool operator==(const ColumnMetadata::Ptr& a, const String& b) { return a->name() == b; }

class TableMetadataBase
    : public MetadataBase
    , public RefCounted<TableMetadataBase> {
public:
  typedef SharedRefPtr<TableMetadataBase> Ptr;
  typedef Vector<CassClusteringOrder> ClusteringOrderVec;

  class ColumnIterator : public MetadataIteratorImpl<VecIteratorImpl<ColumnMetadata::Ptr> > {
  public:
    ColumnIterator(const ColumnIterator::Collection& collection)
        : MetadataIteratorImpl<VecIteratorImpl<ColumnMetadata::Ptr> >(
              CASS_ITERATOR_TYPE_COLUMN_META, collection) {}
    const ColumnMetadata* column() const { return impl_.item().get(); }
  };

  TableMetadataBase(const VersionNumber& server_version, const String& name,
                    const RefBuffer::Ptr& buffer, const Row* row, bool is_virtual);

  TableMetadataBase(const TableMetadataBase& other)
      : MetadataBase(other)
      , RefCounted<TableMetadataBase>()
      , is_virtual_(other.is_virtual_)
      , columns_(other.columns_)
      , columns_by_name_(other.columns_by_name_)
      , partition_key_(other.partition_key_)
      , clustering_key_(other.clustering_key_)
      , clustering_key_order_(other.clustering_key_order_) {}

  virtual ~TableMetadataBase() {}

  bool is_virtual() const { return is_virtual_; }

  const ColumnMetadata::Vec& columns() const { return columns_; }
  const ColumnMetadata::Vec& partition_key() const { return partition_key_; }
  const ColumnMetadata::Vec& clustering_key() const { return clustering_key_; }
  const ClusteringOrderVec& clustering_key_order() const { return clustering_key_order_; }

  Iterator* iterator_columns() const { return new ColumnIterator(columns_); }
  const ColumnMetadata* get_column(const String& name) const;
  virtual void add_column(const VersionNumber& server_version, const ColumnMetadata::Ptr& column);
  void clear_columns();
  void build_keys_and_sort(const VersionNumber& server_version, SimpleDataTypeCache& cache);

protected:
  const bool is_virtual_;

  ColumnMetadata::Vec columns_;
  ColumnMetadata::Map columns_by_name_;
  ColumnMetadata::Vec partition_key_;
  ColumnMetadata::Vec clustering_key_;
  ClusteringOrderVec clustering_key_order_;
};

class ViewMetadata : public TableMetadataBase {
public:
  typedef SharedRefPtr<ViewMetadata> Ptr;
  typedef internal::Map<String, Ptr> Map;
  typedef Vector<Ptr> Vec;

  static const ViewMetadata::Ptr NIL;

  ViewMetadata(const VersionNumber& server_version, const TableMetadata* table, const String& name,
               const RefBuffer::Ptr& buffer, const Row* row, bool is_virtual);

  ViewMetadata(const ViewMetadata& other, const TableMetadata* table)
      : TableMetadataBase(other)
      , base_table_(table) {}

  const TableMetadata* base_table() const { return base_table_; }

private:
  // This cannot be a reference counted pointer because it would cause a cycle.
  // This is okay because the lifetime of the table will exceed the lifetime
  // of a table's view. That is, a table's views will be removed when a table is
  // removed.
  const TableMetadata* base_table_;
};

class ViewIteratorBase : public Iterator {
public:
  ViewIteratorBase(CassIteratorType type)
      : Iterator(type) {}

  virtual ViewMetadata* view() const = 0;
};

class ViewIteratorVec : public ViewIteratorBase {
public:
  ViewIteratorVec(const ViewMetadata::Vec& views)
      : ViewIteratorBase(CASS_ITERATOR_TYPE_MATERIALIZED_VIEW_META)
      , impl_(views) {}

  virtual ViewMetadata* view() const { return impl_.item().get(); }
  virtual bool next() { return impl_.next(); }

private:
  VecIteratorImpl<ViewMetadata::Ptr> impl_;
};

class ViewIteratorMap : public ViewIteratorBase {
public:
  ViewIteratorMap(const ViewMetadata::Map& views)
      : ViewIteratorBase(CASS_ITERATOR_TYPE_MATERIALIZED_VIEW_META)
      , impl_(views) {}

  virtual ViewMetadata* view() const { return impl_.item().get(); }
  virtual bool next() { return impl_.next(); }

private:
  MapIteratorImpl<ViewMetadata::Ptr> impl_;
};

inline bool operator<(const ViewMetadata::Ptr& a, const ViewMetadata::Ptr& b) {
  return a->name() < b->name();
}

inline bool operator<(const ViewMetadata::Ptr& a, const String& b) { return a->name() < b; }

inline bool operator==(const ViewMetadata::Ptr& a, const ViewMetadata::Ptr& b) {
  return a->name() == b->name();
}

class TableMetadata : public TableMetadataBase {
public:
  typedef SharedRefPtr<TableMetadata> Ptr;
  typedef internal::Map<String, Ptr> Map;
  typedef Vector<Ptr> Vec;
  typedef Vector<String> KeyAliases;

  static const TableMetadata::Ptr NIL;

  class IndexIterator : public MetadataIteratorImpl<VecIteratorImpl<IndexMetadata::Ptr> > {
  public:
    IndexIterator(const IndexIterator::Collection& collection)
        : MetadataIteratorImpl<VecIteratorImpl<IndexMetadata::Ptr> >(CASS_ITERATOR_TYPE_INDEX_META,
                                                                     collection) {}
    const IndexMetadata* index() const { return impl_.item().get(); }
  };

  TableMetadata(const VersionNumber& server_version, const String& name,
                const RefBuffer::Ptr& buffer, const Row* row, bool is_virtual);

  TableMetadata(const TableMetadata& other)
      : TableMetadataBase(other)
      , indexes_(other.indexes_)
      , indexes_by_name_(other.indexes_by_name_) {}

  const ViewMetadata::Vec& views() const { return views_; }
  const IndexMetadata::Vec& indexes() const { return indexes_; }

  Iterator* iterator_views() const { return new ViewIteratorVec(views_); }
  const ViewMetadata* get_view(const String& name) const;
  virtual void add_column(const VersionNumber& server_version, const ColumnMetadata::Ptr& column);
  void add_view(const ViewMetadata::Ptr& view);
  void sort_views();

  Iterator* iterator_indexes() const { return new IndexIterator(indexes_); }
  const IndexMetadata* get_index(const String& name) const;
  void add_index(const IndexMetadata::Ptr& index);
  void clear_indexes();

  void key_aliases(SimpleDataTypeCache& cache, KeyAliases* output) const;

private:
  ViewMetadata::Vec views_;
  IndexMetadata::Vec indexes_;
  IndexMetadata::Map indexes_by_name_;
};

class KeyspaceMetadata : public MetadataBase {
public:
  typedef internal::Map<String, KeyspaceMetadata> Map;
  typedef CopyOnWritePtr<KeyspaceMetadata::Map> MapPtr;

  class TableIterator : public MetadataIteratorImpl<MapIteratorImpl<TableMetadata::Ptr> > {
  public:
    TableIterator(const TableIterator::Collection& collection)
        : MetadataIteratorImpl<MapIteratorImpl<TableMetadata::Ptr> >(CASS_ITERATOR_TYPE_TABLE_META,
                                                                     collection) {}
    const TableMetadata* table() const { return static_cast<TableMetadata*>(impl_.item().get()); }
  };

  class TypeIterator : public MetadataIteratorImpl<MapIteratorImpl<UserType::Ptr> > {
  public:
    TypeIterator(const TypeIterator::Collection& collection)
        : MetadataIteratorImpl<MapIteratorImpl<UserType::Ptr> >(CASS_ITERATOR_TYPE_TYPE_META,
                                                                collection) {}
    const UserType* type() const { return impl_.item().get(); }
  };

  class FunctionIterator : public MetadataIteratorImpl<MapIteratorImpl<FunctionMetadata::Ptr> > {
  public:
    FunctionIterator(const FunctionIterator::Collection& collection)
        : MetadataIteratorImpl<MapIteratorImpl<FunctionMetadata::Ptr> >(
              CASS_ITERATOR_TYPE_FUNCTION_META, collection) {}
    const FunctionMetadata* function() const { return impl_.item().get(); }
  };

  class AggregateIterator : public MetadataIteratorImpl<MapIteratorImpl<AggregateMetadata::Ptr> > {
  public:
    AggregateIterator(const AggregateIterator::Collection& collection)
        : MetadataIteratorImpl<MapIteratorImpl<AggregateMetadata::Ptr> >(
              CASS_ITERATOR_TYPE_AGGREGATE_META, collection) {}
    const AggregateMetadata* aggregate() const { return impl_.item().get(); }
  };

  KeyspaceMetadata(const String& name, bool is_virtual = false)
      : MetadataBase(name)
      , is_virtual_(is_virtual)
      , tables_(new TableMetadata::Map())
      , views_(new ViewMetadata::Map())
      , user_types_(new UserType::Map())
      , functions_(new FunctionMetadata::Map())
      , aggregates_(new AggregateMetadata::Map()) {}

  void update(const VersionNumber& server_version, const RefBuffer::Ptr& buffer, const Row* row);

  bool is_virtual() const { return is_virtual_; }

  const FunctionMetadata::Map& functions() const { return *functions_; }
  const UserType::Map& user_types() const { return *user_types_; }

  Iterator* iterator_tables() const { return new TableIterator(*tables_); }
  const TableMetadata* get_table(const String& name) const;
  const TableMetadata::Ptr& get_table(const String& name);
  void add_table(const TableMetadata::Ptr& table);

  Iterator* iterator_views() const { return new ViewIteratorMap(*views_); }
  const ViewMetadata* get_view(const String& name) const;
  const ViewMetadata::Ptr& get_view(const String& name);
  void add_view(const ViewMetadata::Ptr& view);

  void drop_table_or_view(const String& table_name);

  Iterator* iterator_user_types() const { return new TypeIterator(*user_types_); }
  const UserType* get_user_type(const String& type_name) const;
  const UserType::Ptr& get_or_create_user_type(const String& name, bool is_frozen);
  void drop_user_type(const String& type_name);

  Iterator* iterator_functions() const { return new FunctionIterator(*functions_); }
  const FunctionMetadata* get_function(const String& full_function_name) const;
  void add_function(const FunctionMetadata::Ptr& function);
  void drop_function(const String& full_function_name);

  Iterator* iterator_aggregates() const { return new AggregateIterator(*aggregates_); }
  const AggregateMetadata* get_aggregate(const String& full_aggregate_name) const;
  void add_aggregate(const AggregateMetadata::Ptr& aggregate);
  void drop_aggregate(const String& full_aggregate_name);

  StringRef strategy_class() const { return strategy_class_; }
  const Value* strategy_options() const { return &strategy_options_; }

private:
  void internal_add_table(const TableMetadata::Ptr& table, const ViewMetadata::Vec& views);

private:
  const bool is_virtual_;
  StringRef strategy_class_;
  Value strategy_options_;

  CopyOnWritePtr<TableMetadata::Map> tables_;
  CopyOnWritePtr<ViewMetadata::Map> views_;
  CopyOnWritePtr<UserType::Map> user_types_;
  CopyOnWritePtr<FunctionMetadata::Map> functions_;
  CopyOnWritePtr<AggregateMetadata::Map> aggregates_;
};

class Metadata {
public:
  class KeyspaceIterator : public MetadataIteratorImpl<MapIteratorImpl<KeyspaceMetadata> > {
  public:
    KeyspaceIterator(const KeyspaceIterator::Collection& collection)
        : MetadataIteratorImpl<MapIteratorImpl<KeyspaceMetadata> >(CASS_ITERATOR_TYPE_KEYSPACE_META,
                                                                   collection) {}
    const KeyspaceMetadata* keyspace() const { return &impl_.item(); }
  };

  class SchemaSnapshot : public Allocated {
  public:
    SchemaSnapshot(uint32_t version, const VersionNumber& server_version,
                   const KeyspaceMetadata::MapPtr& keyspaces)
        : version_(version)
        , server_version_(server_version)
        , keyspaces_(keyspaces) {}

    uint32_t version() const { return version_; }
    VersionNumber server_version() const { return server_version_; }

    const KeyspaceMetadata* get_keyspace(const String& name) const;
    Iterator* iterator_keyspaces() const { return new KeyspaceIterator(*keyspaces_); }

    const UserType* get_user_type(const String& keyspace_name, const String& type_name) const;

  private:
    uint32_t version_;
    VersionNumber server_version_;
    KeyspaceMetadata::MapPtr keyspaces_;
  };

  static String full_function_name(const String& name, const StringVec& signature);

public:
  Metadata()
      : updating_(&front_)
      , schema_snapshot_version_(0) {
    uv_mutex_init(&mutex_);
  }

  ~Metadata() { uv_mutex_destroy(&mutex_); }

  SchemaSnapshot schema_snapshot() const;

  void update_keyspaces(const ResultResponse* result, bool is_virtual);
  void update_tables(const ResultResponse* result);
  void update_views(const ResultResponse* result);
  void update_columns(const ResultResponse* result);
  void update_indexes(const ResultResponse* result);
  void update_user_types(const ResultResponse* result);
  void update_functions(const ResultResponse* result);
  void update_aggregates(const ResultResponse* result);

  void drop_keyspace(const String& keyspace_name);
  void drop_table_or_view(const String& keyspace_name, const String& table_or_view_name);
  void drop_user_type(const String& keyspace_name, const String& type_name);
  void drop_function(const String& keyspace_name, const String& full_function_name);
  void drop_aggregate(const String& keyspace_name, const String& full_aggregate_name);

  // This clears and allows updates to the back buffer while preserving
  // the front buffer for snapshots.
  void clear_and_update_back(const VersionNumber& server_version);

  // This swaps the back buffer to the front and makes incremental updates
  // happen directly to the front buffer.
  void swap_to_back_and_update_front();

  void clear();

private:
  bool is_front_buffer() const { return updating_ == &front_; }

private:
  class InternalData {
  public:
    InternalData()
        : keyspaces_(new KeyspaceMetadata::Map()) {}

    const KeyspaceMetadata::MapPtr& keyspaces() const { return keyspaces_; }

    void update_keyspaces(const VersionNumber& server_version, const ResultResponse* result,
                          bool is_virtual);
    void update_tables(const VersionNumber& server_version, const ResultResponse* result);
    void update_views(const VersionNumber& server_version, const ResultResponse* result);
    void update_columns(const VersionNumber& server_version, SimpleDataTypeCache& cache,
                        const ResultResponse* result);
    void update_legacy_indexes(const VersionNumber& server_version, const ResultResponse* result);
    void update_indexes(const VersionNumber& server_version, const ResultResponse* result);
    void update_user_types(const VersionNumber& server_version, SimpleDataTypeCache& cache,
                           const ResultResponse* result);
    void update_functions(const VersionNumber& server_version, SimpleDataTypeCache& cache,
                          const ResultResponse* result);
    void update_aggregates(const VersionNumber& server_version, SimpleDataTypeCache& cache,
                           const ResultResponse* result);

    void drop_keyspace(const String& keyspace_name);
    void drop_table_or_view(const String& keyspace_name, const String& table_or_view_name);
    void drop_user_type(const String& keyspace_name, const String& type_name);
    void drop_function(const String& keyspace_name, const String& full_function_name);
    void drop_aggregate(const String& keyspace_name, const String& full_aggregate_name);

    void clear() { keyspaces_->clear(); }

    void swap(InternalData& other) {
      CopyOnWritePtr<KeyspaceMetadata::Map> temp = other.keyspaces_;
      other.keyspaces_ = keyspaces_;
      keyspaces_ = temp;
    }

  private:
    KeyspaceMetadata* get_or_create_keyspace(const String& name, bool is_virtual = false);

  private:
    CopyOnWritePtr<KeyspaceMetadata::Map> keyspaces_;

  private:
    DISALLOW_COPY_AND_ASSIGN(InternalData);
  };

  InternalData* updating_;
  InternalData front_;
  InternalData back_;

  VersionNumber server_version_;
  uint32_t schema_snapshot_version_;

  // This lock prevents partial snapshots when updating metadata
  mutable uv_mutex_t mutex_;

  // Only used internally on a single thread, there's
  // no need for copy-on-write.
  SimpleDataTypeCache cache_;

private:
  DISALLOW_COPY_AND_ASSIGN(Metadata);
};

}}} // namespace datastax::internal::core

EXTERNAL_TYPE(datastax::internal::core::Metadata::SchemaSnapshot, CassSchemaMeta)
EXTERNAL_TYPE(datastax::internal::core::KeyspaceMetadata, CassKeyspaceMeta)
EXTERNAL_TYPE(datastax::internal::core::TableMetadata, CassTableMeta)
EXTERNAL_TYPE(datastax::internal::core::ViewMetadata, CassMaterializedViewMeta)
EXTERNAL_TYPE(datastax::internal::core::ColumnMetadata, CassColumnMeta)
EXTERNAL_TYPE(datastax::internal::core::IndexMetadata, CassIndexMeta)
EXTERNAL_TYPE(datastax::internal::core::FunctionMetadata, CassFunctionMeta)
EXTERNAL_TYPE(datastax::internal::core::AggregateMetadata, CassAggregateMeta)

#endif
