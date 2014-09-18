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

#include <map>
#include <string>

namespace cass {

class BufferPiece;
class Row;

class KeyspaceMetadata {
public:
  KeyspaceMetadata(const boost::string_ref& name);

  static KeyspaceMetadata* from_schema_row(const Row* schema_row);

  const std::string& name() { return name_; }
  bool durable_writes() { return durable_writes_; }
  const std::string& strategy_class() { return strategy_class_; }
  const std::map<std::string,std::string>& strategy_options() { return strategy_options_; }

private:
  void options_from_json(const BufferPiece& options_json);

private:
  const std::string name_;
  bool durable_writes_;
  std::string strategy_class_;
  std::map<std::string,std::string> strategy_options_;
};

class ColumnFamilyMetadata {
public:
  ColumnFamilyMetadata(const boost::string_ref& keyspace_name,
                       const boost::string_ref& name);

  static ColumnFamilyMetadata* from_schema_row(const Row* schema_row);

  const std::string& keyspace_name() { return keyspace_name_; }
  const std::string& name() { return name_; }

private:
  void options_from_json(const BufferPiece& options_json);

private:
  const std::string keyspace_name_;
  const std::string name_;
};

class ColumnMetadata {
public:
  ColumnMetadata(const boost::string_ref& name);

  static ColumnMetadata* from_schema_row(const Row* schema_row);

  const std::string& keyspace_name() { return keyspace_name_; }
  const std::string& column_family_name() { return column_family_name_; }
  const std::string& name() { return name_; }

private:
  void options_from_json(const BufferPiece& options_json);

private:
  const std::string keyspace_name_;
  const std::string column_family_name_;
  const std::string name_;
};

class SchemaMetadata {
public:
private:
};

} // namespace cass

#endif
