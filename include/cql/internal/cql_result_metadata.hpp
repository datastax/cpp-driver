/*
  Copyright (c) 2013 Matthew Stump

  This file is part of cassandra.

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

#ifndef CQL_RESULT_METADATA_H_
#define CQL_RESULT_METADATA_H_

#include <string>
#include <vector>
#include <boost/noncopyable.hpp>
#include <boost/tuple/tuple.hpp>
#include <boost/unordered_map.hpp>

#include "cql/cql.hpp"

namespace cql {

class cql_result_metadata_t :
        boost::noncopyable {

public:
    typedef boost::tuple<std::string, std::string, std::string> column_name_t;

    cql_result_metadata_t();

    std::string
    str() const;

    cql::cql_byte_t*
    read(cql::cql_byte_t* input);

    cql_int_t
    flags() const;

    void
    flags(cql_int_t v);

    cql_int_t
    column_count() const;

    void
    column_count(cql_int_t v);

    bool
    has_global_keyspace() const;

    bool
    has_global_table() const;

    const std::string&
    global_keyspace() const;

    void
    global_keyspace(const std::string& keyspace);

    const std::string&
    global_table() const;

    void
    global_table(const std::string& table);

    bool
    exists(const std::string& column) const;

    bool
    exists(const std::string& keyspace,
           const std::string& table,
           const std::string& column) const;

    bool
    column_name(int i,
                std::string& output_keyspace,
                std::string& output_table,
                std::string& output_column) const;

    bool
    column_class(int i,
                 std::string& output) const;

    bool
    column_class(const std::string& column,
                 std::string& output) const;

    bool
    column_class(const std::string& keyspace,
                 const std::string& table,
                 const std::string& column,
                 std::string& output) const;

    bool
    column_type(int i,
                cql::cql_column_type_enum& output) const;

    bool
    column_type(const std::string& column,
                cql::cql_column_type_enum& output) const;

    bool
    column_type(const std::string& keyspace,
                const std::string& table,
                const std::string& column,
                cql::cql_column_type_enum& output) const;

    bool
    get_index(const std::string& column,
              int& output) const;

    bool
    get_index(const std::string& keyspace,
              const std::string& table,
              const std::string& column,
              int& output) const;

    bool
    collection_primary_class(int i,
                             std::string& output) const;

    bool
    collection_primary_class(const std::string& column,
                             std::string& output) const;

    bool
    collection_primary_class(const std::string& keyspace,
                             const std::string& table,
                             const std::string& column,
                             std::string& output) const;

    bool
    collection_primary_type(int i,
                            cql::cql_column_type_enum& output) const;

    bool
    collection_primary_type(const std::string& column,
                            cql::cql_column_type_enum& output) const;

    bool
    collection_primary_type(const std::string& keyspace,
                            const std::string& table,
                            const std::string& column,
                            cql::cql_column_type_enum& output) const;

    bool
    collection_secondary_class(int i,
                               std::string& output) const;

    bool
    collection_secondary_class(const std::string& column,
                               std::string& output) const;

    bool
    collection_secondary_class(const std::string& keyspace,
                               const std::string& table,
                               const std::string& column,
                               std::string& output) const;

    bool
    collection_secondary_type(int i,
                              cql::cql_column_type_enum& output) const;

    bool
    collection_secondary_type(const std::string& column,
                              cql::cql_column_type_enum& output) const;

    bool
    collection_secondary_type(const std::string& keyspace,
                              const std::string& table,
                              const std::string& column,
                              cql::cql_column_type_enum& output) const;

private:

    struct option_t {
        column_name_t name;
        cql::cql_column_type_enum primary_type;
        cql::cql_column_type_enum collection_primary_type;
        cql::cql_column_type_enum collection_secondary_type;
        std::string primary_class;
        std::string collection_primary_class;
        std::string collection_secondary_class;

        option_t() :
            primary_type(CQL_COLUMN_TYPE_UNKNOWN),
            collection_primary_type(CQL_COLUMN_TYPE_UNKNOWN),
            collection_secondary_type(CQL_COLUMN_TYPE_UNKNOWN)
        {}

        option_t(cql::cql_column_type_enum primary_type) :
            primary_type(primary_type),
            collection_primary_type(CQL_COLUMN_TYPE_UNKNOWN),
            collection_secondary_type(CQL_COLUMN_TYPE_UNKNOWN)
        {}

        option_t(cql::cql_column_type_enum primary_type,
                 cql::cql_column_type_enum collection_primary_type) :
            primary_type(primary_type),
            collection_primary_type(collection_primary_type),
            collection_secondary_type(CQL_COLUMN_TYPE_UNKNOWN)
        {}

        option_t(cql::cql_column_type_enum primary_type,
                 cql::cql_column_type_enum collection_primary_type,
                 cql::cql_column_type_enum collection_secondary_type) :
            primary_type(primary_type),
            collection_primary_type(collection_primary_type),
            collection_secondary_type(collection_secondary_type)
        {}
    };

    struct column_name_hash
            : std::unary_function<column_name_t, std::size_t> {
        std::size_t
        operator()(const column_name_t& n) const {
            std::size_t seed = 0;
            boost::hash_combine(seed, n.get<0>());
            boost::hash_combine(seed, n.get<1>());
            boost::hash_combine(seed, n.get<2>());
            return seed;
        }
    };

    struct column_name_equality
            : std::binary_function<column_name_t, column_name_t, bool> {
        std::size_t
        operator()(const column_name_t& a,
                   const column_name_t& b) const {

            return a.get<0>() == b.get<0>()
                   && a.get<1>() == b.get<1>()
                   && a.get<2>() == b.get<2>();
        }
    };

    typedef boost::unordered_map<column_name_t,
            int,
            column_name_hash,
            column_name_equality> column_name_idx_t;

    cql_int_t                   _flags;
    cql_int_t                   _column_count;
    std::string                 _global_keyspace_name;
    std::string                 _global_table_name;
    column_name_idx_t           _column_name_idx;
    std::vector<option_t>       _columns;
};

} // namespace cql

#endif // CQL_RESULT_METADATA_H_
