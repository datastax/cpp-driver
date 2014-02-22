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

#ifndef CQL_RESULT_H_
#define CQL_RESULT_H_

#include <vector>
#include "cql/cql.hpp"

namespace cql {

class cql_list_t;
class cql_map_t;
class cql_set_t;

class cql_result_t {

public:
    virtual
    ~cql_result_t() {};

    virtual cql::cql_result_type_enum
    result_type() const = 0;

    virtual cql::cql_opcode_enum
    opcode() const = 0;

    virtual std::string
    str() const = 0;

    virtual size_t
    column_count() const = 0;

    virtual size_t
    row_count() const = 0;

    virtual const std::vector<cql::cql_byte_t>&
    query_id() const = 0;

    virtual bool
    next() = 0;

    virtual bool
    exists(const std::string& column) const = 0;

    virtual bool
    column_name(int i,
                std::string& output_keyspace,
                std::string& output_table,
                std::string& output_column) const = 0;

    virtual bool
    column_class(int i,
                 std::string& output) const = 0;

    virtual bool
    column_class(const std::string& column,
                 std::string& output) const = 0;

    virtual bool
    column_type(int i,
                cql_column_type_enum& output) const = 0;

    virtual bool
    column_type(const std::string& column,
                cql_column_type_enum& output) const = 0;

    virtual bool
    get_index(const std::string& column,
              int& output) const = 0;

    virtual bool
    is_null(int i,
            bool& output) const = 0;

    virtual bool
    is_null(const std::string& column,
            bool& output) const = 0;

    virtual bool
    get_bool(int i,
             bool& output) const = 0;

    virtual bool
    get_bool(const std::string& column,
             bool& output) const = 0;

    virtual bool
    get_int(int i,
            cql_int_t& output) const = 0;

    virtual bool
    get_int(const std::string& column,
            cql_int_t& output) const = 0;

    virtual bool
    get_float(int i,
              float& output) const = 0;

    virtual bool
    get_float(const std::string& column,
              float& output) const = 0;

    virtual bool
    get_double(int i,
               double& output) const = 0;

    virtual bool
    get_double(const std::string& column,
               double& output) const = 0;

    virtual bool
    get_bigint(int i,
               cql::cql_bigint_t& output) const = 0;

    virtual bool
    get_bigint(const std::string& column,
               cql::cql_bigint_t& output) const = 0;

    virtual bool
    get_string(int i,
               std::string& output) const = 0;

    virtual bool
    get_string(const std::string& column,
               std::string& output) const = 0;

    virtual bool
    get_data(int i,
             cql::cql_byte_t** output,
             cql::cql_int_t& size) const = 0;

    virtual bool
    get_data(const std::string& column,
             cql::cql_byte_t** output,
             cql::cql_int_t& size) const = 0;

    virtual bool
    get_list(int i,
             cql::cql_list_t** output) const = 0;

    virtual bool
    get_list(const std::string& column,
             cql::cql_list_t** output) const = 0;

    virtual bool
    get_set(int i,
            cql::cql_set_t** output) const = 0;

    virtual bool
    get_set(const std::string& column,
            cql::cql_set_t** output) const = 0;

    virtual bool
    get_map(int i,
            cql::cql_map_t** output) const = 0;

    virtual bool
    get_map(const std::string& column,
            cql::cql_map_t** output) const = 0;


};

} // namespace cql

#endif // CQL_RESULT_H_
