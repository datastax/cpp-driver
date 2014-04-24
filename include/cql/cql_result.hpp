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

#include <boost/asio/ip/address.hpp>

namespace cql {

class cql_list_t;
class cql_map_t;
class cql_set_t;
class cql_host_t;
class cql_uuid_t;
class cql_decimal_t;
class cql_varint_t;

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
    get_nullity(int i,
                bool& output) const = 0;

    virtual bool
    get_nullity(const std::string& column,
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
			
    CQL_DEPRECATED virtual bool
    get_data(int i,
             cql::cql_byte_t** output,
             cql::cql_int_t& size) const = 0;
			
    CQL_DEPRECATED virtual bool
    get_data(const std::string& column,
             cql::cql_byte_t** output,
             cql::cql_int_t& size) const = 0;

	virtual bool
    get_data(int i,
			 std::vector< cql::cql_byte_t > & output ) const = 0;
			
    virtual bool
    get_data(const std::string& column,
             std::vector< cql::cql_byte_t > & output ) const = 0;
									
    CQL_DEPRECATED virtual bool
    get_list(int i,
             cql::cql_list_t** output) const = 0;
			
    CQL_DEPRECATED virtual bool
    get_list(const std::string& column,
             cql::cql_list_t** output) const = 0;

    virtual bool
    get_set(int i,
            cql::cql_set_t** output) const = 0;

    CQL_DEPRECATED virtual bool
    get_set(const std::string& column,
            cql::cql_set_t** output) const = 0;
			
    CQL_DEPRECATED virtual bool
    get_map(int i,
            cql::cql_map_t** output) const = 0;

    CQL_DEPRECATED virtual bool
    get_map(const std::string& column,
            cql::cql_map_t** output) const = 0;
			
	virtual bool
    get_list(int i,
             boost::shared_ptr< cql::cql_list_t > & output) const = 0;			

	virtual bool
    get_list(const std::string& column,	
             boost::shared_ptr< cql::cql_list_t > & output) const = 0;			

	virtual bool
    get_set(int i,
            boost::shared_ptr< cql::cql_set_t > & output) const = 0;		

	virtual bool
    get_set(const std::string& column,
            boost::shared_ptr< cql::cql_set_t > & output) const = 0;		

	virtual bool
    get_map(int i,	
            boost::shared_ptr< cql::cql_map_t > & output) const = 0;		

	virtual bool
    get_map(const std::string& column,
            boost::shared_ptr< cql::cql_map_t > & output) const = 0;		

	virtual bool
    get_counter(int i,
                cql::cql_bigint_t& output) const = 0;

    virtual bool
    get_counter(const std::string& column,
                cql::cql_bigint_t& output) const = 0;

	virtual bool
    get_ascii(int i,
               std::string& output) const = 0;

    virtual bool
    get_ascii(const std::string& column,
              std::string& output) const = 0;

	virtual bool
    get_varchar(int i,
                std::string& output) const = 0;

    virtual bool
    get_varchar(const std::string& column,
                std::string& output) const = 0;

	virtual bool
    get_uuid(int i,
			 cql_uuid_t& output) const = 0;

    virtual bool
    get_uuid(const std::string& column,
             cql_uuid_t& output) const = 0;

	virtual bool
    get_uuid(int i,
			 std::string & output) const = 0;

    virtual bool
    get_uuid(const std::string& column,
             std::string & output) const = 0;

	virtual bool
	get_timestamp(int i,
        cql::cql_bigint_t& output) const = 0;
		
	virtual bool
	get_timestamp(const std::string& column,
        cql::cql_bigint_t& output) const = 0;	

	virtual bool
	get_timeuuid(int i,
        cql::cql_bigint_t& output) const = 0;
		
	virtual bool
	get_timeuuid(const std::string& column,
        cql::cql_bigint_t& output) const = 0;	

	virtual bool
	get_blob(int i, 
	         std::vector<cql::cql_byte_t>& output) const = 0;
					
	virtual bool
	get_blob(std::string const & column, 
	         std::vector<cql::cql_byte_t>& output) const = 0;

	virtual bool
	get_blob(int i,
		     std::pair<cql::cql_byte_t*,cql::cql_int_t>& output ) const = 0;

	virtual bool
	get_blob(std::string const & column, 
	          std::pair<cql::cql_byte_t*,cql::cql_int_t>& output ) const = 0;

	virtual bool
    get_text(int i,
             std::string& output) const = 0;

    virtual bool
    get_text(const std::string& column,
             std::string& output) const = 0;

	virtual bool
	get_inet(int i,		
	         boost::asio::ip::address & output) const = 0;
						
	virtual bool
	get_inet(std::string const& column, 
	         boost::asio::ip::address & output) const = 0;
	
	virtual bool
	get_decimal(std::string const & column,
				cql::cql_decimal_t & output) const = 0;

	virtual bool
	get_decimal(int i,
				cql::cql_decimal_t & output) const = 0;

	virtual bool
	get_varint(std::string const & column,
			   cql::cql_varint_t & output) const = 0;

	virtual bool
	get_varint(int i,
			   cql::cql_varint_t & output) const = 0;
};		
			
} // namespace cql

#endif // CQL_RESULT_H_
