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

#ifndef CQL_MESSAGE_RESULT_IMPL_H_
#define CQL_MESSAGE_RESULT_IMPL_H_

#include <boost/asio/buffer.hpp>

#include "cql/cql.hpp"
#include "cql/cql_result.hpp"
#include "cql/cql_uuid.hpp"

#include "cql/internal/cql_util.hpp"
#include "cql/internal/cql_message.hpp"
#include "cql/internal/cql_result_metadata.hpp"

namespace cql {

class cql_uuid_t;
    
class cql_message_result_impl_t :
    boost::noncopyable,
public cql_message_t,
    public cql_result_t {

public:
    cql_message_result_impl_t(size_t size);

    cql_message_result_impl_t();

    cql::cql_result_type_enum
    result_type() const;

    cql::cql_opcode_enum
    opcode() const;

    cql_int_t
    size() const;

    std::string
    str() const;

    size_t
    column_count() const;

    size_t
    row_count() const;
        
    const std::vector<cql::cql_byte_t>&
    query_id() const;

    void
    set_as_traced();
        
    bool
    get_tracing_id(cql_uuid_t& tracing_id_) const;
        
    bool
    next();

    bool
    consume(cql::cql_error_t* err);

    bool
    prepare(cql::cql_error_t* err);

    cql_message_buffer_t
    buffer();

    const cql_result_metadata_t&
    get_metadata();

    bool
    exists(const std::string& column) const;

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
    column_type(int i,
                cql_column_type_enum& output) const;

    bool
    column_type(const std::string& column,
                cql_column_type_enum& output) const;

    bool
    get_index(const std::string& column,
              int& output) const;

    bool
    get_nullity(int i,
                bool& output) const;

    bool
    get_nullity(const std::string& column,
                bool& output) const;

    bool
    get_bool(int i,
             bool& output) const;

    bool
    get_bool(const std::string& column,
             bool& output) const;

    bool
    get_int(int i,
            cql_int_t& output) const;

    bool
    get_int(const std::string& column,
            cql_int_t& output) const;

    bool
    get_float(int i,
              float& output) const;

    bool
    get_float(const std::string& column,
              float& output) const;

    bool
    get_double(int i,
               double& output) const;

    bool
    get_double(const std::string& column,
               double& output) const;

    bool
    get_bigint(int i,
               cql::cql_bigint_t& output) const;

    bool
    get_bigint(const std::string& column,
               cql::cql_bigint_t& output) const;

    bool
    get_string(int i,
               std::string& output) const;

    bool
    get_string(const std::string& column,
               std::string& output) const;
	
    CQL_DEPRECATED bool
    get_data(int i,
             cql::cql_byte_t** output,
             cql::cql_int_t& size) const;
				
    CQL_DEPRECATED bool		
    get_data(const std::string& column,
             cql::cql_byte_t** output,
             cql::cql_int_t& size) const;

	virtual bool
    get_data(int i,
			 std::vector< cql::cql_byte_t > & output ) const;
			
    virtual bool
    get_data(const std::string& column,
             std::vector< cql::cql_byte_t > & output ) const;	
			
    CQL_DEPRECATED bool
    get_list(int i,
             cql::cql_list_t** output) const;

	bool
    get_list(int i,
             boost::shared_ptr< cql::cql_list_t > & output) const;	

    CQL_DEPRECATED bool
    get_list(const std::string& column,
             cql::cql_list_t** output) const;

    bool
    get_list(const std::string& column,	
             boost::shared_ptr< cql::cql_list_t > & output) const;	

    CQL_DEPRECATED bool
    get_set(int i,
            cql::cql_set_t** output) const;

    bool
    get_set(int i,
            boost::shared_ptr< cql::cql_set_t > & output) const;

    CQL_DEPRECATED bool
    get_set(const std::string& column,
            cql::cql_set_t** output) const;

    bool
    get_set(const std::string& column,
            boost::shared_ptr< cql::cql_set_t > & output) const;

    CQL_DEPRECATED bool
    get_map(int i,
            cql::cql_map_t** output) const;

	bool
    get_map(int i,	
            boost::shared_ptr< cql::cql_map_t > & output) const;

    CQL_DEPRECATED bool
    get_map(const std::string& column,
            cql::cql_map_t** output) const;

    bool
    get_map(const std::string& column,
            boost::shared_ptr< cql::cql_map_t > & output) const;
				
    bool		
    get_keyspace_name(std::string& output) const;
        
    inline bool
    is_valid(int i,
             cql::cql_column_type_enum column_type) const {
        bool index_null = false;
        if (!get_nullity(i, index_null) || index_null) {
            return false;
        }

        cql::cql_column_type_enum t = cql::CQL_COLUMN_TYPE_UNKNOWN;
        if (!_metadata.column_type(i, t)) {
            return false;
        }

        if (column_type != t) {
            return false;
        }

        return (*reinterpret_cast<cql::cql_int_t*>(_row[i]) != 0);
    }			

	bool
    get_counter(int i,
               cql::cql_bigint_t& output) const;

    bool
    get_counter(const std::string& column,
               cql::cql_bigint_t& output) const;

	bool
    get_ascii(int i,
			  std::string& output) const;
		
    bool
    get_ascii(const std::string& column,
			  std::string& output) const;
		
	bool	
    get_varchar(int i,
				std::string& output) const;

    bool	
    get_varchar(const std::string& column,
				std::string& output) const;

	bool
    get_uuid(int i,
			 cql_uuid_t& output) const;

    bool
    get_uuid(const std::string& column,
             cql_uuid_t& output) const;

	bool
    get_uuid(int i,
			 std::string& output) const;
				
    bool
    get_uuid(const std::string& column,
             std::string & output) const;
		
	bool
	get_timestamp(int i,
				  cql::cql_bigint_t& output) const;
				
	bool
	get_timestamp(const std::string& column,
				  cql::cql_bigint_t& output) const;		
			
	bool
	get_timeuuid(int i,
				 cql::cql_bigint_t& output) const;
		
	bool
	get_timeuuid(const std::string& column,
				 cql::cql_bigint_t& output) const;	

	bool
	get_blob(int i, 
			 std::vector<cql::cql_byte_t>& output) const;
					
	bool
	get_blob(std::string const & column, 
	         std::vector<cql::cql_byte_t>& output) const;

	bool
	get_blob(int i,
		     std::pair<cql::cql_byte_t*,cql::cql_int_t>& output) const;
					
	bool
	get_blob(std::string const& column, 
	         std::pair<cql::cql_byte_t*,cql::cql_int_t>& output) const;

	bool
    get_text(int i,
			 std::string& output) const;
				
    bool
    get_text(const std::string& column,
			 std::string& output) const;
				
	bool
	get_inet(int i, 
			 boost::asio::ip::address & output) const;
					
	bool
	get_inet(std::string const& column,
			 boost::asio::ip::address & output) const;

	bool
	get_decimal(std::string const & column,
				cql::cql_decimal_t & output ) const;

	bool
	get_decimal(int i,
				cql::cql_decimal_t & output ) const;

	bool
	get_varint(std::string const & column,
			   cql::cql_varint_t & output ) const;

	bool
	get_varint(int i,
			   cql::cql_varint_t & output ) const;
				
private:
    cql::cql_message_buffer_t     _buffer;
    cql::cql_byte_t*              _pos;
    std::vector<cql::cql_byte_t*> _row;
    size_t                        _row_pos;
    cql_int_t                     _row_count;
    cql_int_t                     _column_count;
    std::vector<cql::cql_byte_t>  _query_id;
    cql::cql_result_type_enum     _result_type;
    std::string                   _keyspace_name;
    std::string                   _table_name;
    cql::cql_result_metadata_t    _metadata;
    cql_uuid_t                    _tracing_id;
    bool                          _am_i_traced;
};

} // namespace cql

#endif // CQL_MESSAGE_RESULT_IMPL_H_


