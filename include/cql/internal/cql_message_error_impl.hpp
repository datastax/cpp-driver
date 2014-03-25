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

#ifndef CQL_MESSAGE_ERROR_IMPL_H_
#define CQL_MESSAGE_ERROR_IMPL_H_

#include "cql/cql.hpp"
#include "cql/internal/cql_message.hpp"

namespace cql {

class cql_message_error_impl_t :
    public cql_message_t {

public:

    cql_message_error_impl_t();

    cql_message_error_impl_t(size_t size);

    cql_message_error_impl_t(cql_int_t code,
                             const std::string& message);

    const std::string&
    message() const;

    void
    message(const std::string& m);

    cql_int_t
    code() const;

    void
    code(cql_int_t c);

    cql::cql_opcode_enum
    opcode() const;

    cql_int_t
    size() const;

    std::string
    str() const;

    bool
    consume(cql::cql_error_t* err);

    bool
    prepare(cql::cql_error_t* err);

    cql_message_buffer_t
    buffer();
        
        
    /**
     Reads the data provided with UNAVAILABLE error.
     @param[out] consistency  consistency level of the query having triggered the exception
     @param[out] required     number of nodes that should be alive to satisfy \p consistency
     @param[out] alive        number of replicas that were known to be alive when the request has been
     processed
     @return true if accessed data is indeed available, false otherwise
     */
    bool
    get_unavailable_data(cql_consistency_enum& consistency,
                         int&                  required,
                         int&                  alive) const;
        
    /**
     Reads the data provided with WRITE TIMEOUT error.
     @param[out] consistency  consistency level of the query having triggered the exception
     @param[out] received     number of nodes that acknowledged the request
     @param[out] block_for    number of replicas whose acknowledgement is required to achieve \p consistency
     @param[out] write_type   string that describes the type of the write that timed out
     @return true if accessed data is indeed available, false otherwise
     */
    bool
    get_write_timeout_data(cql_consistency_enum& consistency,
                           int&                  received,
                           int&                  block_for,
                           std::string&          write_type) const;

    /**
     Reads the data provided with READ TIMEOUT error.
     @param[out] consistency   consistency level of the query having triggered the exception
     @param[out] received      number of nodes that acknowledged the request
     @param[out] block_for     number of replicas whose acknowledgement is required to achieve \p consistency
     @param[out] data_present  it is false if replica was asked for data and did not respond or true otherwise
     @return true if accessed data is indeed available, false otherwise
    */
    bool
    get_read_timeout_data(cql_consistency_enum& consistency,
                          int&                  received,
                          int&                  block_for,
                          bool&                 data_present) const;
			
	 /**
     Reads the data provided with UNPREPARED error
     @param[out] unknown_id   the ID of the prepared statement
     @return true if accessed data is indeed available, false otherwise
    */
	bool	
	get_unprepared_data( std::vector<cql_byte_t> & unknown_id ) const;			

	 /**
     Reads the data provided with ALREADY_EXISTS_DATA error
     @param[out] keyspace   the name of the keyspace which already exits
     @param[out] table_name   the name of the table which already exists
     @return true if accessed data is indeed available, false otherwise
    */
	bool
	get_already_exists_data( std::string & keyspace, 
							 std::string & table_name ) const;	


private:
        
    bool
    _read_server_error(cql_error_t* error, std::istream& input);
    bool
    _read_protocol_error(cql_error_t* error, std::istream& input);
    bool
    _read_bad_credentials_error(cql_error_t* error, std::istream& input);
    bool
    _read_unavailable_error(cql_error_t* error, std::istream& input);
    bool
    _read_overloaded_error(cql_error_t* error, std::istream& input);
    bool
    _read_is_bootstrapping_error(cql_error_t* error, std::istream& input);
    bool
    _read_truncate_error(cql_error_t* error, std::istream& input);
    bool
    _read_write_timeout_error(cql_error_t* error, std::istream& input);
    bool
    _read_read_timeout_error(cql_error_t* error, std::istream& input);
    bool
    _read_syntax_error(cql_error_t* error, std::istream& input);
    bool
    _read_unauthorized_error(cql_error_t* error, std::istream& input);
    bool
    _read_invalid_error(cql_error_t* error, std::istream& input);
    bool
    _read_config_error(cql_error_t* error, std::istream& input);
    bool
    _read_already_exists_error(cql_error_t* error, std::istream& input);
    bool
    _read_unprepared_error(cql_error_t* error, std::istream& input);
        
    cql::cql_message_buffer_t _buffer;
    cql_int_t                 _code;
    std::string               _message;
    bool                      _is_data_read; // Was error data read?
        
    cql_consistency_enum      _consistency;
    int                       _required;
    int                       _alive;
    int                       _received;
    int                       _block_for;
    std::string               _write_type;
    bool                      _data_present;
	std::string				  _key_space_exists;
	std::string				  _table_exists;	
	std::vector<cql_byte_t>   _unknown_id_unprepared_error;
};			
			
} // namespace cql

#endif // CQL_MESSAGE_ERROR_IMPL_H_
