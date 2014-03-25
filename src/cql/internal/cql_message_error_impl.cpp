/*
 *      Copyright (C) 2013 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
#include <sstream>
#include <boost/foreach.hpp>

#include "cql/internal/cql_vector_stream.hpp"
#include "cql/internal/cql_serialization.hpp"
#include "cql/internal/cql_defines.hpp"

#include "cql/internal/cql_message_error_impl.hpp"

cql::cql_message_error_impl_t::cql_message_error_impl_t() :
    _buffer(new std::vector<cql::cql_byte_t>()),
    _code(-1),
    _message(),
    _is_data_read(false)
{}

cql::cql_message_error_impl_t::cql_message_error_impl_t(size_t size) :
    _buffer(new std::vector<cql::cql_byte_t>(size)),
    _code(-1),
    _message(),
    _is_data_read(false)
{}

cql::cql_message_error_impl_t::cql_message_error_impl_t(cql::cql_int_t code,
        const std::string& message) :
    _buffer(new std::vector<cql::cql_byte_t>(0)),
    _code(code),
    _message(message),
    _is_data_read(false)
{}

const std::string&
cql::cql_message_error_impl_t::message() const {
    return _message;
}

void
cql::cql_message_error_impl_t::message(const std::string& m) {
    _message = m;
}

cql::cql_int_t
cql::cql_message_error_impl_t::code() const {
    return _code;
}

void
cql::cql_message_error_impl_t::code(cql::cql_int_t c) {
    _code = c;
}

cql::cql_opcode_enum
cql::cql_message_error_impl_t::opcode() const {
    return CQL_OPCODE_ERROR;
}

cql::cql_int_t
cql::cql_message_error_impl_t::size() const {
    return _buffer->size();
}

std::string
cql::cql_message_error_impl_t::str() const {
    return _message;
}

bool
cql::cql_message_error_impl_t::consume(cql::cql_error_t* err) {
    cql::vector_stream_t buffer(*_buffer);
    std::istream input(&buffer);
			
    cql::decode_int(input, _code);
    cql::decode_string(input, _message);
			
    switch (_code) {
        case CQL_ERROR_SERVER:					//// 0x0000
            _is_data_read = _read_server_error(err, input);
            break;
        case CQL_ERROR_PROTOCOL:				//// 0x000A
            _is_data_read = _read_protocol_error(err, input);
            break;
        case CQL_ERROR_BAD_CREDENTIALS:			//// 0x0100
            _is_data_read = _read_bad_credentials_error(err, input);
            break;
        case CQL_ERROR_UNAVAILABLE:				//// 0x1000
            _is_data_read = _read_unavailable_error(err, input);
            break;
        case CQL_ERROR_OVERLOADED:				//// 0x1001
            _is_data_read = _read_overloaded_error(err, input);
            break;
        case CQL_ERROR_IS_BOOTSTRAPPING:		//// 0x1002
            _is_data_read = _read_is_bootstrapping_error(err, input);
            break;
        case CQL_ERROR_TRUNCATE:				//// 0x1003
            _is_data_read = _read_truncate_error(err, input);
            break;
        case CQL_ERROR_WRITE_TIMEOUT:			//// 0x1100
            _is_data_read = _read_write_timeout_error(err, input);
            break;
        case CQL_ERROR_READ_TIMEOUT:			//// 0x1200
            _is_data_read = _read_read_timeout_error(err, input);
            break;
        case CQL_ERROR_SYNTAX:					//// 0x2000
            _is_data_read = _read_syntax_error(err, input);
            break;
        case CQL_ERROR_UNAUTHORIZED:			//// 0x2100
            _is_data_read = _read_unauthorized_error(err, input);
            break;
        case CQL_ERROR_INVALID:					//// 0x2200
            _is_data_read = _read_invalid_error(err, input);
            break;	
        case CQL_ERROR_CONFIG:					//// 0x2300
            _is_data_read = _read_config_error(err, input);
            break;
        case CQL_ERROR_ALREADY_EXISTS:			//// 0x2400 
            _is_data_read = _read_already_exists_error(err, input);
            break;	
        case CQL_ERROR_UNPREPARED:				//// 0x2500
            _is_data_read = _read_unprepared_error(err, input);
            break;
        default: {
            assert(0 && "Cassandra responded with unknown error code.");
        }	
    }
    
    return true;
}

bool
cql::cql_message_error_impl_t::prepare(cql::cql_error_t*) {
    _buffer->resize(sizeof(_code) + sizeof(cql::cql_short_t) + _message.size());

    cql::vector_stream_t buffer(*_buffer);
    std::ostream output(&buffer);

    cql::encode_int(output, _code);
    cql::encode_string(output, _message);
    return true;
}

cql::cql_message_buffer_t
cql::cql_message_error_impl_t::buffer() {
    return _buffer;
}


bool
cql::cql_message_error_impl_t::get_unavailable_data(
    cql_consistency_enum& consistency,
    int&                  required,
    int&                  alive) const
{
    if ((cql_cassandra_error_code_enum)_code != CQL_ERROR_UNAVAILABLE
            || !_is_data_read) {
        return false;
    }
    consistency = _consistency;
    required    = _required;
    alive       = _alive;
    return true;
}

bool
cql::cql_message_error_impl_t::get_write_timeout_data(
    cql_consistency_enum& consistency,
    int&                  received,
    int&                  block_for,
    std::string&          write_type) const
{
    if ((cql_cassandra_error_code_enum)_code != CQL_ERROR_WRITE_TIMEOUT
            || !_is_data_read) {
        return false;
    }
    
    consistency = _consistency;
    received    = _received;
    block_for   = _block_for;
    write_type  = _write_type;
    return true;
}

bool
cql::cql_message_error_impl_t::get_read_timeout_data(
    cql_consistency_enum& consistency,
    int&                  received,
    int&                  block_for,
    bool&                 data_present) const
{
    if ((cql_cassandra_error_code_enum)_code != CQL_ERROR_READ_TIMEOUT
            || !_is_data_read) {
        return false;
    }

    consistency  = _consistency;
    received     = _received;
    block_for    = _block_for;
    data_present = _data_present;
    return true;
}


bool 
cql::cql_message_error_impl_t::get_unprepared_data( 
													std::vector<cql_byte_t> & unknown_id ) const 
{			
	if ((cql_cassandra_error_code_enum)_code != CQL_ERROR_UNPREPARED
       || !_is_data_read) {
        return false;
    }	
		
	unknown_id = _unknown_id_unprepared_error;
	return true;
}		


bool
cql::cql_message_error_impl_t::get_already_exists_data( std::string & keyspace, 
														std::string & table_name ) const
{
	if ((cql_cassandra_error_code_enum)_code != CQL_ERROR_ALREADY_EXISTS
		|| !_is_data_read) {
        return false;
    }		
			
	keyspace = _key_space_exists;
	table_name = _table_exists;
	return true;
}			


bool  
cql::cql_message_error_impl_t::_read_server_error(cql_error_t* error, std::istream& input)
{		    
    return input.fail() ? false : true;		
}		
	
bool 
cql::cql_message_error_impl_t::_read_protocol_error(cql_error_t* error, std::istream& input)
{		    
    return input.fail() ? false : true;		
}		
		
bool 
cql::cql_message_error_impl_t::_read_bad_credentials_error(cql_error_t* error, std::istream& input)
{
    return input.fail() ? false : true;		
}

bool
cql::cql_message_error_impl_t::_read_unavailable_error(cql_error_t* error, std::istream& input)
{
    cql_short_t consistency_short;
    decode_short(input, consistency_short);
    _consistency = (cql_consistency_enum)consistency_short;
    
    decode_int(input, _required);
    decode_int(input, _alive);

    return input.fail() ? false : true; // It is important to tell whether succeeded.
}		
					
bool
cql::cql_message_error_impl_t::_read_overloaded_error(cql_error_t* error, std::istream& input)
{		
    return input.fail() ? false : true; 
}		
				
bool
cql::cql_message_error_impl_t::_read_is_bootstrapping_error(cql_error_t* error, std::istream& input)
{		
    return input.fail() ? false : true; 
}		
				
bool
cql::cql_message_error_impl_t::_read_truncate_error(cql_error_t* error, std::istream& input)
{
    return input.fail() ? false : true; 
}

bool
cql::cql_message_error_impl_t::_read_write_timeout_error(cql_error_t* error, std::istream& input)
{
    cql_short_t consistency_short;
    decode_short(input, consistency_short);
    _consistency = (cql_consistency_enum)consistency_short;
    
    decode_int(input, _received);
    decode_int(input, _block_for);
    decode_string(input, _write_type);
    
    return input.fail() ? false : true; // It is important to tell whether succeeded.
}

bool
cql::cql_message_error_impl_t::_read_read_timeout_error(cql_error_t* error, std::istream& input)
{
    cql_short_t consistency_short;
    cql::decode_short(input, consistency_short);
    _consistency = (cql_consistency_enum)consistency_short;
    
    decode_int(input, _received);
    decode_int(input, _block_for);
    decode_bool(input, _data_present);
    
    return input.fail() ? false : true; // It is important to tell whether succeeded.
}
		
bool	
cql::cql_message_error_impl_t::_read_syntax_error(cql_error_t* error, std::istream& input)
{		    
    return input.fail() ? false : true;		
}		
	
bool	
cql::cql_message_error_impl_t::_read_unauthorized_error(cql_error_t* error, std::istream& input)
{		
    return input.fail() ? false : true;		
}		
		
bool	
cql::cql_message_error_impl_t::_read_invalid_error(cql_error_t* error, std::istream& input)
{
    return input.fail() ? false : true;		
}
		
bool	
cql::cql_message_error_impl_t::_read_config_error(cql_error_t* error, std::istream& input)
{
    return input.fail() ? false : true;		
}						
						
							
bool					
cql::cql_message_error_impl_t::_read_already_exists_error(cql_error_t* error, std::istream& input)
{			
	std::string key_space;						
	std::string table_name;						
	decode_string(input, _key_space_exists);			
	decode_string(input, _table_exists );									
    return input.fail() ? false : true;			
}				

	
bool	
cql::cql_message_error_impl_t::_read_unprepared_error(cql_error_t* error, std::istream& input) 
{
	decode_short_bytes( input, _unknown_id_unprepared_error );
    return input.fail() ? false : true;			
}		
		
