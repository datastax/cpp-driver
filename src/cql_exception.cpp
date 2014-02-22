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
#include "cql_util.hpp"
#include "cql_exception.hpp"

cql::cql_exception::cql_exception(
		const char* message)
		: _buffer(empty_when_null(message)), 
		  _buffer_allocated(false) { }

cql::cql_exception::cql_exception(
		const std::string& message)
	:   _buffer(""), 
        _buffer_allocated(false)
{
	size_t string_length = message.length() + 1; 

	char* buffer = (char *) malloc(sizeof(char) * string_length);
	if(buffer) {
		safe_strncpy(buffer, message.c_str(), string_length);

		this->_buffer = buffer;
		this->_buffer_allocated = true;
	}
}

cql::cql_exception::~cql_exception() throw()
{
	if(_buffer_allocated && _buffer) {
		free((void *)_buffer);
		_buffer = 0;
	}
}

const char*
cql::cql_exception::what() const throw() {
	return _buffer;
}



