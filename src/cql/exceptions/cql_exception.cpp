#include "cql/internal/cql_util.hpp"
#include "cql/exceptions/cql_exception.hpp"

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



