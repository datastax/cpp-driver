#include "cql/exceptions/cql_exception.hpp"

cql::cql_exception::cql_exception()
    : _what_buffer_initialized(false) { }

const char*
cql::cql_exception::what() const throw() {
    if(!_what_buffer_initialized) {
        prepare_what_buffer();
        _what_buffer_initialized = true;
    }
    
    return _what_buffer;
}



