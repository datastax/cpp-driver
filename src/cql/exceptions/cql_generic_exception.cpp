#include <cstdlib>
#include <cassert>

#include "cql/internal/cql_util.hpp"
#include "cql/exceptions/cql_generic_exception.hpp"


cql::cql_generic_exception::cql_generic_exception(const char* message) {
    assert(message != NULL);
    safe_strncpy(_what_buffer, message, sizeof(_what_buffer));
}
    
void
cql::cql_generic_exception::prepare_what_buffer() const { }