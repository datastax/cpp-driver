#include <cassert>

#include "cql/internal/cql_util.hpp"
#include "cql/exceptions/cql_invalid_type_exception.hpp"

cql::cql_invalid_type_exception::
    cql_invalid_type_exception(const char* param_name, const char* expected_type, const char* received_type) {
    
    assert(param_name != NULL);
    assert(expected_type != NULL);
    assert(received_type != NULL);
    
    safe_strncpy(_parameter_name, param_name, sizeof(_parameter_name));
    safe_strncpy(_expected_type, expected_type, sizeof(_expected_type));
    safe_strncpy(_received_type, received_type, sizeof(_received_type));
}

void
cql::cql_invalid_type_exception::prepare_what_buffer() const {
    snprintf(_what_buffer, sizeof(_what_buffer),
        "Received object of type: %s, expected type: %s. " 
        "(Parameter name that caused exception: %s)",
        _received_type, _expected_type, _parameter_name);
}