/*
 * File:   cql_exception.hpp
 * Author: mc
 *
 * Created on September 11, 2013, 5:53 PM
 */

#ifndef CQL_EXCEPTION_HPP
#define	CQL_EXCEPTION_HPP

#include <exception>

namespace cql {
// Size of buffer used to hold what message and other strings
// in cql exceptions.
const size_t CQL_EXCEPTION_BUFFER_SIZE = 128;

// Size of buffer used to format what() message.
const size_t CQL_EXCEPTION_WHAT_BUFFER_SIZE = 512;

// Base class for all exceptions throwed by driver.
class cql_exception: public std::exception {
public:
    cql_exception(const char* message)
        : _message(message) { }

    virtual const char* what() const throw() {
        return _message;
    }

private:
    const char* const _message;
};

}

#endif	/* CQL_EXCEPTION_HPP */

