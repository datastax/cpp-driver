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

