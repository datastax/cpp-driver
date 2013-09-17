/*
 * File:   cql_exception.hpp
 * Author: mc
 *
 * Created on September 11, 2013, 5:53 PM
 */

#ifndef CQL_EXCEPTION_HPP
#define	CQL_EXCEPTION_HPP

#include <cstdlib>
#include <string>
#include <exception>

namespace cql {

// Base class for all exceptions thrown by the driver.
class cql_exception: public std::exception {
public:
    // Constructor version for STATIC strings
    cql_exception(const char* message);

	// Constructor version for dynamically generated strings
	cql_exception(const std::string& message);

	virtual ~cql_exception();

	// Returns text message that describes exception.
	virtual const char* what() const throw();

private:
    bool _buffer_allocated;
	const char* _buffer;
};

}

#endif	/* CQL_EXCEPTION_HPP */

