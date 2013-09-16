/*
 * File:   cql_exception.hpp
 * Author: mc
 *
 * Created on September 11, 2013, 5:53 PM
 */

#ifndef CQL_EXCEPTION_HPP
#define	CQL_EXCEPTION_HPP

#include <cstdlib>
#include <exception>

namespace cql {
// Size of buffer used to hold user message and other strings
// in cql exceptions.
const size_t CQL_EXCEPTION_BUFFER_SIZE = 128;

// Size of buffer used to format what() message.
const size_t CQL_EXCEPTION_WHAT_BUFFER_SIZE = 512;

// Base class for all exceptions throwed by driver.
class cql_exception: public std::exception {
public:
    // Returns text message that describes exception.
    virtual const char* what() const throw();
    
protected:
    // This constructor doesn't initialize what buffer
    cql_exception();
    
    // Method overriden in derived classes that is responisble 
    // for populating what message buffer.
    // This is only called on first call to what.
    virtual void prepare_what_buffer() const = 0;
    
    // We allow use of dynamically generated strings in exceptions
    // by coping message passed by user to internal buffer.
    // We cannot use std::string here, because its copy
    // constructor can throw exception (e.g. no memory available),
    // which will result in std::terminate().
    mutable char _what_buffer[CQL_EXCEPTION_WHAT_BUFFER_SIZE];
    
private:
    mutable bool _what_buffer_initialized;
};

}

#endif	/* CQL_EXCEPTION_HPP */

