
/*
 * File:   cql_already_exists_exception.hpp
 * Author: mc
 *
 * Created on September 16, 2013, 9:53 AM
 */

#ifndef CQL_ALREADY_EXISTS_EXCEPTION_H_
#define	CQL_ALREADY_EXISTS_EXCEPTION_H_

#include "cql/exceptions/cql_query_validation_exception.hpp"

namespace cql {
class cql_already_exists_exception: public cql_exception {
public:
    cql_already_exists_exception(const char* keyspace, const char* table);
	
	//  Gets the name of keyspace that either already exists or is home to the table that
	//  already exists.
	const char* keyspace() const throw();

	//  Gets whether the query yielding this exception was a table creation
	//  attempt.
	const char* table() const throw();

	//  Gets whether the query yielding this exception was a table creation
	//  attempt.
	bool table_creation() const throw();

	virtual const char* what() const throw();
private:
	char _keyspace_buffer[CQL_EXCEPTION_BUFFER_SIZE];
	char _table_buffer[CQL_EXCEPTION_BUFFER_SIZE];
	mutable char _what_buffer[CQL_EXCEPTION_WHAT_BUFFER_SIZE];
};
}

#endif	/* CQL_ALREADY_EXISTS_EXCEPTION_H_ */
