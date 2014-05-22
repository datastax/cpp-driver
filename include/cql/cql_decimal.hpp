/*
 This file is part of cassandra.
 
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
 
 http://www.apache.org/licenses/LICENSE-2.0
 
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

#ifndef CQL_DECIMAL_HPP_
#define	CQL_DECIMAL_HPP_

#include "cql/cql.hpp"
#include <vector>

namespace cql {
		
class CQL_EXPORT cql_decimal_t
{
public:	

	explicit cql_decimal_t(cql_byte_t* bytes, int size);	
	explicit cql_decimal_t(const std::vector<cql_byte_t>& bytes);
	cql_decimal_t();
	cql_decimal_t(cql_decimal_t const & a);
	cql_decimal_t & operator=(cql_decimal_t const & a);

	std::vector<cql_byte_t> const
	get_data() const;

	bool
	is_convertible_to_int32();

	bool
	is_convertible_to_int64();

	bool
	is_convertible_to_double();

	bool
	convert_to_int32(cql::cql_int_t & output);

	bool
	convert_to_int64(cql::cql_bigint_t & output);

	bool
	convert_to_double(double & output);
	
private:	
	std::vector<cql_byte_t> _data;

};

}


#endif	/* CQL_DECIMAL_HPP_ */
