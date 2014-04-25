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

#ifndef CQL_VARINT_HPP_
#define	CQL_VARINT_HPP_

#include "cql/cql.hpp"

#include <string>
#include <vector>

namespace cql {

//// a template generic function to create a VARINT of any length from the cassandra byte array representation.
//// This function can create a boost::multiprecision::int_cpp object from this array of bytes. 
template< typename T >
bool 
deserialize_varint(T& output, std::vector<cql::cql_byte_t> v)
{
	std::size_t const t = v.size();
		
	if(t < 1)
		return false;		

	if(t == 1 && v[0] == 0) {
		output = 0;
		return true;
	}
		
	cql::cql_byte_t const last_byte_first_digit = v[0] & 0x80;		//// take the most meaning bit of the first byte.
	T r(0);

	if(last_byte_first_digit == 0x80) {								//// the most meaning bit of the first byte is set to 1. It means that the value is NEGATIVE. 		
		for(std::size_t i = 0; i < v.size(); ++i)
			v[i] = v[i] ^ 0xFF;										//// negate all bits to negate the value
	}

	for(std::size_t i = 0; i < t; ++i) {
		r = r << 8;
		r = r | T(v[i]);	
	}	

	if(last_byte_first_digit == 0x80) {								//// the most meaning bit of the first byte is set to 1. It means that the value is NEGATIVE. 		
		r = (r + 1) * T(-1);				
	}

	output = r;	
	return true;		
}


class CQL_EXPORT cql_varint_t
{
public:

	explicit cql_varint_t(cql_byte_t* bytes, int size);
    explicit cql_varint_t(const std::vector<cql_byte_t>& bytes);
    cql_varint_t();
    cql_varint_t(cql_varint_t const & a);
	cql_varint_t & operator=(cql_varint_t const & a);

	std::vector<cql_byte_t> const
	get_data() const;

	bool
	is_convertible_to_int64();

	bool
	convert_to_int64(cql::cql_bigint_t & output);

private:
	std::vector<cql_byte_t> _data;
};

}

#endif	/* CQL_VARINT_HPP_ */
