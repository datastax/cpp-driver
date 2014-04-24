#include "cql\cql_varint.hpp"

//// a template generic function to create a VARINT of any length from the byte array representation.
//// this function can create a boost::multiprecision::int_cpp object from this array of bytes. 
template< typename T >
bool 
deserialize_varint(int i, T& output, std::vector<cql::cql_byte_t> v)
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

cql::cql_varint_t::cql_varint_t(cql_byte_t* bytes, int size)
{
	_data.resize( size );
	for( int i = 0; i < size; ++i )
		_data[i] = bytes[i];
}

cql::cql_varint_t::cql_varint_t(const std::vector<cql_byte_t>& bytes) : 
	_data( bytes )
{
}

cql::cql_varint_t::cql_varint_t()
{
}

cql::cql_varint_t::cql_varint_t(cql_varint_t const & a)
{
	this->_data = a._data;
}

cql::cql_varint_t & cql::cql_varint_t::operator=(cql_varint_t const & a)
{
	this->_data = a._data;	
	return *this;
}

std::vector<cql::cql_byte_t> const
cql::cql_varint_t::get_data() const
{
	return _data;
}

bool
cql::cql_varint_t::is_convertible_to_int64()
{
	return(_data.size() >= 1 && _data.size() <= 8);	
}

bool
cql::cql_varint_t::	convert_to_int64(cql::cql_bigint_t & output)
{
	if( !is_convertible_to_int64() )
		return false;

	std::vector<cql_byte_t> const & v = _data;

	cql::cql_byte_t const first_digit = v[0];	//// take the first digit of the data bytes.
	cql::cql_byte_t const last_byte_first_digit = first_digit & 0x80;		//// take the most meaning bit of the first byte.		
	cql::cql_byte_t arr[8];						//// temporary array for making conversion to int. 
	cql::cql_byte_t byte_for_filling(0x00);		//// the byte for filling in the more significant bytes. 

	if(last_byte_first_digit == 0x80) {			//// the most meaning bit of the first byte is set to 1. It means that the value is NEGATIVE. 
		byte_for_filling = 0xFF;				//// the filing will be with 0xFF value.
	}		
			
	int const number_of_bytes_for_filling(8 - v.size());
			
	for(int j = 0; j < number_of_bytes_for_filling; ++j) {		
		arr[j] = byte_for_filling;		
	}			
					
	int const number_of_bytes_to_copy(v.size());	
				
	for(int i = 0; i < number_of_bytes_to_copy; ++i) {			
		arr[i + number_of_bytes_for_filling] = v[i];	
	}		
			
	cql::cql_bigint_t result(0);
			
	for(int i = 0;  i < 8; ++i) {
		result = result << 8;	
		result = result | static_cast< int >(arr[i]);
	}		
			
	output = result;		
	return true;	
}		
