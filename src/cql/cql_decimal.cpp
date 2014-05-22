#include <cmath>

#include "cql/cql_decimal.hpp"

cql::cql_decimal_t::cql_decimal_t(cql_byte_t* bytes, int size)
{
	_data.resize( size );
	for( int i = 0; i < size; ++i )
		_data[i] = bytes[i];
}

cql::cql_decimal_t::cql_decimal_t(const std::vector<cql_byte_t>& bytes) : 
	_data( bytes )
{
}

cql::cql_decimal_t::cql_decimal_t()
{
}

cql::cql_decimal_t::cql_decimal_t(cql_decimal_t const & a)
{
	this->_data = a._data;
}

cql::cql_decimal_t & cql::cql_decimal_t::operator=(cql_decimal_t const & a)
{
	this->_data = a._data;
	return *this;	
}

std::vector<cql::cql_byte_t> const
cql::cql_decimal_t::get_data() const
{
	return _data;
}

bool
cql::cql_decimal_t::is_convertible_to_int32()
{
	if(_data.size() < 5)
		return false;		//// there must be at least four bytes in the vector.	
			
	if(_data[0] != 0 || _data[1] != 0 || _data[2] != 0 || _data[3] != 0)		//// the first four bytes must be zero. 
		return false;		

	return(_data.size() <= 8);	// it will be convertible to int32 if there is less than 9 bytes in the vector. 		
}

bool
cql::cql_decimal_t::is_convertible_to_int64()
{
	if(_data.size() < 5)
		return false;				//// there must be at least four bytes in the vector.	
			
	if(_data[0] != 0 || _data[1] != 0 || _data[2] != 0 || _data[3] != 0)		//// the first four bytes must be zero. 
		return false;		

	return(_data.size() <= 12);		//// it will be convertible to int32 if there less than 12 bytes in the vector. 
}

bool
cql::cql_decimal_t::is_convertible_to_double()
{
	if(_data.size() < 5)
		return false;		//// there must be at least four bytes in the vector.	
			
	if(_data[0] != 0 || _data[1] != 0 || _data[2] != 0)		//// the first three bytes must be zero. 
		return false;		

	if(_data.size() == 5 && _data[4] == 0) 
		return true;		//// the value inside is exactly zero.	
					
	if(_data.size() > 12)		//// the data consists of more than 64 bits. It will not be convertible to double.;	
		return false;

	return true;
}

bool
cql::cql_decimal_t::convert_to_int32(cql::cql_int_t & output)
{
	std::vector<cql_byte_t> const & v = _data;

	if(v.size() < 5)
		return false;		//// there must be at least four bytes in the vector.	
			
	if(v[0] != 0 || v[1] != 0 || v[2] != 0 || v[3] != 0)		//// the first four bytes must be zero. 
		return false;		
		
	if(v.size() > 8)		//// the data consists of more than 32 bits. It will not be convertible to int32;	
		return false;
				
	cql::cql_byte_t first_digit = v[4];		//// take the first digit of the data bytes.
	cql::cql_byte_t const last_byte_first_digit = first_digit & 0x80;		//// take the most meaning bit of the first byte.				
	cql::cql_byte_t arr[4];				//// temporary array for making conversion to int. 
	cql::cql_byte_t byte_for_filling(0x00);		//// the byte for filling in the more significant bytes. 
		
	if(last_byte_first_digit == 0x80) {	//// the most meaning bit of the first byte is set to 1. It means that the value is NEGATIVE. 
	   byte_for_filling = 0xFF;		//// the filing will be with 0xFF value.
	}		
			
	int const number_of_bytes_for_filling(8 - v.size());
			
	for(int i = 0; i < number_of_bytes_for_filling; ++i) {		
		arr[i] = byte_for_filling;		
	}			
				
	int const number_of_bytes_to_copy(v.size() - 4);	
				
	for(int i = 0; i < number_of_bytes_to_copy; ++i) {		
		arr[i + number_of_bytes_for_filling ] = v[i + 4];	
	}		
			
	int result(0);
			
	for(int i = 0;  i < 4; ++i) {
		result = result << 8;	
		result = result | static_cast< int >(arr[i]);
	}		

	output = result;									
	return true;
}

bool
cql::cql_decimal_t::convert_to_int64(cql::cql_bigint_t & output)
{
	std::vector<cql_byte_t> const & v = _data;

	if(v.size() < 5)
		return false;		//// there must be at least four bytes in the vector.	
			
	if(v[0] != 0 || v[1] != 0 || v[2] != 0 || v[3] != 0)		//// the first four bytes must be zero. 
		return false;		
		
	if(v.size() > 12)		//// the data consists of more than 64 bits. It will not be convertible to int64;	
		return false;
	
	cql::cql_byte_t const first_digit = v[4];		//// take the first digit of the data bytes.
	cql::cql_byte_t const last_byte_first_digit = first_digit & 0x80;		//// take the most meaning bit of the first byte.	
	cql::cql_byte_t arr[8];							//// temporary array for making conversion to int. 
	cql::cql_byte_t byte_for_filling(0x00);			//// the byte for filling in the more significant bytes. 

	if(last_byte_first_digit == 0x80) {	//// the most meaning bit of the first byte is set to 1. It means that the value is NEGATIVE. 
		byte_for_filling = 0xFF;		//// the filing will be with 0xFF value.
	}		
			
	int const number_of_bytes_for_filling(12 - v.size());
			
	for(int i = 0; i < number_of_bytes_for_filling; ++i) {		
		arr[i] = byte_for_filling;
	}			
				
	int const number_of_bytes_to_copy(v.size() - 4);	
					
	for(int i = 0; i < number_of_bytes_to_copy; ++i) {		
		arr[i + number_of_bytes_for_filling] = v[i + 4];	
	}		
			
	cql::cql_bigint_t result(0);
			
	for(int i = 0;  i < 8; ++i) {
		result = result << 8;	
		result = result | static_cast< int >(arr[i]);
	}		
			
	output = result;						
	return true;
}

bool
cql::cql_decimal_t::convert_to_double(double & output)
{
	std::vector<cql_byte_t> const & v = _data;

	if(v.size() < 5)
		return false;		//// there must be at least four bytes in the vector.	
			
	if(v[0] != 0 || v[1] != 0 || v[2] != 0)		//// the first three bytes must be zero. 
		return false;		

	if(v.size() == 5 && v[ 4 ] == 0) {
		output = 0.0;		//// the first bytes are zero. This is ZERO. 
		return true;		
	}	
			
	if(v.size() > 12)		//// the data consists of more than 64 bits. It will not be convertible to double.;	
		return false;
				
	cql::cql_byte_t const first_digit = v[4];								//// take the first digit of the data bytes.
	cql::cql_byte_t const last_byte_first_digit = first_digit & 0x80;		//// take the most meaning bit of the first byte.
	cql::cql_byte_t arr[8];						//// temporary array for making conversion to int64.	 
	cql::cql_byte_t byte_for_filling(0x00);		//// the byte for filling in the more significant bytes. 

	if(last_byte_first_digit == 0x80) {	//// the most meaning bit of the first byte is set to 1. It means that the value is NEGATIVE. 
		byte_for_filling = 0xFF;		//// the filing will be with 0xFF value.
	}		
			
	int const number_of_bytes_for_filling(12 - v.size());
			
	for(int j = 0; j < number_of_bytes_for_filling; ++j) {		
		arr[j] = byte_for_filling;		
	}			
				
	int const number_of_bytes_to_copy(v.size() - 4);	
					
	for(int j = 0; j < number_of_bytes_to_copy; ++j) {		
		arr[j + number_of_bytes_for_filling] = v[j + 4];	
	}			
			
	cql::cql_bigint_t result(0);
			
	for(int j = 0; j < 8; ++j) {
		result = result << 8;	
		result = result | static_cast< int >(arr[j]);
	}		

	unsigned char const number_of_position_after_dot(v[3]);
		
	double const f1 = static_cast< double >(result);

	if(number_of_position_after_dot == 0) {
		output = f1;
		return true;
	}		
			
	output = f1 * ::pow(10.0, -number_of_position_after_dot);
	return true;			
}


