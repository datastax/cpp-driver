#define BOOST_TEST_DYN_LINK

#ifdef STAND_ALONE
#define BOOST_TEST_MODULE cassandra
#endif

#include "cql/cql.hpp"
#include "cql_ccm_bridge.hpp"
#include "test_utils.hpp"
#include "policy_tools.hpp"

#include "cql/cql_session.hpp"
#include "cql/cql_cluster.hpp"
#include "cql/cql_builder.hpp"
				
#include <boost/test/unit_test.hpp>
#include <boost/test/debug.hpp>
#include "cql/policies/cql_round_robin_policy.hpp"

struct CONSISTENCY_MY_TESTS_FOR_DIFFERENT_TYPES : test_utils::CCM_SETUP {
    CONSISTENCY_MY_TESTS_FOR_DIFFERENT_TYPES() : CCM_SETUP(1,0) {}
};																						
															
BOOST_FIXTURE_TEST_SUITE( consistency_my_tests_types, CONSISTENCY_MY_TESTS_FOR_DIFFERENT_TYPES )				
									
void 
generate_random_blob(std::vector<cql::cql_byte_t>& output, 
					 int size )
{
	output.resize(size);

	for(int i = 0; i < size; ++i)
		output[i] = static_cast<cql::cql_byte_t>(rand());	
}	
		
std::string 
convert_blob_vector_to_string( std::vector<cql::cql_byte_t> const & v )
{	
	std::string res;

	for(int i = 0; i < v.size(); ++i) {	
		std::string k = ( boost::str(boost::format("%x") % static_cast< int >( v[i] ) ) );	
		if(k.length() == 1)
			res += "0";

		res += k;
	}	

	return "0x" + res;
}	
		
std::string 
convert_vec_of_bytes_to_str(std::vector<cql::cql_byte_t> const & v )
{			
	std::string result;
			
	for(int i = 0; i < v.size(); ++i) {		
		cql::cql_byte_t b[2];
		b[0] = v[ i ] & 0xF0;
		b[1] = v[ i ] & 0x0F;
		b[0] = b[0] >> 4;
		
		for(int j = 0; j < 2; ++j) {
			result += ( b[j] < 10 ) ? ( '0' + b[j] ) : ( 'a' + b[j] - 10 );
		}	

		result += " ";	
	}	
		
	return result;	
}			
						
cql::cql_bigint_t 
generate_random_int_64()
{
	cql::cql_bigint_t result( 0 );
		
	for(int i = 0; i < 8; ++i) {	
		result = result << 8;	
		result = result | static_cast< cql::cql_int_t >(rand() % 256);
	}	
		
	return result;	
}					
					
std::string 
generate_random_string(int size)
{			
	std::string res;	
	for(int i = 0; i < size; ++i) {		
		int j = rand() % 3;

		if(j == 0)
			res += unsigned char( '0' + static_cast< unsigned char >( rand() % 9 ) );
		else if(j == 1)
			res += unsigned char( 'a' + static_cast< unsigned char >( rand() % 23 ) );
		else if(j == 2)
			res += unsigned char( 'A' + static_cast< unsigned char >( rand() % 23 ) );
	}					
	return res;		
}					
					
cql::cql_int_t 
generate_random_int_32()
{		
	cql::cql_int_t result( 0 );
		
	for(int i = 0; i < 4; ++i) {	
		result = result << 8;	
		result = result | static_cast< cql::cql_int_t >(rand() % 256);
	}	
		
	return result;	
}		
	
double 
generate_random_double()
{
	cql::cql_bigint_t t1 = generate_random_int_64();
	cql::cql_bigint_t t2 = generate_random_int_64();
		
	if(t2 < -1000 || t2 > 1000){		
		double r1 = static_cast<double>(t1);
		double r2 = static_cast<double>(t2);
		int rnd = rand() % 5;			
		int count = rand() % 5 + 1;		
								
		if(rnd == 0) {					
			for(int i = 0; i < count; ++i)
				r1 = r1 / ( ( static_cast<double>(rand() ) + 100.0 ));	 
		}
		else if(rnd == 1) {		
			for(int i = 0; i < count; ++i)
				r2 = r2 / ( ( static_cast<double>( rand() ) + 100.0 ));	 
		}	
				
		double r3 = r1 / r2;
		return r3;	
	}			
	else		
		return static_cast< double >(t1) / static_cast<double>(1000000.0);	
}				
		
std::string 
generate_random_inet_v6()
{	
	std::string res;
	for(int i = 0; i < 32; ++i) {
		if(i > 3 && i % 4 == 0)
			res += ":";

		int k = rand() % 16;
		res += (boost::str(boost::format("%x") % k));	
	}

	return res;
}	
		
std::string 
generate_random_inet()
{		
	int arr[4];
	for( int i = 0; i < 4; ++i )
		arr[ i ] = rand() % 256;
		
	std::string a1 = (boost::str(boost::format("%d.%d.%d.%d") % arr[0] % arr[1] % arr[2] % arr[3]));				
	return a1;
}		
		
cql::cql_bigint_t 
generate_random_time_stamp_2()
{							
	int const max_rand(3600);
	cql::cql_bigint_t result( rand() % max_rand );
	cql::cql_bigint_t t100 = max_rand;
								
	for(int i = 0; i < 4; ++i) {		
		result = result * t100 + static_cast<cql::cql_bigint_t>(rand() % max_rand);	
	}			
	return result;
}		
		
void 
generate_random_uuid_2(std::vector<cql::cql_byte_t>& v )
{		
	v.resize( 16 );
		
	for(int i = 0; i < 16; ++i) {
		v[i] = rand() % 256;	
	}		
}			

void 
convert_timestamp_to_uuid_2( cql::cql_bigint_t ts, 
							 std::vector<cql::cql_byte_t>& v_bytes )
{			
	v_bytes.resize(16);	
			
	for(int i = 0; i < 16; ++i)		
		v_bytes[i] = rand() % 256;	
		
	std::vector<cql::cql_byte_t> chars_vec;
			
	for(int i = 0; i < 8; ++i) {	
		cql::cql_bigint_t t = ts & static_cast<cql::cql_bigint_t>(0xFF);		
		cql::cql_byte_t t2 = static_cast<cql::cql_byte_t>(t);	
		chars_vec.push_back(t2);					
		ts = ts >> 8;			
	}		
			
	v_bytes[3] = chars_vec[0];
	v_bytes[2] = chars_vec[1];
	v_bytes[1] = chars_vec[2];
	v_bytes[0] = chars_vec[3];
	v_bytes[5] = chars_vec[4];
	v_bytes[4] = chars_vec[5];
	v_bytes[7] = chars_vec[6];							
	v_bytes[6] = chars_vec[7] & 0x0F;		//// take only half of the byte, because the timeuuid is only 60 bits, not 64. 
	cql::cql_byte_t t6 = 0x10;					
	v_bytes[6] = v_bytes[6] | t6;			
	int k = 0;
}				

std::string 
make_conversion_uuid_to_string_2(std::vector<cql::cql_byte_t> const & v)
{
	std::string result;

	if(v.size() != 16)	
		return "";			
	
	for(int i = 0; i < 16; ++i) {
		cql::cql_byte_t b[2];
		b[0] = v[i] & 0xF0;
		b[1] = v[i] & 0x0F;
		b[0] = b[0] >> 4;

		if(i == 4 || i == 6 || i == 8 || i == 10)
			result += "-";	
		
		for(int j = 0; j < 2; ++j) {
			result += ( b[j] < 10 ) ? ( '0' + b[j] ) : ( 'a' + b[j] - 10 );
		}	
	}		
			
	return result;	
}		
	
			
bool	
compare_two_inet_ipv6( std::string const a1, std::string const a2 )
{		
	std::vector< std::string > v1, v2;
		
	int index1 = 0;
	int index2 = 0;
	
	while(true)
	{
		index2 = a1.find( ":", index1 );

		if( index2 < 0 )
			break;

		std::string sub = a1.substr( index1, index2 - index1 );
		v1.push_back( sub );
		index1 = index2 + 1;
	}

	v1.push_back( a1.substr( index1, 4 ) );

	index1 = 0;
	
	while(true)
	{
		index2 = a2.find( ":", index1 );

		if( index2 < 0 )
			break;
			
		std::string sub = a2.substr( index1, index2 - index1 );
		v2.push_back( sub );
		index1 = index2 + 1;
	}
		
	v2.push_back( a2.substr( index1, 4 ) );

	if( v1.size() != v2.size() )
		return false;

	for( int i = 0; i < v1.size(); ++i )
	{
		if( v1[i] == v2[i] )
			continue;

		std::string j1 = v1[ i ];
		std::string j2 = v2[ i ];

		while( j1.size() < 4 )
			j1 = "0" + j1;

		while( j2.size() < 4 )
			j2 = "0" + j2;

		if( j1 != j2 )
			return false;
	}

	return true;	
}		
	
	
/////  --run_test=consistency_my_tests_types/consistency_my_tests_2
BOOST_AUTO_TEST_CASE(consistency_my_tests_2)
{								
	int const number_of_records_inserted_to_one_table = 300;		// number of rows inserted to each table.	
				
	srand( (unsigned)time(NULL) );																			
	cql::cql_consistency_enum consistency = cql::CQL_CONSISTENCY_QUORUM;
																																				
// number of nodes in ccm --------------------------1 ---- 2 ---- 3 ---- 4 ---- 5 ---- 6 ----
//    CQL_CONSISTENCY_ANY          = 0x0000,       wrong  wrong	 wrong  wrong  wrong  wrong
//    CQL_CONSISTENCY_ONE          = 0x0001,        ok    less   less   less    ok    less
//    CQL_CONSISTENCY_TWO          = 0x0002,       wrong  wrong  wrong  wrong  wrong  wrong 
//    CQL_CONSISTENCY_THREE        = 0x0003,       wrong  wrong  wrong  wrong  wrong  wrong 
//    CQL_CONSISTENCY_QUORUM       = 0x0004,        ok     ok    less   less   less   less
//    CQL_CONSISTENCY_ALL          = 0x0005,       wrong  less   less   less   less   less
//    CQL_CONSISTENCY_LOCAL_QUORUM = 0x0006,       wrong  wrong  wrong  wrong  wrong  wrong
//    CQL_CONSISTENCY_EACH_QUORUM  = 0x0007,       wrong  wrong  wrong  wrong  wrong  wrong
				
	builder->with_load_balancing_policy(boost::shared_ptr<cql::cql_load_balancing_policy_t>(new cql::cql_round_robin_policy_t()));
	boost::shared_ptr<cql::cql_cluster_t> cluster(builder->build());
	boost::shared_ptr<cql::cql_session_t> session(cluster->connect());
														
	test_utils::query(session, str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT) % test_utils::SIMPLE_KEYSPACE % "1"));
	session->set_keyspace(test_utils::SIMPLE_KEYSPACE);					
						
	{  //// 1. Check all types in one huge table 
					
		std::string const table_name = "table_test_all";
		std::string const create_table_query = 				
		"CREATE TABLE " + table_name + " ( "
		"t00 bigint PRIMARY KEY "
		",t01 bigint "	
		",t02 ascii "
		",t03 blob "
		",t04 boolean "
		",t05 decimal "
		",t06 double "
		",t07 float "
		",t08 int "
		",t09 text "
		",t10 timestamp "
		",t11 uuid "
		",t12 timeuuid "	
		",t13 varchar "
		",t14 varint "
		",t15 inet "
		");";	
				
		test_utils::query(session, create_table_query, consistency);
						
		int const integer_number_of_rows = number_of_records_inserted_to_one_table;		// when the value is 1700 the driver hangs up. 
		
		std::map<int,cql::cql_bigint_t> t_01_map;
		std::map<int,std::string> t_02_map;
		std::map<int,std::vector<cql::cql_byte_t> > t_03_map;
		std::map<int,bool> t_04_map;
		std::map<int,double> t_05_map;
		std::map<int,double> t_06_map;
		std::map<int,float> t_07_map;
		std::map<int,int> t_08_map;
		std::map<int,std::string> t_09_map;
		std::map<int,cql::cql_bigint_t> t_10_map;
		std::map<int,std::vector<cql::cql_byte_t> > t_11_map;
		std::map<int,cql::cql_bigint_t> t_12_map;
		std::map<int,std::string> t_13_map;
		std::map<int,cql::cql_bigint_t> t_14_map;
		std::map<int,std::string> t_15_map;
			
		for(int i = 0; i < integer_number_of_rows; ++i) {	
			cql::cql_bigint_t t_01 = generate_random_int_64();
			std::string t_02 = generate_random_string(rand() % 400 + 1);
					
			std::vector< cql::cql_byte_t > t_03_2;	
			generate_random_blob(t_03_2, rand() % 2400 + 1);	
			std::string t_03 = convert_blob_vector_to_string(t_03_2);
				
			bool t_04 = (rand() % 2 == 0);
			std::string t_04_str = t_04 ? "true" : "false";

			double t_05 = generate_random_double();	
			double t_06 = generate_random_double();	
			float t_07 = static_cast< float >(generate_random_double());	
			int t_08 = generate_random_int_32();	
			std::string t_09 = generate_random_string(rand() % 2400 + 1);
			cql::cql_bigint_t t_10 = generate_random_time_stamp_2();	

			std::vector<cql::cql_byte_t> t_11_2;	
			generate_random_uuid_2(t_11_2);		
			std::string t_11 = make_conversion_uuid_to_string_2(t_11_2);

			cql::cql_bigint_t t_12_2 = generate_random_time_stamp_2();	
			std::vector<cql::cql_byte_t> t_12_3;
			convert_timestamp_to_uuid_2(t_12_2, t_12_3);
			std::string t_12 = make_conversion_uuid_to_string_2(t_12_3);
						
			std::string t_13 = generate_random_string(rand() % 2400 + 1);
			cql::cql_bigint_t t_14 = generate_random_int_64();	
					
			std::string t_15 = rand() % 2 == 0 ? generate_random_inet_v6() : generate_random_inet();
				
			t_01_map.insert(std::make_pair(i,t_01));
			t_02_map.insert(std::make_pair(i,t_02));;
			t_03_map.insert(std::make_pair(i,t_03_2));;
			t_04_map.insert(std::make_pair(i,t_04));;
			t_05_map.insert(std::make_pair(i,t_05));;
			t_06_map.insert(std::make_pair(i,t_06));;
			t_07_map.insert(std::make_pair(i,t_07));;
			t_08_map.insert(std::make_pair(i,t_08));;
			t_09_map.insert(std::make_pair(i,t_09));;
			t_10_map.insert(std::make_pair(i,t_10));;
			t_11_map.insert(std::make_pair(i,t_11_2));;
			t_12_map.insert(std::make_pair(i,t_12_2));;
			t_13_map.insert(std::make_pair(i,t_13));;
			t_14_map.insert(std::make_pair(i,t_14));;
			t_15_map.insert(std::make_pair(i,t_15));;
											
			std::string query_string( boost::str(boost::format("INSERT INTO %s (t00,t01,t02,t03,t04,t05,t06,t07,t08,t09,t10,t11,t12,t13,t14,t15) VALUES (%d, %d,'%s',%s,%s,%1.22d,%1.22d,%1.22d,%d,'%s',%d,%s,%s,'%s',%d,'%s' );")  % table_name % i % t_01 % t_02 % t_03 % t_04_str % t_05 % t_06 % t_07 % t_08 % t_09 % t_10 % t_11 % t_12 % t_13 % t_14 % t_15));			
			boost::shared_ptr<cql::cql_query_t> _query(new cql::cql_query_t(query_string,consistency));	
			session->query(_query);										
		}		
				
		boost::shared_ptr<cql::cql_result_t> result = test_utils::query(session,str(boost::format("SELECT t00,t01,t02,t03,t04,t05,t06,t07,t08,t09,t10,t11,t12,t13,t14,t15 FROM %s;") % table_name),consistency);		
				
		int number_of_rows_selected(0);
		while(result->next()) {					
			++number_of_rows_selected;

			cql::cql_bigint_t t_00_(0);
			cql::cql_bigint_t t_01_(0);
			std::string t_02_;				
			std::vector<cql::cql_byte_t> t_03_; 				
			bool t_04_(false);
			double t_05_(0.0);	
			double t_06_(0.0);	
			float t_07_(0.0);	
			int t_08_(0.0);	
			std::string t_09_;
			cql::cql_bigint_t t_10_(0.0);	
			std::vector<cql::cql_byte_t> t_11_;	
			cql::cql_bigint_t t_12_(0.0);						
			std::string t_13_;
			cql::cql_bigint_t t_14_(0.0);						

			if(!result->get_bigint(0,t_00_)) {
				BOOST_FAIL( "Wrong value for type: bigint for primary key" );
			}

			if(result->get_bigint(1,t_01_)) {
				if(t_01_map[t_00_] != t_01_) {
					std::cout << t_01_map[ t_00_ ] << " <> " <<  t_01_ << std::endl;
					BOOST_FAIL("Wrong value for type: bigint");
				}	
			}
			else {
				BOOST_FAIL( "Fail in reading data from result." );
			}

			if(result->get_ascii(2,t_02_)) {
				if(t_02_map[t_00_] != t_02_) {
					std::cout << t_02_map[t_00_] << " <> " <<  t_02_ << std::endl;
					BOOST_FAIL( "Wrong value for type: ascii" );
				}
			}
			else {
				BOOST_FAIL( "Fail in reading data from result." );
			}

			if(result->get_blob(3,t_03_)) {			//// get blob as copied vector.	
				if(t_03_map[t_00_] != t_03_) {	
					BOOST_FAIL("Wrong value for type: blob");
				}	
			}		
			else {		
				BOOST_FAIL("Fail in reading data from result.");
			}		
						
			std::pair<cql::cql_byte_t*,cql::cql_int_t> blob_ptr(NULL,-1);	// get blob as a pure pointer to a raw table of bytes. 
			if(result->get_blob(3,blob_ptr)) {		
				if(t_03_map[t_00_].size() != blob_ptr.second) {
					BOOST_FAIL("Reading blob as pointer. Wrong size.");
				}

				for(int i = 0; i < blob_ptr.second; ++i) {	
					if(blob_ptr.first[i] != t_03_map[t_00_][i]) {
						BOOST_FAIL("Wrong value for type: blob");
					}
				}		
			}		
			else {		
				BOOST_FAIL("Fail in reading data from result.");
			}		
					
			if(result->get_bool(4,t_04_)) {
				if(t_04_map[t_00_] != t_04_) {
					std::cout << t_04_map[t_00_] << " <> " <<  t_04_ << std::endl;
					BOOST_FAIL("Wrong value for type: boolean");
				}
			}
			else {
				BOOST_FAIL("Fail in reading data from result.");
			}

			if(result->get_decimal_double(5,t_05_)) {
				if(t_05_map[t_00_] != t_05_) {
					if(t_05_ != 0.0) {
						double const diff = (t_05_map[t_00_] - t_05_)/t_05_;
							
						if(diff > 1.0e-015 || diff < -1.0e-015) {		
							std::string dr1 = boost::str(boost::format("%1.25d") % t_05_map[t_00_]);
							std::string dr2 = boost::str(boost::format("%1.25d") % t_05_);
							BOOST_FAIL("The value of double is not correct. " + dr1 + " " + dr2);
						}	
					}
				}
			}
			else {
				std::cout << "The value of decimal is too big. Not possible to convert to double" << std::endl;	
			}

			if(result->get_double(6,t_06_)) {
				if(t_06_map[t_00_] != t_06_) {
					std::cout << t_06_map[t_00_] << " <> " <<  t_06_ << std::endl;
					BOOST_FAIL( "Wrong value for type: double" );
				}
			}
			else {
				BOOST_FAIL( "Fail in reading data from result." );
			}

			if(result->get_float(7,t_07_)) {
				if( t_07_map[t_00_] != t_07_) {
					std::cout << t_07_map[t_00_] << " <> " <<  t_07_ << std::endl;
					BOOST_FAIL( "Wrong value for type: float" );
				}
			}
			else {
				BOOST_FAIL( "Fail in reading data from result." );
			}

			if(result->get_int(8,t_08_)) {
				if(t_08_map[t_00_] != t_08_) {
					std::cout << t_08_map[t_00_] << " <> " <<  t_08_ << std::endl;
					BOOST_FAIL( "Wrong value for type: int" );
				}
			}
			else {
				BOOST_FAIL( "Fail in reading data from result." );
			}

			if(result->get_text(9,t_09_)) {
				if(t_09_map[t_00_] != t_09_) {
					std::cout << t_09_map[t_00_] << " <> " <<  t_09_ << std::endl;
					BOOST_FAIL( "Wrong value for type: text" );
				}
			}
			else {
				BOOST_FAIL( "Fail in reading data from result." );
			}

			if(result->get_timestamp(10,t_10_)) {
				if(t_10_map[t_00_] != t_10_) {
					std::cout << t_10_map[t_00_] << " <> " <<  t_10_ << std::endl;
					BOOST_FAIL("Wrong value for type: timestamp");
				}
			}
			else {
				BOOST_FAIL("Fail in reading data from result.");
			}		
					
			cql::cql_uuid_t uuid_;
			if(result->get_uuid(11,uuid_)) {		
				std::vector<cql::cql_byte_t> v = uuid_.get_data();
				if(t_11_map[t_00_] != v) {	
					BOOST_FAIL("Wrong value for type: uuid");
				}	
			}		
			else {		
				BOOST_FAIL("Fail in reading data from result.");
			}		
					
			if(result->get_timeuuid(12,t_12_)) {
				if(t_12_map[t_00_] != t_12_) {
					std::cout << t_12_map[t_00_] << " <> " <<  t_12_ << std::endl;
					BOOST_FAIL("Wrong value for type: timeuuid");
				}
			}
			else {
				BOOST_FAIL("Fail in reading data from result.");
			}

			if(result->get_varchar(13,t_13_)) {
				if(t_13_map[t_00_] != t_13_) {
					std::cout << t_13_map[t_00_] << " <> " <<  t_13_ << std::endl;
					BOOST_FAIL( "Wrong value for type: varchar" );
				}
			}
			else {
				BOOST_FAIL( "Fail in reading data from result." );
			}

			if(result->get_varint(14,t_14_)) {
				if(t_14_map[t_00_] != t_14_) {
					std::cout << t_14_map[t_00_] << " <> " <<  t_14_ << std::endl;
					BOOST_FAIL("Wrong value for type: varint");
				}		
			}			
			else {
				BOOST_FAIL("Fail in reading data from result.");
			}
					
			boost::asio::ip::address t_15_;
			if(result->get_inet(15, t_15_)) {
				std::string const ip_addr_str = t_15_.to_string();	
				if(t_15_map[t_00_] != ip_addr_str) {
					std::string a1 = t_15_map[t_00_];
					std::string a2 = ip_addr_str;
					if(!compare_two_inet_ipv6(a1,a2)) {
						std::cout << t_15_map[t_00_] << " <> " <<  t_15_ << std::endl;
						BOOST_FAIL("Wrong value for type: inet");
					}
				}	
			}
			else {	
				BOOST_FAIL("Fail in reading data from result.");
			}
		}			

		if(number_of_rows_selected != integer_number_of_rows) {										
			BOOST_FAIL( "varint. The number of selected rows is wrong. " );
		}				
	}					
						
	//// 1. Check blob.		
	{						
		std::string const table_name = test_utils::SIMPLE_TABLE + "_blob";
		test_utils::query(session,str(boost::format("CREATE TABLE %s(tweet_id bigint PRIMARY KEY, t1 bigint, t2 blob, t3 bigint );") % table_name),consistency);
			
		std::map<int,std::vector<cql::cql_byte_t> > blob_map;																							
		int const integer_number_of_rows = number_of_records_inserted_to_one_table;		// when the value is 1700 the driver hangs up. 
																	
		for(int i = 0; i < integer_number_of_rows; ++i) {								
			std::vector<cql::cql_byte_t> blob_vector;
			generate_random_blob(blob_vector, rand() % 10000 + 1);
			std::string blob_str = convert_blob_vector_to_string(blob_vector);					
			blob_map.insert(std::make_pair(i, blob_vector ));
			std::string query_string(boost::str(boost::format("INSERT INTO %s (tweet_id,t1,t2,t3) VALUES (%d,%d,%s,%d);") % table_name % i % i % blob_str % i ));	
			boost::shared_ptr<cql::cql_query_t> _query(new cql::cql_query_t(query_string,consistency));	
			session->query(_query);										
		}		
						
		boost::shared_ptr<cql::cql_result_t> result = test_utils::query(session,str(boost::format("SELECT t1, t2, t3 FROM %s;") % table_name),consistency);		
		cql::cql_bigint_t row_count(0);

		int number_of_rows_selected(0);
		while(result->next()) {					
			++number_of_rows_selected;
					
			cql::cql_bigint_t t1(0), t2(0);										
			std::vector<cql::cql_byte_t> blob_1;
			std::pair<cql::cql_byte_t*,cql::cql_int_t> blob_2; 
				
			result->get_bigint(0,t1);
			result->get_bigint(2,t2);
				
			if(!result->get_blob(1,blob_1)) {			
				BOOST_FAIL("Blob. Error receiving blob as vector." );
			}		
				
			if(!result->get_blob(1,blob_2)) {			
				BOOST_FAIL("Blob. Error receiving blob as pointer");
			}		
							
			std::map<int,std::vector<cql::cql_byte_t> >::const_iterator p = blob_map.find(t1);			
				
			if(p == blob_map.end()) {
				BOOST_FAIL("Wrong. No such element in the blob_map.");
			}

			if(blob_1 != p->second) {
				BOOST_FAIL("The elements of the blobs do not fit.");
			}

			if(p->second.size() != blob_2.second) {	
				BOOST_FAIL("The elements of the blobs do not fit.");
			}	
				
			for(int i = 0; i < p->second.size(); ++i) {
				if(p->second[i] != blob_2.first[i]) {	
					BOOST_FAIL("The elements of the blobs in the pointer do not fit.");
				}	
			}		
		}		
				
		if(number_of_rows_selected != integer_number_of_rows) {										
			BOOST_FAIL( "varint. The number of selected rows is wrong. " );
		}					
	}					
					
	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
							
	//// 1. Check inet.		
	{						
		std::string const table_name = test_utils::SIMPLE_TABLE + "_var_inet";
		test_utils::query(session,str(boost::format("CREATE TABLE %s(tweet_id bigint PRIMARY KEY, t1 bigint, t2 inet, t3 bigint );") % table_name ),consistency);

		std::map<int,std::string> inet_map;																							
		int const integer_number_of_rows = number_of_records_inserted_to_one_table;		// when the value is 1700 the driver hangs up. 
																	
		for(int i = 0; i < integer_number_of_rows; ++i) {											
			std::string inet2;
					
			if(i % 2 == 0)
				inet2 = generate_random_inet();
			else	
				inet2 = generate_random_inet_v6(); 
					
			inet_map.insert(std::make_pair(i,inet2));		
			std::string query_string(boost::str(boost::format("INSERT INTO %s (tweet_id,t1,t2,t3) VALUES (%d,%d,'%s',%d);") % table_name % i % i % inet2 % i ));	
			boost::shared_ptr<cql::cql_query_t> _query(new cql::cql_query_t(query_string,consistency));	
			session->query(_query);										
		}		
						
		boost::shared_ptr<cql::cql_result_t> result = test_utils::query(session,str(boost::format("SELECT t1, t2, t3 FROM %s;") % table_name),consistency );
		cql::cql_bigint_t row_count(0);

		int number_of_rows_selected(0);
		while(result->next()) {					
			++number_of_rows_selected;
					
			cql::cql_bigint_t t1(0), t2(0);
						
			result->get_bigint(0,t1);
			result->get_bigint(2,t2);					
					
			boost::asio::ip::address inet_3;						
			if(result->get_inet(1,inet_3)) {			
				std::map<int,std::string>::const_iterator p = inet_map.find(t1);
				if(p == inet_map.end()) {					
					BOOST_FAIL("There is no such element in inet map.");
				}		
												
				if(inet_3.to_string() != p->second) {	
					std::string a1 = inet_3.to_string();
					std::string a2 = p->second;	
					if(!compare_two_inet_ipv6(a1,a2)) {
						BOOST_FAIL("The value of inet is not correct.");
					}
				}	
			}		
			else {			
				BOOST_FAIL("Error. A valid inet is reported as an invalid inet.");
			}	
		}		

		if(number_of_rows_selected != integer_number_of_rows) {										
			BOOST_FAIL("varint. The number of selected rows is wrong.");
		}					
	}					

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	//// 2. Check the varint.	
						
	{					
		std::string const table_name = test_utils::SIMPLE_TABLE + "_var_int";
		test_utils::query(session,str(boost::format("CREATE TABLE %s(tweet_id bigint PRIMARY KEY, t1 bigint, t2 varint, t3 bigint );") % table_name ),consistency);
					
		std::map<int,cql::cql_bigint_t> varint_map;
																							
		int const integer_number_of_rows = number_of_records_inserted_to_one_table;		// when the value is 1700 the driver hangs up. 
																	
		for(int i = 0; i < integer_number_of_rows; ++i)
		{				
			cql::cql_bigint_t ii = generate_random_int_64();	
					
			if(i < 10) {		//// check also the values from -5 to 5. 
				ii = i - 5;
			}		
			else if(i < 70) {				
				ii = ii/( static_cast<cql::cql_bigint_t>(rand()) * static_cast<cql::cql_bigint_t>(rand()) * static_cast<cql::cql_bigint_t>(i) + static_cast<cql::cql_bigint_t>(10));		// use also small numbers.	
			}					
				
			varint_map.insert(std::make_pair(i,ii));
			std::string query_string( boost::str(boost::format("INSERT INTO %s (tweet_id,t1,t2,t3) VALUES (%d,%d,%d,%d);") % table_name % i % i % ii % i ));	
			boost::shared_ptr<cql::cql_query_t> _query(new cql::cql_query_t(query_string,consistency));	
			session->query(_query);										
		}				
						
		boost::shared_ptr<cql::cql_result_t> result = test_utils::query(session,str(boost::format("SELECT t1, t2, t3 FROM %s;") % table_name),consistency);

		cql::cql_bigint_t row_count(0);

		int number_of_rows_selected(0);
		while(result->next()) {					
			++number_of_rows_selected;
					
			cql::cql_bigint_t t1(0), t2(0);						
			result->get_bigint(0,t1);
			result->get_bigint(2,t2);							
					
			cql::cql_bigint_t varint_2(0);
						
			if(result->get_varint(1,varint_2)) {					
				std::map<int,cql::cql_bigint_t>::const_iterator p = varint_map.find(t1);
				if(p == varint_map.end()) {					
					BOOST_FAIL("There is no such element in varint map.");
				}		
						
				std::cout << varint_2 << " " << p->second << std::endl;
						
				if(varint_2 != p->second) {
					BOOST_FAIL("The value of INT64 is not correct. ");
				}
			}	
			else {			
				BOOST_FAIL("Error. A valid varint is reported as an invalid varint.");
			}	
		}		

		if(number_of_rows_selected != integer_number_of_rows) {										
			BOOST_FAIL("varint. The number of selected rows is wrong.");
		}											
	}					
										
	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	{	//// 3. Check the 32-int.		
		test_utils::query(session,str(boost::format("CREATE TABLE %s(tweet_id bigint PRIMARY KEY, t1 bigint, t2 decimal, t3 bigint );") % test_utils::SIMPLE_TABLE),consistency);
														
		std::map<int,cql::cql_int_t> int_map;
																							
		int const integer_number_of_rows = number_of_records_inserted_to_one_table;		// when the value is 1700 the driver hangs up. 
																
		for(int i = 0; i < integer_number_of_rows; ++i) {											
			cql::cql_int_t ii = generate_random_int_32();
										
			if(i < 10)		//// check also the values from -5 to 5. 
				ii = i - 5;				
										
			int_map.insert(std::make_pair(i,ii));
			std::string query_string( boost::str(boost::format("INSERT INTO %s (tweet_id,t1,t2,t3) VALUES (%d,%d,%d,%d);") % test_utils::SIMPLE_TABLE % i % i % ii % i));	
			boost::shared_ptr<cql::cql_query_t> _query(new cql::cql_query_t(query_string,consistency));	
			session->query(_query);										
		}			
					
		cql::cql_bigint_t row_count(0);										
		boost::shared_ptr<cql::cql_result_t> result = test_utils::query(session, str(boost::format("SELECT t1, t2, t3 FROM %s;") % test_utils::SIMPLE_TABLE),consistency);
										
		int number_of_rows_selected(0);
		while(result->next()) {			
			++number_of_rows_selected;
			cql::cql_bigint_t t1(0), t2(0);
						
			result->get_bigint(0,t1);
			result->get_bigint(2,t2);
			
			if(result->get_decimal_is_int(1)) {
				cql::cql_int_t r(0);
				result->get_decimal_int(1,r);
						
				std::map<int,cql::cql_int_t>::const_iterator p = int_map.find(t1);
				if(p == int_map.end()) {					
					BOOST_FAIL("There is no such element in INT32 map.");
				}			
						
				if(r != p->second) {			
					BOOST_FAIL("The value of INT32 is not correct.");
				}	
					
				{	//// retrieve it also with int64 and compare the results.						
					cql::cql_bigint_t bi(0);
					result->get_decimal_int_64(1,bi);
					cql::cql_bigint_t bi2 = r;
					if(bi2 != bi) {	
						BOOST_FAIL("An int32 value retrieved as int64 gave a different result.");
					}	
				}		
			}			
			else {			
				BOOST_FAIL("An INT32 value is consisdered as a not valid int32. ");
			}																	
		}				
							
		if(number_of_rows_selected != integer_number_of_rows) {												
			BOOST_FAIL("INT32. The number of selected rows is wrong.");
		}					
	}					
						
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	//						
	{	//// 4. Check the 64-int.	
						
		std::string const table_name = test_utils::SIMPLE_TABLE + "_int64";					
		test_utils::query(session,str(boost::format("CREATE TABLE %s(tweet_id bigint PRIMARY KEY, t1 bigint, t2 decimal, t3 bigint );") % table_name),consistency);					
		std::map<int,cql::cql_bigint_t> int64_map;
													
		int const integer_number_of_rows = number_of_records_inserted_to_one_table;											
		for(int i = 0; i < integer_number_of_rows; ++i)
		{											
			cql::cql_bigint_t ii = generate_random_int_64();	
					
			if(i < 10) {		//// check also the values from -5 to 5. 
				ii = i - 5;
			}		
			else if(i < 70) {				
				ii = ii/( static_cast<cql::cql_bigint_t>(rand())*static_cast<cql::cql_bigint_t>(rand())*static_cast<cql::cql_bigint_t>(i) + static_cast<cql::cql_bigint_t>(10));	// use also small numbers.	
			}				
					
			int64_map.insert(std::make_pair(i,ii));
			std::string query_string( boost::str(boost::format("INSERT INTO %s (tweet_id,t1,t2,t3) VALUES (%d,%d,%d,%d);") % table_name % i % i % ii % i ));	
			boost::shared_ptr<cql::cql_query_t> _query(new cql::cql_query_t(query_string,consistency));	
			session->query(_query);										
		}			
											
		boost::shared_ptr<cql::cql_result_t> result = test_utils::query(session, str(boost::format("SELECT t1, t2, t3 FROM %s ;") % table_name),consistency);
										
		int number_of_rows_selected(0);
		while(result->next()) {					
			++number_of_rows_selected;

			cql::cql_bigint_t t1(0),t2(0);						
			result->get_bigint(0,t1);
			result->get_bigint(2,t2);
					
			if(result->get_decimal_is_int_64(1)) {
				cql::cql_bigint_t r(0);
				result->get_decimal_int_64(1,r);
					
				std::map<int,cql::cql_bigint_t>::const_iterator p = int64_map.find(t1);
				if(p == int64_map.end()) {					
					BOOST_FAIL("There is no such element in INT64 map.");
				}		

				if(r != p->second) {
					BOOST_FAIL("The value of INT64 is not correct.");
				}

				if(result->get_decimal_is_int(1)) {		//// the value is so small that it should be able to retrieve as int32.
					cql::cql_int_t i32(0);
					result->get_decimal_int(1,i32);
					
					cql::cql_bigint_t i64 = i32;
					if(i64 != r) {	
						BOOST_FAIL("A small int32 variable retrieved as int32 has wrong value.");
					}	
				}															
			}
			else {			
				BOOST_FAIL("Error. A valid int64 is reported as an invalid int64.");
			}
		}				
						
		if(number_of_rows_selected != integer_number_of_rows) {				
			BOOST_FAIL("INT64. The number of selected rows is wrong.");
		}						
	}					
						
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	//			
	{	//// 5. Check the double	
						
		std::string const table_name = test_utils::SIMPLE_TABLE + "_double_test";			
		test_utils::query(session,str(boost::format("CREATE TABLE %s(tweet_id bigint PRIMARY KEY, t1 bigint, t2 decimal, t3 bigint );") % table_name ),consistency);					
		std::map<int,double> double_map;									
		int const integer_number_of_rows = number_of_records_inserted_to_one_table;			
													
		for(int i = 0; i < integer_number_of_rows; ++i) {					
			double ii = generate_random_double();	
					
			if(i < 10)
				ii = i - 5;	
												
			double_map.insert(std::make_pair(i,ii));
			std::string query_string(boost::str(boost::format("INSERT INTO %s (tweet_id,t1,t2,t3) VALUES (%d,%d,%1.25d,%d);") % table_name % i % i % ii % i ));
			boost::shared_ptr<cql::cql_query_t> _query(new cql::cql_query_t(query_string,consistency));	
			session->query(_query);								
		}										
																												
		boost::shared_ptr<cql::cql_result_t> result = test_utils::query(session,str(boost::format("SELECT t1, t2, t3 FROM %s;") % table_name),consistency);
										
		int number_of_rows_selected(0);
		while(result->next())
		{					
			++number_of_rows_selected;
			cql::cql_bigint_t t1(0), t2(0);						
			result->get_bigint(0,t1);
			result->get_bigint(2,t2);							
								
			if(result->get_decimal_is_double(1)) {		
				double r(0);
				result->get_decimal_double(1,r);
					
				std::map<int,double>::const_iterator p = double_map.find(t1);
				if(p == double_map.end()) {					
					BOOST_FAIL("There is no such element in double map.");
				}		
						
				if(r != p->second) {							
					if(p->second != 0.0) {			
						double const diff = (r - p->second)/p->second;
							
						if(diff > 1.0e-015 || diff < -1.0e-015) {		
							std::string dr1 = boost::str(boost::format("%1.25d") % r);
							std::string dr2 = boost::str(boost::format("%1.25d") % p->second);
							BOOST_FAIL("The value of double is not correct. " + dr1 + " " + dr2);
						}	
					}													
				}				
							
				if(result->get_decimal_is_int_64(1)) {		//// this value also is reported as a valid int64. 
					cql::cql_bigint_t bi(0);
					result->get_decimal_int_64(1,bi);
						
					double bi2 = bi;						
					if(bi2 != r)
					{	
						BOOST_FAIL("The value retrieved as int64 gave a different result.");
					}	
				}		
			}					
			else {
				std::cout << "Not a valid double value" << std::endl;	
			}			
		}				
						
		if(number_of_rows_selected != integer_number_of_rows) {					
			std::cout << "WRONG. The number of rows is WRONG. " << std::endl;	
			BOOST_FAIL("double. The number of selected rows is wrong.");
		}				
		else {
			std::cout << "The number of rows is OK. " << std::endl;	
		}			
	}					
}					
					
BOOST_AUTO_TEST_SUITE_END()	
				
			