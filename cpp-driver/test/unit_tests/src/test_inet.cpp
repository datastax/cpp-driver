/*
  Copyright (c) 2014-2016 DataStax

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

#ifdef STAND_ALONE
#   define BOOST_TEST_MODULE cassandra
#endif

#include "cassandra.h"

#include <boost/test/unit_test.hpp>

#include <string.h>

BOOST_AUTO_TEST_SUITE(inet)

BOOST_AUTO_TEST_CASE(ipv4)
{
  // From string and back
  const char* ip_address = "127.0.0.1";
  CassInet inet;
  BOOST_CHECK(cass_inet_from_string(ip_address, &inet) == CASS_OK);

  char output[CASS_INET_STRING_LENGTH];
  cass_inet_string(inet, output);
  BOOST_CHECK(strcmp(ip_address, output) == 0);


  // Inavlid address
  BOOST_CHECK(cass_inet_from_string("<invalid>", &inet) == CASS_ERROR_LIB_BAD_PARAMS);
  BOOST_CHECK(cass_inet_from_string("127.0.0.", &inet) == CASS_ERROR_LIB_BAD_PARAMS);

}

BOOST_AUTO_TEST_CASE(ipv6)
{
  // From string and back
  const char* ip_address = "ffff::ffff:b3ff:fe1e:8329";
  CassInet inet;
  BOOST_CHECK(cass_inet_from_string(ip_address, &inet) == CASS_OK);

  char output[CASS_INET_STRING_LENGTH];
  cass_inet_string(inet, output);
  BOOST_CHECK(strcmp(ip_address, output) == 0);

  // Inavlid address
  BOOST_CHECK(cass_inet_from_string("ffff", &inet) == CASS_ERROR_LIB_BAD_PARAMS);
}

BOOST_AUTO_TEST_CASE(length)
{
  const char* ip_address = "127.0.0.1";
  const char* ip_address_junk = "127.0.0.1<junk>";

  CassInet inet;
  BOOST_CHECK(cass_inet_from_string_n(ip_address_junk, strlen(ip_address), &inet) == CASS_OK);

  char output[CASS_INET_STRING_LENGTH];
  cass_inet_string(inet, output);
  BOOST_CHECK(strcmp(ip_address, output) == 0);

  const char* max_ip_address = "ffff:ffff:ffff:ffff:ffff:ffff:255.255.255.255";
  const char* max_ip_address_v6 = "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"; // Last octets converted to hex

  BOOST_CHECK(cass_inet_from_string_n(max_ip_address, strlen(max_ip_address), &inet) == CASS_OK);

  cass_inet_string(inet, output);
  BOOST_CHECK(strcmp(max_ip_address_v6, output) == 0);

  // Too long
  const char* too_long = "ffff:ffff:ffff:ffff:ffff:ffff:255.255.255.255_";

  BOOST_CHECK(cass_inet_from_string_n(too_long, strlen(too_long), &inet) == CASS_ERROR_LIB_BAD_PARAMS);
}

BOOST_AUTO_TEST_SUITE_END()

