#define BOOST_TEST_DYN_LINK
#ifdef STAND_ALONE
#   define BOOST_TEST_MODULE cassandra
#endif

#include <boost/test/unit_test.hpp>

#include "cassandra.h"
#include "test_utils.hpp"

struct ConfigTests {
    ConfigTests() { }
};

BOOST_FIXTURE_TEST_SUITE(config, ConfigTests)


BOOST_AUTO_TEST_CASE(test_options)
{
  test_utils::CassClusterPtr cluster(cass_cluster_new());

  {
    cass_size_t data_length;
    cass_size_t connect_timeout = 9999;
    cass_cluster_setopt(cluster.get(), CASS_OPTION_CONNECT_TIMEOUT, &connect_timeout, sizeof(connect_timeout));
    cass_size_t connect_timeout_out = 0;
    data_length = sizeof(connect_timeout_out);
    cass_cluster_getopt(cluster.get(), CASS_OPTION_CONNECT_TIMEOUT, &connect_timeout_out, &data_length);
    BOOST_REQUIRE(connect_timeout == connect_timeout_out && data_length == sizeof(connect_timeout_out));
  }

  {
    cass_size_t data_length;
    cass_int32_t port = 7000;
    cass_cluster_setopt(cluster.get(), CASS_OPTION_PORT, &port, sizeof(port));
    cass_int32_t port_out = 0;
    data_length = sizeof(port_out);
    cass_cluster_getopt(cluster.get(), CASS_OPTION_PORT, &port_out, &data_length);
    BOOST_REQUIRE(port == port_out && data_length == sizeof(port_out));
  }
}


BOOST_AUTO_TEST_CASE(test_invalid)
{
  test_utils::CassClusterPtr cluster(cass_cluster_new());

  cass_size_t temp = 0;
  BOOST_REQUIRE(cass_cluster_setopt(cluster.get(), CASS_OPTION_CONNECT_TIMEOUT, &temp, sizeof(temp) - 1) == CASS_ERROR_LIB_INVALID_OPTION_SIZE);

  cass_size_t temp_out = 0;
  cass_size_t temp_out_size = sizeof(temp_out) - 1;
  BOOST_REQUIRE(cass_cluster_getopt(cluster.get(), CASS_OPTION_CONNECT_TIMEOUT, &temp_out, &temp_out_size) == CASS_ERROR_LIB_INVALID_OPTION_SIZE);

}

BOOST_AUTO_TEST_CASE(test_contact_points)
{
  char buffer[1024];
  cass_size_t buffer_size;
  test_utils::CassClusterPtr cluster(cass_cluster_new());

  // Simple
  const char* contact_points1 = "127.0.0.1,127.0.0.2,127.0.0.3";
  cass_cluster_setopt(cluster.get(), CASS_OPTION_CONTACT_POINTS, contact_points1, strlen(contact_points1));
  buffer_size = sizeof(buffer);
  cass_cluster_getopt(cluster.get(), CASS_OPTION_CONTACT_POINTS, buffer, &buffer_size);
  BOOST_REQUIRE(strcmp(contact_points1, buffer) == 0);

  // Clear
  cass_cluster_setopt(cluster.get(), CASS_OPTION_CONTACT_POINTS, "", 0);
  buffer_size = sizeof(buffer);
  cass_cluster_getopt(cluster.get(), CASS_OPTION_CONTACT_POINTS, buffer, &buffer_size);
  BOOST_REQUIRE(buffer_size == 0);

  // Extra commas
  const char* contact_points1_commas = ",,,,127.0.0.1,,,,127.0.0.2,127.0.0.3,,,,";
  cass_cluster_setopt(cluster.get(), CASS_OPTION_CONTACT_POINTS, contact_points1_commas, strlen(contact_points1_commas));
  buffer_size = sizeof(buffer);
  cass_cluster_getopt(cluster.get(), CASS_OPTION_CONTACT_POINTS, buffer, &buffer_size);
  BOOST_REQUIRE(strcmp(contact_points1, buffer) == 0);

  // Clear
  cass_cluster_setopt(cluster.get(), CASS_OPTION_CONTACT_POINTS, "", 0);
  buffer_size = sizeof(buffer);
  cass_cluster_getopt(cluster.get(), CASS_OPTION_CONTACT_POINTS, buffer, &buffer_size);
  BOOST_REQUIRE(buffer_size == 0);

  // Extra whitespace
  const char* contact_points1_ws = "   ,\r\n,  ,   ,  127.0.0.1 ,,,  ,\t127.0.0.2,127.0.0.3,  \t\n, ,,   ";
  cass_cluster_setopt(cluster.get(), CASS_OPTION_CONTACT_POINTS, contact_points1_ws, strlen(contact_points1_ws));
  buffer_size = sizeof(buffer);
  cass_cluster_getopt(cluster.get(), CASS_OPTION_CONTACT_POINTS, buffer, &buffer_size);
  BOOST_REQUIRE(strcmp(contact_points1, buffer) == 0);

  // Clear
  cass_cluster_setopt(cluster.get(), CASS_OPTION_CONTACT_POINTS, "", 0);
  buffer_size = sizeof(buffer);
  cass_cluster_getopt(cluster.get(), CASS_OPTION_CONTACT_POINTS, buffer, &buffer_size);
  BOOST_REQUIRE(buffer_size == 0);

  // Append
  const char* contact_point1 = "127.0.0.1";
  cass_cluster_setopt(cluster.get(), CASS_OPTION_CONTACT_POINTS, contact_point1, strlen(contact_point1));

  const char* contact_point2 = "127.0.0.2";
  cass_cluster_setopt(cluster.get(), CASS_OPTION_CONTACT_POINTS, contact_point2, strlen(contact_point2));

  const char* contact_point3 = "127.0.0.3";
  cass_cluster_setopt(cluster.get(), CASS_OPTION_CONTACT_POINTS, contact_point3, strlen(contact_point3));

  buffer_size = sizeof(buffer);
  cass_cluster_getopt(cluster.get(), CASS_OPTION_CONTACT_POINTS, buffer, &buffer_size);
  BOOST_REQUIRE(strcmp(contact_points1, buffer) == 0);
}

BOOST_AUTO_TEST_SUITE_END()


