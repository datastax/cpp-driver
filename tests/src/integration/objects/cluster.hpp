/*
  Copyright (c) 2016 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __TEST_CLUSTER_HPP__
#define __TEST_CLUSTER_HPP__
#include "cassandra.h"

#include "objects/object_base.hpp"

#include "objects/session.hpp"

#include <string>

#include <gtest/gtest.h>

namespace test {
namespace driver {

/**
 * Wrapped cluster object (builder)
 */
class Cluster : public Object<CassCluster, cass_cluster_free> {
public:
  /**
   * Create the cluster for the builder object
   */
  Cluster()
    : Object<CassCluster, cass_cluster_free>(cass_cluster_new()) {}

  /**
   * Destroy the cluster
   */
  virtual ~Cluster() {};

  /**
   * Build/Create the cluster
   *
   * @return Cluster object
   */
  static Cluster build() {
    return Cluster();
  }

  /**
   * Assign/Append the contact points; passing an empty string will clear
   * the contact points
   *
   * @param contact_points A comma delimited list of hosts (addresses or
   *                       names
   * @return Cluster object
   */
  Cluster& with_contact_points(const std::string& contact_points) {
    EXPECT_EQ(CASS_OK, cass_cluster_set_contact_points(get(),
      contact_points.c_str()));
    return *this;
  }

  /**
   * Enable/Disable the use of hostname resolution
   *
   * This is useful for authentication (Kerberos) or encryption (SSL)
   * services that require a valid hostname for verification.
   *
   * NOTE: Not available if using libuv 0.10.x or earlier
   *
   * @param enable True if hostname resolution should be enable; false
   *               otherwise (default: false)
   * @return Cluster object
   */
  Cluster& with_hostname_resolution(bool enable = true) {
    EXPECT_EQ(CASS_OK, cass_cluster_set_use_hostname_resolution(get(),
      (enable == true ? cass_true : cass_false)));
    return *this;
  }

  /**
   * Assign the use of a particular binary protocol version; driver will
   * automatically downgrade to the lowest server supported version on
   * connection
   *
   * @param protocol_version Binary protocol version
   * @return Cluster object
   */
  Cluster& with_protocol_version(int protocol_version) {
    EXPECT_EQ(CASS_OK, cass_cluster_set_protocol_version(get(),
      protocol_version));
    return *this;
  }

  /**
   * Enable/Disable the schema metadata
   *
   * If disabled this allows the driver to skip over retrieving and
   * updating schema metadata, but it also disables the usage of token-aware
   * routing and session->schema() will always return an empty object. This
   * can be useful for reducing the startup overhead of short-lived sessions
   *
   * @param enable True if schema metada should be enabled; false otherwise
   *               (default: true)
   * @return Cluster object
   */
  Cluster& with_schema_metadata(bool enable = true) {
    cass_cluster_set_use_schema(get(), (enable == true ? cass_true : cass_false));
    return *this;
  }

  /**
   * Create a new session and establish a connection to the server;
   * synchronously
   *
   * @param keyspace Keyspace to use (default: None)
   * @return Session object
   * @throws Session::Exception If session could not be established
   */
  Session connect(const std::string& keyspace = "") {
    return Session::connect(get(), keyspace);
  }
};

} // namespace driver
} // namespace test

#endif // __TEST_CLUSTER_HPP__
