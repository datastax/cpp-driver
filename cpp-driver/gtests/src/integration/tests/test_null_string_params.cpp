/*
  Copyright (c) 2017 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#include "integration.hpp"
#include "cassandra.h"

/**
 * Null string api args test
 */
class NullStringApiArgsTest : public Integration {
public:
  static void SetUpTestCase() {
  }

  static void TearDownTestCase() {
  }

  void SetUp() {
    //TODO: Update test to work with remote deployments
    // Ensure test can run for current configuration
#ifdef _WIN32
    return;
#endif
#ifdef CASS_USE_LIBSSH2
    if (Options::deployment_type() == CCM::DeploymentType::REMOTE) {
      return;
    }
#endif

    // Call the parent setup function (override startup and session connection)
    Integration::SetUp();
    test_cluster_ = cass_cluster_new();
  }

  void TearDown() {
    cass_cluster_free(test_cluster_);
  }

protected:
  CassCluster *test_cluster_;
};


/**
 * Perform connection to DSE using Kerberos authentication
 *
 * This test will perform a connection to a DSE server using Kerberos/GSSAPI
 * authentication against a single node cluster and execute a simple query.
 *
 * @jira_ticket CPP-368
 * @test_category cass:basic
 * @since 1.0.0
 * @expected_result Do not crash
 */
CASSANDRA_INTEGRATION_TEST_F(NullStringApiArgsTest, NullStringArgs) {
  cass_cluster_set_contact_points(test_cluster_, NULL);
}
