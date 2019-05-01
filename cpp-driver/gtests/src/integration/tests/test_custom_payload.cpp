/*
  Copyright (c) DataStax, Inc.

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

#include "integration.hpp"

/**
 * Custom payload integration tests; single node cluster
 */
class CustomPayloadTests : public Integration {
public:
  void SetUp() {
    // Call the parent setup function
    is_session_requested_ = false;
    is_ccm_start_requested_ = false;
    Integration::SetUp();

    // Stop the active cluster to apply custom payload mirroring query handler
    if (!ccm_->is_cluster_down()) {
      ccm_->stop_cluster();
    }

    // Restart the cluster with the appropriate JVM arguments and establish connection
    ccm_->start_cluster("-Dcassandra.custom_query_handler_class=org.apache.cassandra.cql3."
                        "CustomPayloadMirroringQueryHandler");
    connect();
  }
};

/**
 * Perform a custom payload execution using a simple statement
 *
 * This test will perform a custom payload execution using a simple statement
 * and validate the results against single node cluster.
 *
 * @test_category queries:custom_payload
 * @since core:2.2.0-beta1
 * @cassandra_version 2.2.0
 * @expected_result Custom payload is executed and validated
 */
CASSANDRA_INTEGRATION_TEST_F(CustomPayloadTests, Simple) {
  CHECK_FAILURE;
  CHECK_VERSION(2.2.0);

  // Create the custom payload to be associated with the statement
  CustomPayload custom_payload;
  custom_payload.set("key1", Blob("value1"));
  custom_payload.set("key2", Blob("value2"));
  custom_payload.set("key3", Blob("value3"));

  // Create and execute the statement with the applied custom payload
  Statement statement(SELECT_ALL_SYSTEM_LOCAL_CQL);
  statement.set_custom_payload(custom_payload);
  Result result = session_.execute(statement);

  // Validate the custom payload was applied on the server side
  ASSERT_EQ(custom_payload.item_count(), result.custom_payload().item_count());
  for (size_t i = 0; i < custom_payload.item_count(); i++) {
    std::pair<const std::string, Blob> expected_item = custom_payload.item(i);
    std::pair<const std::string, Blob> item = result.custom_payload().item(i);
    ASSERT_EQ(expected_item.first, item.first);
    ASSERT_EQ(expected_item.second, item.second);
  }
}
