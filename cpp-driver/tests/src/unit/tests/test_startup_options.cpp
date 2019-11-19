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

#include "unit.hpp"

#include "driver_info.hpp"
#include "query_request.hpp"
#include "session.hpp"

#define APPLICATION_NAME "DataStax C/C++ Test Harness"
#define APPLICATION_VERSION "1.0.0"

using namespace datastax;
using namespace datastax::internal;
using namespace datastax::internal::core;

inline bool operator==(const CassUuid& rhs, const CassUuid& lhs) {
  return rhs.clock_seq_and_node == lhs.clock_seq_and_node &&
         rhs.time_and_version == lhs.time_and_version;
}

inline bool operator!=(const CassUuid& rhs, const CassUuid& lhs) { return !(rhs == lhs); }

class StartupRequestUnitTest : public Unit {
public:
  void TearDown() {
    ASSERT_TRUE(session_.close()->wait_for(WAIT_FOR_TIME));
    Unit::TearDown();
  }

  Session& session() { return session_; }
  const String& client_id() const { return client_id_; }
  Config& config() { return config_; }
  const mockssandra::RequestHandler* simple_with_client_options() {
    mockssandra::SimpleRequestHandlerBuilder builder;
    builder.on(mockssandra::OPCODE_QUERY)
        .system_local()
        .system_peers()
        .client_options() // Allow for fake query to get client options
        .empty_rows_result(1);
    return builder.build();
  }

  void connect() {
    config_.contact_points().push_back(Address("127.0.0.1", 9042));
    internal::core::Future::Ptr connect_future(session_.connect(config_));
    ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME))
        << "Timed out waiting for session to connect";
    ASSERT_FALSE(connect_future->error()) << cass_error_desc(connect_future->error()->code) << ": "
                                          << connect_future->error()->message;

    char client_id[CASS_UUID_STRING_LENGTH];
    cass_uuid_string(session_.client_id(), client_id);
    client_id_ = client_id;
  }

  Map<String, String> client_options() {
    Request::ConstPtr request(new QueryRequest(CLIENT_OPTIONS_QUERY, 0));
    ResponseFuture::Ptr future = static_cast<ResponseFuture::Ptr>(session_.execute(request));
    EXPECT_TRUE(future->wait_for(WAIT_FOR_TIME)) << "Timed out executing query";
    EXPECT_FALSE(future->error()) << cass_error_desc(future->error()->code) << ": "
                                  << future->error()->message;

    Map<String, String> options;
    ResultResponse::Ptr response = static_cast<ResultResponse::Ptr>(future->response());
    const Row row = response->first_row();
    for (size_t i = 0; i < row.values.size(); ++i) {
      String key = response->metadata()->get_column_definition(i).name.to_string();
      String value = row.values[i].decoder().as_string();
      options[key] = value;
    }

    return options;
  }

private:
  Config config_;
  Session session_;
  String client_id_;
};

TEST_F(StartupRequestUnitTest, Standard) {
  mockssandra::SimpleCluster cluster(simple_with_client_options());
  ASSERT_EQ(cluster.start_all(), 0);

  connect();
  Map<String, String> options = client_options();
  ASSERT_EQ(4u, options.size());

  ASSERT_EQ(client_id(), options["CLIENT_ID"]);
  ASSERT_EQ(CASS_DEFAULT_CQL_VERSION, options["CQL_VERSION"]);
  ASSERT_EQ(driver_name(), options["DRIVER_NAME"]);
  ASSERT_EQ(driver_version(), options["DRIVER_VERSION"]);
}

TEST_F(StartupRequestUnitTest, EnableNoCompact) {
  mockssandra::SimpleCluster cluster(simple_with_client_options());
  ASSERT_EQ(cluster.start_all(), 0);

  config().set_no_compact(true);
  connect();
  Map<String, String> options = client_options();
  ASSERT_EQ(5u, options.size());

  ASSERT_EQ(client_id(), options["CLIENT_ID"]);
  ASSERT_EQ(CASS_DEFAULT_CQL_VERSION, options["CQL_VERSION"]);
  ASSERT_EQ(driver_name(), options["DRIVER_NAME"]);
  ASSERT_EQ(driver_version(), options["DRIVER_VERSION"]);
  ASSERT_EQ("true", options["NO_COMPACT"]);
}

TEST_F(StartupRequestUnitTest, Application) {
  mockssandra::SimpleCluster cluster(simple_with_client_options());
  ASSERT_EQ(cluster.start_all(), 0);

  config().set_application_name(APPLICATION_NAME);
  config().set_application_version(APPLICATION_VERSION);
  connect();
  Map<String, String> options = client_options();
  ASSERT_EQ(6u, options.size());

  ASSERT_EQ(APPLICATION_NAME, options["APPLICATION_NAME"]);
  ASSERT_EQ(APPLICATION_VERSION, options["APPLICATION_VERSION"]);
  ASSERT_EQ(client_id(), options["CLIENT_ID"]);
  ASSERT_EQ(CASS_DEFAULT_CQL_VERSION, options["CQL_VERSION"]);
  ASSERT_EQ(driver_name(), options["DRIVER_NAME"]);
  ASSERT_EQ(driver_version(), options["DRIVER_VERSION"]);
}

TEST_F(StartupRequestUnitTest, SetClientId) {
  mockssandra::SimpleCluster cluster(simple_with_client_options());
  ASSERT_EQ(cluster.start_all(), 0);

  CassUuid generated_client_id = session().client_id();
  CassUuid assigned_client_id;
  ASSERT_EQ(CASS_OK,
            cass_uuid_from_string("03398c99-c635-4fad-b30a-3b2c49f785c2", &assigned_client_id));
  config().set_client_id(assigned_client_id);

  connect();
  CassUuid current_client_id = session().client_id();
  ASSERT_EQ(assigned_client_id, current_client_id);
  ASSERT_NE(generated_client_id, current_client_id);
  Map<String, String> options = client_options();
  ASSERT_EQ(4u, options.size());

  ASSERT_EQ("03398c99-c635-4fad-b30a-3b2c49f785c2", options["CLIENT_ID"]);
  ASSERT_EQ(CASS_DEFAULT_CQL_VERSION, options["CQL_VERSION"]);
  ASSERT_EQ(driver_name(), options["DRIVER_NAME"]);
  ASSERT_EQ(driver_version(), options["DRIVER_VERSION"]);
}
