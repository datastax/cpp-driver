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

#include <gtest/gtest.h>

#include "mockssandra.hpp"
#include "query_request.hpp"
#include "session.hpp"

#define WAIT_FOR_TIME 5 * 1000 * 1000 // 5 seconds
#define KEYSPACE "datastax"
#define PORT 9042

TEST(SessionUnitTest, ExecuteQueryNotConnected) {
  cass::SharedRefPtr<cass::QueryRequest> request(cass::Memory::allocate<cass::QueryRequest>("blah", 0));

  cass::Session session;
  cass::Future::Ptr future = session.execute(request, NULL);
  ASSERT_EQ(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE, future->error()->code);
}

TEST(SessionUnitTest, InvalidKeyspace) {
  mockssandra::SimpleRequestHandlerBuilder builder;
  builder.on(mockssandra::OPCODE_QUERY)
    .system_local()
    .system_peers()
    .use_keyspace("blah")
    .empty_rows_result(1);
  mockssandra::SimpleCluster cluster(builder.build());
  cluster.start_all();

  cass::Config config;
  config.contact_points().push_back("127.0.0.1");
  cass::Future::Ptr connect_future(cass::Memory::allocate<cass::Future>(cass::Future::FUTURE_TYPE_SESSION));
  cass::Session session;

  cass::Logger::set_log_level(CASS_LOG_DISABLED);
  session.connect(config, "invalid", connect_future);
  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  ASSERT_EQ(CASS_ERROR_LIB_UNABLE_TO_SET_KEYSPACE, connect_future->error()->code);

  cass::Future::Ptr close_future(cass::Memory::allocate<cass::Future>(cass::Future::FUTURE_TYPE_SESSION));
  session.close(close_future);
  ASSERT_TRUE(close_future->wait_for(WAIT_FOR_TIME));
}

TEST(SessionUnitTest, InvalidDataCenter) {
  mockssandra::SimpleRequestHandlerBuilder builder;
  mockssandra::SimpleCluster cluster(builder.build());
  cluster.start_all();

  cass::Config config;
  config.contact_points().push_back("127.0.0.1");
  config.set_load_balancing_policy(cass::Memory::allocate<cass::DCAwarePolicy>(
                                     "invalid_data_center",
                                     0,
                                     false));
  cass::Future::Ptr connect_future(cass::Memory::allocate<cass::Future>(cass::Future::FUTURE_TYPE_SESSION));
  cass::Session session;

  cass::Logger::set_log_level(CASS_LOG_DISABLED);
  session.connect(config, "", connect_future);
  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  ASSERT_EQ(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE, connect_future->error()->code);

  cass::Future::Ptr close_future(cass::Memory::allocate<cass::Future>(cass::Future::FUTURE_TYPE_SESSION));
  session.close(close_future);
  ASSERT_TRUE(close_future->wait_for(WAIT_FOR_TIME));
}


TEST(SessionUnitTest, InvalidLocalAddress) {
  mockssandra::SimpleRequestHandlerBuilder builder;
  mockssandra::SimpleCluster cluster(builder.build());
  cluster.start_all();

  cass::Config config;
  config.set_local_address(Address("1.1.1.1", PORT)); // Invalid
  config.contact_points().push_back("127.0.0.1");
  config.set_load_balancing_policy(cass::Memory::allocate<cass::DCAwarePolicy>(
                                     "invalid_data_center",
                                     0,
                                     false));
  cass::Future::Ptr connect_future(cass::Memory::allocate<cass::Future>(cass::Future::FUTURE_TYPE_SESSION));
  cass::Session session;

  cass::Logger::set_log_level(CASS_LOG_DISABLED);
  session.connect(config, "", connect_future);
  ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME));
  ASSERT_EQ(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE, connect_future->error()->code);

  cass::Future::Ptr close_future(cass::Memory::allocate<cass::Future>(cass::Future::FUTURE_TYPE_SESSION));
  session.close(close_future);
  ASSERT_TRUE(close_future->wait_for(WAIT_FOR_TIME));
}
