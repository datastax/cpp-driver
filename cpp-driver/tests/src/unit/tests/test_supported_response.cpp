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

#include "loop_test.hpp"

#include "options_request.hpp"
#include "request_callback.hpp"
#include "supported_response.hpp"

using namespace datastax;
using namespace datastax::internal;
using namespace datastax::internal::core;

class SupportedResponseUnitTest : public LoopTest {
public:
  const mockssandra::RequestHandler* simple_cluster_with_options() {
    mockssandra::SimpleRequestHandlerBuilder builder;
    builder.on(mockssandra::OPCODE_OPTIONS).execute(new SupportedOptions());
    return builder.build();
  }

public:
  static void on_connect(Connector* connector, StringMultimap* supported_options) {
    ASSERT_TRUE(connector->is_ok());
    *supported_options = connector->supported_options();
  }

private:
  class SupportedOptions : public mockssandra::Action {
  public:
    virtual void on_run(mockssandra::Request* request) const {
      Vector<String> compression;
      Vector<String> cql_version;
      Vector<String> protocol_versions;
      compression.push_back("snappy");
      compression.push_back("lz4");
      cql_version.push_back("3.4.5");
      protocol_versions.push_back("3/v3");
      protocol_versions.push_back("4/v4");

      StringMultimap supported;
      supported["COMPRESSION"] = compression;
      supported["CQL_VERSION"] = cql_version;
      supported["PROTOCOL_VERSIONS"] = protocol_versions;

      String body;
      mockssandra::encode_string_map(supported, &body);
      request->write(mockssandra::OPCODE_SUPPORTED, body);
    }
  };
};

TEST_F(SupportedResponseUnitTest, Simple) {
  mockssandra::SimpleCluster cluster(simple_cluster_with_options());
  ASSERT_EQ(cluster.start_all(), 0);

  StringMultimap supported_options;
  ASSERT_EQ(0u, supported_options.size());
  Connector::Ptr connector(new Connector(Host::Ptr(new Host(Address("127.0.0.1", PORT))),
                                         PROTOCOL_VERSION,
                                         bind_callback(on_connect, &supported_options)));
  connector->connect(loop());
  uv_run(loop(), UV_RUN_DEFAULT);

  ASSERT_EQ(3u, supported_options.size());
  {
    Vector<String> compression = supported_options.find("COMPRESSION")->second;
    ASSERT_EQ(2u, compression.size());
    EXPECT_EQ("snappy", compression[0]);
    EXPECT_EQ("lz4", compression[1]);
  }
  {
    Vector<String> cql_version = supported_options.find("CQL_VERSION")->second;
    ASSERT_EQ(1u, cql_version.size());
    EXPECT_EQ("3.4.5", cql_version[0]);
  }
  {
    Vector<String> protocol_versions = supported_options.find("PROTOCOL_VERSIONS")->second;
    ASSERT_EQ(2u, protocol_versions.size());
    EXPECT_EQ("3/v3", protocol_versions[0]);
    EXPECT_EQ("4/v4", protocol_versions[1]);
  }

  { // Non-existent key
    EXPECT_EQ(supported_options.end(), supported_options.find("invalid"));
  }
}

TEST_F(SupportedResponseUnitTest, UppercaseKeysOnly) {
  class CaseInsensitiveSupportedOptions : public mockssandra::Action {
  public:
    virtual void on_run(mockssandra::Request* request) const {
      Vector<String> camel_key;
      camel_key.push_back("success");

      StringMultimap supported;
      supported["CamEL_KeY"] = camel_key;

      String body;
      mockssandra::encode_string_map(supported, &body);
      request->write(mockssandra::OPCODE_SUPPORTED, body);
    }
  };

  mockssandra::SimpleRequestHandlerBuilder builder;
  builder.on(mockssandra::OPCODE_OPTIONS).execute(new CaseInsensitiveSupportedOptions());
  mockssandra::SimpleCluster cluster(builder.build());
  ASSERT_EQ(cluster.start_all(), 0);

  StringMultimap supported_options;
  ASSERT_EQ(0u, supported_options.size());
  Connector::Ptr connector(new Connector(Host::Ptr(new Host(Address("127.0.0.1", PORT))),
                                         PROTOCOL_VERSION,
                                         bind_callback(on_connect, &supported_options)));
  connector->connect(loop());
  uv_run(loop(), UV_RUN_DEFAULT);

  ASSERT_EQ(1u, supported_options.size());
  { // Uppercase
    Vector<String> uppercase = supported_options.find("CAMEL_KEY")->second;
    ASSERT_EQ(1u, uppercase.size());
    EXPECT_EQ("success", uppercase[0]);
  }
  { // Exact key
    EXPECT_EQ(supported_options.end(), supported_options.find("CamEL_KeY"));
  }
}
