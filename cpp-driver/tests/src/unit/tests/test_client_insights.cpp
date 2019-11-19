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

#include "client_insights.hpp"
#include "driver_info.hpp"
#include "get_time.hpp"
#include "query_request.hpp"
#include "result_iterator.hpp"
#include "session.hpp"

using namespace datastax;
using namespace datastax::internal;
using namespace datastax::internal::core;
using namespace datastax::internal::enterprise;

class ClientInsightsUnitTest : public Unit {
public:
  class RpcPayloadLatch : public RefCounted<RpcPayloadLatch> {
  public:
    typedef SharedRefPtr<RpcPayloadLatch> Ptr;

    RpcPayloadLatch(unsigned initial_payload_count = 1)
        : count_(initial_payload_count)
        , future_(new Future(Future::FUTURE_TYPE_GENERIC)) {
      uv_mutex_init(&mutex_);
    }

    ~RpcPayloadLatch() { uv_mutex_destroy(&mutex_); }

    bool wait_for(uint64_t timeout_us) { return future_->wait_for(timeout_us); }

    void reset(unsigned count) {
      ScopedMutex lock(&mutex_);
      count_ = count;
      future_.reset(new Future(Future::FUTURE_TYPE_GENERIC));
    }

    void add_payload(const String& payload) {
      ScopedMutex lock(&mutex_);
      payloads_.push_back(payload);
      if (--count_ == 0) future_->set();
    }

    const Vector<String>& payloads() {
      ScopedMutex lock(&mutex_);
      return payloads_;
    }

    const String& payload() { return payloads().front(); }

  private:
    unsigned count_;
    Future::Ptr future_;
    uv_mutex_t mutex_;
    Vector<String> payloads_;
  };

  class InsightsRpcQuery : public mockssandra::Action {
  public:
    InsightsRpcQuery(const RpcPayloadLatch::Ptr& latch)
        : latch_(latch) {}

    virtual void on_run(mockssandra::Request* request) const {
      String query;
      mockssandra::QueryParameters params;
      if (!request->decode_query(&query, &params)) {
        request->error(mockssandra::ERROR_PROTOCOL_ERROR, "Invalid query message");
      } else if (starts_with(query, "CALL InsightsRpc.reportInsight")) {
        size_t pos = query.find("('") + 2;                 // Skip starting single quote
        size_t len = (query.find_last_of("')") - 1) - pos; // Skip ending single quote
        latch_->add_payload(query.substr(pos, len));
      } else {
        run_next(request);
      }
    }

  private:
    RpcPayloadLatch::Ptr latch_;
  };

public:
  void TearDown() {
    ASSERT_TRUE(session_.close()->wait_for(WAIT_FOR_TIME));
    Unit::TearDown();
  }

  const mockssandra::RequestHandler* simple_dse_with_rpc_call(unsigned expected_payload_count = 1) {
    rpc_payload_latch_.reset(new RpcPayloadLatch(expected_payload_count));

    mockssandra::SimpleRequestHandlerBuilder builder;
    builder.on(mockssandra::OPCODE_QUERY)
        .system_local_dse()
        .system_peers_dse()
        .execute(
            new InsightsRpcQuery(rpc_payload_latch_)) // Allow RPC calls to be stored in cluster
        .is_query("wait")
        .then(mockssandra::Action::Builder().wait(2000).void_result()) // Allow queries to build up
        .void_result();

    return builder.build();
  }

  void connect(unsigned interval_secs = 1) {
    config_.contact_points().push_back(Address("127.0.0.1", 9042));
    config_.set_monitor_reporting_interval_secs(interval_secs);
    Future::Ptr connect_future(session_.connect(config_));
    ASSERT_TRUE(connect_future->wait_for(WAIT_FOR_TIME))
        << "Timed out waiting for session to connect";
    ASSERT_FALSE(connect_future->error()) << cass_error_desc(connect_future->error()->code) << ": "
                                          << connect_future->error()->message;
  }

  String startup_message(uint64_t wait_time_sec = WAIT_FOR_TIME) const {
    if (!rpc_payload_latch_->wait_for(wait_time_sec)) {
      return "";
    }
    return rpc_payload_latch_->payload();
  }

  String status_message(unsigned status_message_index = 1) const {
    if (!rpc_payload_latch_->wait_for(WAIT_FOR_TIME * status_message_index)) {
      return "";
    }
    return rpc_payload_latch_->payloads()[status_message_index];
  }

  void reset_latch(unsigned payload_count = 1) { rpc_payload_latch_->reset(payload_count); }

protected:
  Config config_;
  Session session_;
  RpcPayloadLatch::Ptr rpc_payload_latch_;
};

TEST_F(ClientInsightsUnitTest, StartupMetadata) {
  mockssandra::SimpleCluster cluster(simple_dse_with_rpc_call());
  ASSERT_EQ(cluster.start_all(), 0);
  connect();

  String message = startup_message();
  const uint64_t current_timestamp = get_time_since_epoch_ms();
  json::Document document;
  document.Parse(message.c_str());

  ASSERT_TRUE(document.IsObject());
  ASSERT_TRUE(document.HasMember("metadata"));
  const json::Value& metadata = document["metadata"];
  ASSERT_TRUE(metadata.IsObject());

  { // name
    ASSERT_TRUE(metadata.HasMember("name"));
    ASSERT_STREQ("driver.startup", metadata["name"].GetString());
  }
  { // insight mapping ID
    ASSERT_TRUE(metadata.HasMember("insightMappingId"));
    ASSERT_STREQ("v1", metadata["insightMappingId"].GetString());
  }
  { // insight type
    ASSERT_TRUE(metadata.HasMember("insightType"));
    ASSERT_STREQ("EVENT", metadata["insightType"].GetString());
  }
  { // timestamp
    ASSERT_TRUE(metadata.HasMember("timestamp"));
    ;
    ASSERT_NEAR(current_timestamp, metadata["timestamp"].GetUint64(),
                1000LL); // Allow for 1 second threshold
  }
  { // tags
    ASSERT_TRUE(metadata.HasMember("tags"));
    const json::Value& value = metadata["tags"];
    ASSERT_TRUE(value.IsObject());
    ASSERT_TRUE(value.HasMember("language"));
    ASSERT_STREQ("C/C++", value["language"].GetString());
  }
}

TEST_F(ClientInsightsUnitTest, StartupData) {
  mockssandra::SimpleCluster cluster(simple_dse_with_rpc_call(), 1,
                                     1); // Two DCs one will not be connected to
  ASSERT_EQ(cluster.start_all(), 0);

  config_.contact_points().push_back(Address("localhost", 9042)); // Used for hostname resolve
  String applicationName = "StartupData";
  String applicationVersion = "v1.0.0-test";
  CassConsistency consistency = CASS_CONSISTENCY_ALL;
  CassConsistency serial_consistency = CASS_CONSISTENCY_ONE;
  unsigned core_connections = 3;
  unsigned heartbeat_interval_secs = 5;
  unsigned periodic_status_interval = 7;
  unsigned delay_ms = 9;
  unsigned request_timeout_ms = 11;
  RetryPolicy::Ptr retry_policy(new FallthroughRetryPolicy());
  config_.set_application_name(applicationName);
  config_.set_application_version(applicationVersion);
  config_.set_consistency(consistency);
  config_.set_serial_consistency(serial_consistency);
  config_.set_core_connections_per_host(core_connections);
  config_.set_connection_heartbeat_interval_secs(heartbeat_interval_secs);
  config_.set_protocol_version(ProtocolVersion::lowest_supported());
  config_.set_constant_reconnect(delay_ms);
  config_.set_request_timeout(request_timeout_ms);
  config_.set_retry_policy(retry_policy.get());
  connect(periodic_status_interval);

  String message = startup_message();
  json::Document document;
  document.Parse(message.c_str());

  ASSERT_TRUE(document.IsObject());
  ASSERT_TRUE(document.HasMember("data"));
  const json::Value& data = document["data"];
  ASSERT_TRUE(data.IsObject());

  { // client ID
    ASSERT_TRUE(data.HasMember("clientId"));
    ASSERT_STREQ(to_string(session_.client_id()).c_str(), data["clientId"].GetString());
  }
  { // session ID
    ASSERT_TRUE(data.HasMember("sessionId"));
    ASSERT_STREQ(to_string(session_.session_id()).c_str(), data["sessionId"].GetString());
  }
  { // application name
    ASSERT_TRUE(data.HasMember("applicationName"));
    ASSERT_STREQ(applicationName.c_str(), data["applicationName"].GetString());
  }
  { // application name was generated
    ASSERT_TRUE(data.HasMember("applicationNameWasGenerated"));
    ASSERT_FALSE(data["applicationNameWasGenerated"].GetBool()); // Set with configuration
  }
  { // application version
    ASSERT_TRUE(data.HasMember("applicationVersion"));
    ASSERT_STREQ(applicationVersion.c_str(), data["applicationVersion"].GetString());
  }
  { // driver name
    ASSERT_TRUE(data.HasMember("driverName"));
    ASSERT_STREQ(driver_name(), data["driverName"].GetString());
  }
  { // driver version
    ASSERT_TRUE(data.HasMember("driverVersion"));
    ASSERT_STREQ(driver_version(), data["driverVersion"].GetString());
  }
  { // contact points
    ASSERT_TRUE(data.HasMember("contactPoints"));
    const json::Value& value = data["contactPoints"];
    ASSERT_TRUE(value.IsObject());
    ASSERT_EQ(2u, value.MemberCount());
    ASSERT_TRUE(value.HasMember("127.0.0.1"));
    const json::Value& local_ipv4_1 = value["127.0.0.1"];
    ASSERT_TRUE(local_ipv4_1.IsArray());
    ASSERT_EQ(1u, local_ipv4_1.Size());
    OStringStream ipv4_with_port;
    ipv4_with_port << "127.0.0.1:" << config_.port();
    OStringStream ipv6_with_port;
    ipv6_with_port << "[::1]:" << config_.port();
    ASSERT_EQ(ipv4_with_port.str(), local_ipv4_1.GetArray()[0].GetString());
    ASSERT_TRUE(value.HasMember("localhost"));
    const json::Value& local_hostname = value["localhost"];
    ASSERT_GE(local_hostname.Size(), 1u); // More than one address could be resolved
    String resolved_local_hostname = local_hostname.GetArray()[0].GetString();
    ASSERT_TRUE(ipv6_with_port.str() == resolved_local_hostname ||
                ipv4_with_port.str() == resolved_local_hostname);
  }
  { // data centers
    ASSERT_TRUE(data.HasMember("dataCenters"));
    const json::Value& value = data["dataCenters"];
    ASSERT_TRUE(value.IsArray());
    ASSERT_EQ(1u, value.Size()); // Should only connect to 1 DC based on LBP
    ASSERT_STREQ("dc1", value.GetArray()[0].GetString());
  }
  { // initial control connection
    ASSERT_TRUE(data.HasMember("initialControlConnection"));
    OStringStream ipv4_with_port;
    ipv4_with_port << "127.0.0.1:" << config_.port();
    ASSERT_EQ(ipv4_with_port.str(), data["initialControlConnection"].GetString());
  }
  { // protocol version
    ASSERT_TRUE(data.HasMember("protocolVersion"));
    ASSERT_EQ(ProtocolVersion::lowest_supported().value(), data["protocolVersion"].GetInt());
  }
  { // local address
    ASSERT_TRUE(data.HasMember("localAddress"));
    ASSERT_STREQ("127.0.0.1", data["localAddress"].GetString());
  }
  { // hostname
    ASSERT_TRUE(data.HasMember("hostName"));
    const json::Value& value = data["hostName"];
    // No simple way to validate hostname on different machines
    ASSERT_TRUE(value.IsString());
    ASSERT_GT(value.GetStringLength(), 0u);
  }
  { // execution profiles
    ASSERT_TRUE(data.HasMember("executionProfiles"));
    const json::Value& value = data["executionProfiles"];
    ASSERT_TRUE(value.IsObject());
    ASSERT_EQ(1u, value.MemberCount());
    ASSERT_TRUE(value.HasMember("default"));
    const json::Value& default_profile = value["default"];
    ASSERT_TRUE(default_profile.IsObject());
    ASSERT_EQ(5u, default_profile.MemberCount());
    ASSERT_TRUE(default_profile.HasMember("requestTimeoutMs"));
    ASSERT_EQ(request_timeout_ms, default_profile["requestTimeoutMs"].GetUint());
    ASSERT_TRUE(default_profile.HasMember("consistency"));
    ASSERT_STREQ(cass_consistency_string(consistency), default_profile["consistency"].GetString());
    ASSERT_TRUE(default_profile.HasMember("serialConsistency"));
    ASSERT_STREQ(cass_consistency_string(serial_consistency),
                 default_profile["serialConsistency"].GetString());
    ASSERT_TRUE(default_profile.HasMember("retryPolicy"));
    ASSERT_STREQ("FallthroughRetryPolicy", default_profile["retryPolicy"].GetString());
    ASSERT_TRUE(default_profile.HasMember("loadBalancing"));
    const json::Value& load_balancing = default_profile["loadBalancing"];
    ASSERT_TRUE(load_balancing.IsObject());
    ASSERT_EQ(2u, load_balancing.MemberCount());
    ASSERT_TRUE(load_balancing.HasMember("type"));
    ASSERT_STREQ("DCAwarePolicy", load_balancing["type"].GetString());
    ASSERT_TRUE(load_balancing.HasMember("options"));
    const json::Value& options = load_balancing["options"];
    ASSERT_TRUE(options.IsObject());
    ASSERT_EQ(4u, options.MemberCount());
    ASSERT_TRUE(options.HasMember("localDc"));
    ASSERT_TRUE(options["localDc"].IsNull());
    ASSERT_TRUE(options.HasMember("usedHostsPerRemoteDc"));
    ASSERT_EQ(0u, options["usedHostsPerRemoteDc"].GetUint());
    ASSERT_TRUE(options.HasMember("allowRemoteDcsForLocalCl"));
    ASSERT_FALSE(options["allowRemoteDcsForLocalCl"].GetBool());
    ASSERT_TRUE(options.HasMember("tokenAwareRouting"));
    const json::Value& token_aware_routing = options["tokenAwareRouting"];
    ASSERT_TRUE(token_aware_routing.IsObject());
    ASSERT_EQ(1u, token_aware_routing.MemberCount());
    ASSERT_TRUE(token_aware_routing.HasMember("shuffleReplicas"));
    ASSERT_TRUE(token_aware_routing["shuffleReplicas"].GetBool());
  }
  { // pool size by host distance
    ASSERT_TRUE(data.HasMember("poolSizeByHostDistance"));
    const json::Value& value = data["poolSizeByHostDistance"];
    ASSERT_TRUE(value.IsObject());
    ASSERT_EQ(2u, value.MemberCount());
    ASSERT_TRUE(value.HasMember("local"));
    ASSERT_EQ(core_connections, value["local"].GetUint()); // Only one host connected
    ASSERT_TRUE(value.HasMember("remote"));
    ASSERT_EQ(0u, value["remote"].GetUint());
  }
  { // heartbeat interval
    ASSERT_TRUE(data.HasMember("heartbeatInterval"));
    ASSERT_EQ(heartbeat_interval_secs * 1000u, data["heartbeatInterval"].GetUint64());
  }
  { // compression
    ASSERT_TRUE(data.HasMember("compression"));
    ASSERT_STREQ("NONE", data["compression"].GetString()); // TODO: Update once compression is added
  }
  { // reconnection policy
    ASSERT_TRUE(data.HasMember("reconnectionPolicy"));
    const json::Value& value = data["reconnectionPolicy"];
    ASSERT_TRUE(value.IsObject());
    ASSERT_EQ(2u, value.MemberCount());
    ASSERT_TRUE(value.HasMember("type"));
    ASSERT_STREQ("ConstantReconnectionPolicy", value["type"].GetString());
    ASSERT_TRUE(value.HasMember("options"));
    const json::Value& options = value["options"];
    ASSERT_TRUE(options.IsObject());
    ASSERT_EQ(1u, options.MemberCount());
    ASSERT_TRUE(options.HasMember("delayMs"));
    ASSERT_EQ(delay_ms, options["delayMs"].GetUint());
  }
  { // SSL
    ASSERT_TRUE(data.HasMember("ssl"));
    const json::Value& value = data["ssl"];
    ASSERT_TRUE(value.IsObject());
    ASSERT_EQ(2u, value.MemberCount());
    ASSERT_TRUE(value.HasMember("enabled"));
    ASSERT_FALSE(value["enabled"].GetBool());
    ASSERT_TRUE(value.HasMember("certValidation"));
    ASSERT_FALSE(value["certValidation"].GetBool());
  }
  { // other options
    ASSERT_TRUE(data.HasMember("otherOptions"));
    const json::Value& value = data["otherOptions"];
    ASSERT_TRUE(value.IsObject());
    ASSERT_EQ(1u, value.MemberCount());
    ASSERT_TRUE(value.HasMember("configuration"));
    const json::Value& configuration = value["configuration"];
    ASSERT_TRUE(configuration.IsObject());
    ASSERT_EQ(26u, configuration.MemberCount());
    ASSERT_TRUE(configuration.HasMember("protocolVersion"));
    ASSERT_EQ(config_.protocol_version().value(), configuration["protocolVersion"].GetInt());
    ASSERT_TRUE(configuration.HasMember("useBetaProtocol"));
    ASSERT_EQ(config_.use_beta_protocol_version(), configuration["useBetaProtocol"].GetBool());
    ASSERT_TRUE(configuration.HasMember("threadCountIo"));
    ASSERT_EQ(config_.thread_count_io(), configuration["threadCountIo"].GetUint());
    ASSERT_TRUE(configuration.HasMember("queueSizeIo"));
    ASSERT_EQ(config_.queue_size_io(), configuration["queueSizeIo"].GetUint());
    ASSERT_TRUE(configuration.HasMember("coreConnectionsPerHost"));
    ASSERT_EQ(config_.core_connections_per_host(),
              configuration["coreConnectionsPerHost"].GetUint());
    ASSERT_TRUE(configuration.HasMember("connectTimeoutMs"));
    ASSERT_EQ(config_.connect_timeout_ms(), configuration["connectTimeoutMs"].GetUint());
    ASSERT_TRUE(configuration.HasMember("resolveTimeoutMs"));
    ASSERT_EQ(config_.resolve_timeout_ms(), configuration["resolveTimeoutMs"].GetUint());
    ASSERT_TRUE(configuration.HasMember("maxSchemaWaitTimeMs"));
    ASSERT_EQ(config_.max_schema_wait_time_ms(), configuration["maxSchemaWaitTimeMs"].GetUint());
    ASSERT_TRUE(configuration.HasMember("maxTracingWaitTimeMs"));
    ASSERT_EQ(config_.max_tracing_wait_time_ms(), configuration["maxTracingWaitTimeMs"].GetUint());
    ASSERT_TRUE(configuration.HasMember("tracingConsistency"));
    ASSERT_STREQ(cass_consistency_string(config_.tracing_consistency()),
                 configuration["tracingConsistency"].GetString());
    ASSERT_TRUE(configuration.HasMember("coalesceDelayUs"));
    ASSERT_EQ(config_.coalesce_delay_us(), configuration["coalesceDelayUs"].GetUint64());
    ASSERT_TRUE(configuration.HasMember("newRequestRatio"));
    ASSERT_EQ(config_.new_request_ratio(), configuration["newRequestRatio"].GetInt());
    ASSERT_TRUE(configuration.HasMember("logLevel"));
    ASSERT_STREQ(cass_log_level_string(config_.log_level()), configuration["logLevel"].GetString());
    ASSERT_TRUE(configuration.HasMember("tcpNodelayEnable"));
    ASSERT_EQ(config_.tcp_nodelay_enable(), configuration["tcpNodelayEnable"].GetBool());
    ASSERT_TRUE(configuration.HasMember("tcpKeepaliveEnable"));
    ASSERT_EQ(config_.tcp_keepalive_enable(), configuration["tcpKeepaliveEnable"].GetBool());
    ASSERT_TRUE(configuration.HasMember("tcpKeepaliveDelaySecs"));
    ASSERT_EQ(config_.tcp_keepalive_delay_secs(), configuration["tcpKeepaliveDelaySecs"].GetUint());
    ASSERT_TRUE(configuration.HasMember("connectionIdleTimeoutSecs"));
    ASSERT_EQ(config_.connection_idle_timeout_secs(),
              configuration["connectionIdleTimeoutSecs"].GetUint());
    ASSERT_TRUE(configuration.HasMember("useSchema"));
    ASSERT_EQ(config_.use_schema(), configuration["useSchema"].GetBool());
    ASSERT_TRUE(configuration.HasMember("useHostnameResolution"));
    ASSERT_EQ(config_.use_hostname_resolution(), configuration["useHostnameResolution"].GetBool());
    ASSERT_TRUE(configuration.HasMember("useRandomizedContactPoints"));
    ASSERT_EQ(config_.use_randomized_contact_points(),
              configuration["useRandomizedContactPoints"].GetBool());
    ASSERT_TRUE(configuration.HasMember("maxReusableWriteObjects"));
    ASSERT_EQ(config_.max_reusable_write_objects(),
              configuration["maxReusableWriteObjects"].GetUint());
    ASSERT_TRUE(configuration.HasMember("prepareOnAllHosts"));
    ASSERT_EQ(config_.prepare_on_all_hosts(), configuration["prepareOnAllHosts"].GetBool());
    ASSERT_TRUE(configuration.HasMember("prepareOnUpOrAddHost"));
    ASSERT_EQ(config_.prepare_on_up_or_add_host(), configuration["prepareOnUpOrAddHost"].GetBool());
    ASSERT_TRUE(configuration.HasMember("noCompact"));
    ASSERT_EQ(config_.no_compact(), configuration["noCompact"].GetBool());
    ASSERT_TRUE(configuration.HasMember("cloudSecureConnectBundleLoaded"));
    ASSERT_EQ(config_.cloud_secure_connection_config().is_loaded(),
              configuration["cloudSecureConnectBundleLoaded"].GetBool());
    ASSERT_TRUE(configuration.HasMember("clusterMetadataResolver"));
    ASSERT_STREQ(config_.cluster_metadata_resolver_factory()->name(),
                 configuration["clusterMetadataResolver"].GetString());
  }
  { // platform info
    ASSERT_TRUE(data.HasMember("platformInfo"));
    const json::Value& value = data["platformInfo"];
    // No simple way to validate platform information on different platforms
    ASSERT_TRUE(value.IsObject());
    ASSERT_EQ(3u, value.MemberCount());
    ASSERT_TRUE(value.HasMember("os"));
    const json::Value& os = value["os"];
    ASSERT_TRUE(os.IsObject());
    ASSERT_EQ(3u, os.MemberCount());
    ASSERT_TRUE(os.HasMember("name"));
    ASSERT_TRUE(os["name"].IsString());
    ASSERT_GT(os["name"].GetStringLength(), 0u);
    ASSERT_TRUE(os.HasMember("version"));
    ASSERT_TRUE(os["version"].IsString());
    ASSERT_GT(os["version"].GetStringLength(), 0u);
    ASSERT_TRUE(os.HasMember("arch"));
    ASSERT_TRUE(os["arch"].IsString());
    ASSERT_GT(os["arch"].GetStringLength(), 0u);
    ASSERT_TRUE(value.HasMember("cpus"));
    const json::Value& cpus = value["cpus"];
    ASSERT_TRUE(cpus.IsObject());
    ASSERT_EQ(2u, cpus.MemberCount());
    ASSERT_TRUE(cpus.HasMember("length"));
    ASSERT_TRUE(cpus["length"].IsInt());
    ASSERT_TRUE(cpus.HasMember("model"));
    ASSERT_TRUE(cpus["model"].IsString());
    ASSERT_GT(cpus["model"].GetStringLength(), 0u);
    ASSERT_TRUE(value.HasMember("runtime"));
    const json::Value& runtime = value["runtime"];
    ASSERT_TRUE(runtime.IsObject());
    ASSERT_EQ(3u, runtime.MemberCount());
    // NOTE: No simple way to validate compiler with different compilers
    ASSERT_TRUE(runtime.HasMember("uv"));
    ASSERT_TRUE(runtime["uv"].IsString());
    ASSERT_GT(runtime["uv"].GetStringLength(), 0u);
    ASSERT_TRUE(runtime.HasMember("openssl"));
    ASSERT_TRUE(runtime["openssl"].IsString());
    ASSERT_GT(runtime["openssl"].GetStringLength(), 0u);
  }
  ASSERT_FALSE(data.HasMember(
      "configAntiPatterns")); // Config anti patterns should not exist with current config
  {                           // periodic status interval
    ASSERT_TRUE(data.HasMember("periodicStatusInterval"));
    ASSERT_EQ(periodic_status_interval, data["periodicStatusInterval"].GetUint());
  }
}

TEST_F(ClientInsightsUnitTest, StartupDataMultipleDcs) {
  mockssandra::SimpleCluster cluster(simple_dse_with_rpc_call(), 1, 1);
  ASSERT_EQ(cluster.start_all(), 0);

  LoadBalancingPolicy::Ptr load_balancing_policy(new DCAwarePolicy("dc1", 1, false));
  config_.set_load_balancing_policy(load_balancing_policy.get());
  connect();

  String message = startup_message();
  json::Document document;
  document.Parse(message.c_str());

  const json::Value& data = document["data"];
  ASSERT_EQ(2u, data["dataCenters"].Size());
  ASSERT_STREQ("dc1", data["dataCenters"].GetArray()[0].GetString());
  ASSERT_STREQ("dc2", data["dataCenters"].GetArray()[1].GetString());
}

TEST_F(ClientInsightsUnitTest, StartupDataProtocolVersion) {
  mockssandra::SimpleCluster cluster(simple_dse_with_rpc_call());
  ASSERT_EQ(cluster.start_all(), 0);

  ProtocolVersion configured_protocol_version(CASS_PROTOCOL_VERSION_DSEV2);
  config_.set_protocol_version(
      configured_protocol_version); // Mockssandra does not currently support DSE protocols
  connect();

  String message = startup_message();
  json::Document document;
  document.Parse(message.c_str());

  // Configured and connected protocol versions should be different
  const json::Value& data = document["data"];
  int data_protocol_version = data["protocolVersion"].GetInt();
  int other_options_protocol_version =
      data["otherOptions"]["configuration"]["protocolVersion"].GetInt();
  ASSERT_LT(data_protocol_version, configured_protocol_version.value());
  ASSERT_EQ(other_options_protocol_version, configured_protocol_version.value());
}

TEST_F(ClientInsightsUnitTest, StartupDataMultipleExecutionProfiles) {
  mockssandra::SimpleCluster cluster(simple_dse_with_rpc_call());
  ASSERT_EQ(cluster.start_all(), 0);

  DCAwarePolicy::Ptr dc_aware(new DCAwarePolicy("dc1", 1, true));
  RoundRobinPolicy::Ptr round_robin(new RoundRobinPolicy());
  LatencyAwarePolicy::Settings latency_aware_settings;
  RetryPolicy::Ptr profile_retry_policy(new DowngradingConsistencyRetryPolicy());
  latency_aware_settings.exclusion_threshold = 0.1;
  latency_aware_settings.scale_ns = 1;
  latency_aware_settings.retry_period_ns = 3;
  latency_aware_settings.update_rate_ms = 5;
  latency_aware_settings.min_measured = 7;
  ExecutionProfile quorum_profile;
  quorum_profile.set_consistency(CASS_CONSISTENCY_QUORUM);
  quorum_profile.set_request_timeout(300000);
  ExecutionProfile round_robin_profile;
  round_robin_profile.set_load_balancing_policy(round_robin.get());
  round_robin_profile.set_token_aware_routing(false);
  round_robin_profile.set_latency_aware_routing(true);
  round_robin_profile.set_latency_aware_routing_settings(latency_aware_settings);
  round_robin_profile.set_retry_policy(profile_retry_policy.get());
  config_.set_load_balancing_policy(dc_aware.get());
  config_.set_token_aware_routing_shuffle_replicas(false);
  config_.set_execution_profile("quorum", &quorum_profile);
  config_.set_execution_profile("round_robin", &round_robin_profile);
  connect();

  String message = startup_message();
  json::Document document;
  document.Parse(message.c_str());

  const json::Value& data = document["data"];
  const json::Value& execution_profiles = data["executionProfiles"];
  ASSERT_EQ(3u, execution_profiles.MemberCount());
  ASSERT_TRUE(execution_profiles.HasMember("default"));
  ASSERT_TRUE(execution_profiles.HasMember("round_robin"));
  { // default profile
    const json::Value& execution_profile = execution_profiles["default"];
    ASSERT_EQ(config_.request_timeout(), execution_profile["requestTimeoutMs"].GetUint());
    ASSERT_STREQ(cass_consistency_string(CASS_DEFAULT_CONSISTENCY),
                 execution_profile["consistency"].GetString());
    ASSERT_STREQ(cass_consistency_string(config_.serial_consistency()),
                 execution_profile["serialConsistency"].GetString());
    ASSERT_STREQ("DefaultRetryPolicy", execution_profile["retryPolicy"].GetString());
    const json::Value& load_balancing = execution_profile["loadBalancing"];
    ASSERT_STREQ("DCAwarePolicy", load_balancing["type"].GetString());
    const json::Value& options = load_balancing["options"];
    ASSERT_STREQ("dc1", options["localDc"].GetString());
    ASSERT_EQ(1u, options["usedHostsPerRemoteDc"].GetUint());
    ASSERT_FALSE(options["allowRemoteDcsForLocalCl"].GetBool());
    ASSERT_TRUE(options.HasMember("tokenAwareRouting"));
    ASSERT_FALSE(options["tokenAwareRouting"]["shuffleReplicas"].GetBool());
  }
  { // quorum profile
    const json::Value& execution_profile = execution_profiles["quorum"];
    ASSERT_EQ(quorum_profile.request_timeout_ms(), execution_profile["requestTimeoutMs"].GetUint());
    ASSERT_STREQ(cass_consistency_string(quorum_profile.consistency()),
                 execution_profile["consistency"].GetString());
    ASSERT_FALSE(execution_profiles.HasMember("serialConsistency"));
    ASSERT_FALSE(execution_profiles.HasMember("retryPolicy"));
    ASSERT_FALSE(execution_profiles.HasMember("loadBalancing"));
  }
  { // round robin profile
    const json::Value& execution_profile = execution_profiles["round_robin"];
    ASSERT_FALSE(execution_profiles.HasMember("requestTimeoutMs"));
    ASSERT_FALSE(execution_profiles.HasMember("consistency"));
    ASSERT_FALSE(execution_profiles.HasMember("serialConsistency"));
    ASSERT_FALSE(execution_profiles.HasMember("retryPolicy"));
    const json::Value& load_balancing = execution_profile["loadBalancing"];
    ASSERT_STREQ("RoundRobinPolicy", load_balancing["type"].GetString());
    const json::Value& options = load_balancing["options"];
    const json::Value& latency_aware_routing = options["latencyAwareRouting"];
    ASSERT_EQ(latency_aware_settings.exclusion_threshold,
              latency_aware_routing["exclusionThreshold"].GetDouble());
    ASSERT_EQ(latency_aware_settings.scale_ns, latency_aware_routing["scaleNs"].GetUint64());
    ASSERT_EQ(latency_aware_settings.retry_period_ns,
              latency_aware_routing["retryPeriodNs"].GetUint64());
    ASSERT_EQ(latency_aware_settings.update_rate_ms,
              latency_aware_routing["updateRateMs"].GetUint64());
    ASSERT_EQ(latency_aware_settings.min_measured,
              latency_aware_routing["minMeasured"].GetUint64());
  }
}

TEST_F(ClientInsightsUnitTest, StartupDataExponentialReconnect) {
  mockssandra::SimpleCluster cluster(simple_dse_with_rpc_call());
  ASSERT_EQ(cluster.start_all(), 0);

  unsigned base_delay_ms = 1234u;
  unsigned max_delay_ms = 123456u;
  config_.set_exponential_reconnect(base_delay_ms, max_delay_ms);
  connect();

  String message = startup_message();
  json::Document document;
  document.Parse(message.c_str());

  const json::Value& data = document["data"];
  const json::Value& reconnection_policy = data["reconnectionPolicy"];
  ASSERT_TRUE(reconnection_policy.IsObject());
  ASSERT_EQ(2u, reconnection_policy.MemberCount());
  ASSERT_TRUE(reconnection_policy.HasMember("type"));
  ASSERT_STREQ("ExponentialReconnectionPolicy", reconnection_policy["type"].GetString());
  ASSERT_TRUE(reconnection_policy.HasMember("options"));
  const json::Value& options = reconnection_policy["options"];
  ASSERT_TRUE(options.IsObject());
  ASSERT_EQ(2u, options.MemberCount());
  ASSERT_TRUE(options.HasMember("baseDelayMs"));
  ASSERT_EQ(base_delay_ms, options["baseDelayMs"].GetUint());
  ASSERT_TRUE(options.HasMember("maxDelayMs"));
  ASSERT_EQ(max_delay_ms, options["maxDelayMs"].GetUint());
}

TEST_F(ClientInsightsUnitTest, StartupDataSsl) {
  mockssandra::SimpleCluster cluster(simple_dse_with_rpc_call());
  SslContext::Ptr ssl_context = use_ssl(&cluster).socket_settings.ssl_context;
  ASSERT_EQ(cluster.start_all(), 0);

  config_.set_ssl_context(ssl_context.get());
  connect();

  String message = startup_message();
  json::Document document;
  document.Parse(message.c_str());

  const json::Value& data = document["data"];
  ASSERT_TRUE(data["ssl"]["enabled"].GetBool());
  ASSERT_TRUE(data["ssl"]["certValidation"].GetBool());
}

TEST_F(ClientInsightsUnitTest, StartupDataSslWithoutCertValidation) {
  mockssandra::SimpleCluster cluster(simple_dse_with_rpc_call());
  SslContext::Ptr ssl_context = use_ssl(&cluster).socket_settings.ssl_context;
  ssl_context->set_verify_flags(SSL_VERIFY_NONE);
  ASSERT_EQ(cluster.start_all(), 0);

  config_.set_ssl_context(ssl_context.get());
  connect();

  String message = startup_message();
  json::Document document;
  document.Parse(message.c_str());

  const json::Value& data = document["data"];
  ASSERT_TRUE(data["ssl"]["enabled"].GetBool());
  ASSERT_FALSE(data["ssl"]["certValidation"].GetBool());
}

TEST_F(ClientInsightsUnitTest, StartupDataConfigAntiPatternWithoutSsl) {
  mockssandra::SimpleCluster cluster(simple_dse_with_rpc_call());
  ASSERT_EQ(cluster.start_all(), 0);

  LoadBalancingPolicy::Ptr dc_aware(new DCAwarePolicy("dc1", 1,
                                                      false)); // useRemoteHosts
  RetryPolicy::Ptr retry_policy(new DowngradingConsistencyRetryPolicy());
  config_.set_credentials("cassandra", "cassandra"); // plainTextAuthWithoutSsl
  connect();

  String message = startup_message();
  json::Document document;
  document.Parse(message.c_str());

  const json::Value& data = document["data"];
  ASSERT_TRUE(data.HasMember("configAntiPatterns"));
  const json::Value& config_anti_patterns = data["configAntiPatterns"];
  ASSERT_TRUE(config_anti_patterns.IsObject());
  ASSERT_EQ(1u, config_anti_patterns.MemberCount());
  ASSERT_TRUE(config_anti_patterns.HasMember("plainTextAuthWithoutSsl"));
  ASSERT_TRUE(config_anti_patterns["plainTextAuthWithoutSsl"].IsString());
  ASSERT_GT(config_anti_patterns["plainTextAuthWithoutSsl"].GetStringLength(), 0u);
}

TEST_F(ClientInsightsUnitTest, StartupDataConfigAntiPatternsWithSsl) {
  mockssandra::SimpleCluster cluster(simple_dse_with_rpc_call(), 1, 1);
  SslContext::Ptr ssl_context = use_ssl(&cluster).socket_settings.ssl_context;
  ssl_context->set_verify_flags(SSL_VERIFY_NONE); // sslWithoutCertValidation
  ASSERT_EQ(cluster.start_all(), 0);

  LoadBalancingPolicy::Ptr dc_aware(new DCAwarePolicy("dc1", 1,
                                                      false)); // useRemoteHosts
  RetryPolicy::Ptr retry_policy(new DowngradingConsistencyRetryPolicy());
  config_.set_load_balancing_policy(dc_aware.get());
  config_.set_retry_policy(retry_policy.get()); // downgradingConsistency
  config_.set_ssl_context(ssl_context.get());
  config_.contact_points().push_back(Address("127.0.0.2", 9042)); // contactPointsMultipleDCs
  connect();

  String message = startup_message();
  json::Document document;
  document.Parse(message.c_str());

  const json::Value& data = document["data"];
  ASSERT_TRUE(data.HasMember("configAntiPatterns"));
  const json::Value& config_anti_patterns = data["configAntiPatterns"];
  ASSERT_TRUE(config_anti_patterns.IsObject());
  ASSERT_EQ(4u, config_anti_patterns.MemberCount());
  ASSERT_TRUE(config_anti_patterns.HasMember("contactPointsMultipleDCs"));
  ASSERT_TRUE(config_anti_patterns["contactPointsMultipleDCs"].IsString());
  ASSERT_GT(config_anti_patterns["contactPointsMultipleDCs"].GetStringLength(), 0u);
  ASSERT_TRUE(config_anti_patterns.HasMember("useRemoteHosts"));
  ASSERT_TRUE(config_anti_patterns["useRemoteHosts"].IsString());
  ASSERT_GT(config_anti_patterns["useRemoteHosts"].GetStringLength(), 0u);
  ASSERT_TRUE(config_anti_patterns.HasMember("downgradingConsistency"));
  ASSERT_TRUE(config_anti_patterns["downgradingConsistency"].IsString());
  ASSERT_GT(config_anti_patterns["downgradingConsistency"].GetStringLength(), 0u);
  ASSERT_TRUE(config_anti_patterns.HasMember("sslWithoutCertValidation"));
  ASSERT_TRUE(config_anti_patterns["sslWithoutCertValidation"].IsString());
  ASSERT_GT(config_anti_patterns["sslWithoutCertValidation"].GetStringLength(), 0u);
}

TEST_F(ClientInsightsUnitTest, StatusMetadata) {
  mockssandra::SimpleCluster cluster(simple_dse_with_rpc_call(2));
  ASSERT_EQ(cluster.start_all(), 0);
  connect();

  String message = status_message();
  json::Document document;
  document.Parse(message.c_str());

  ASSERT_TRUE(document.IsObject());
  ASSERT_TRUE(document.HasMember("metadata"));
  const json::Value& metadata = document["metadata"];
  ASSERT_TRUE(metadata.IsObject());

  { // name
    ASSERT_TRUE(metadata.HasMember("name"));
    ASSERT_STREQ("driver.status", metadata["name"].GetString());
  }
}

TEST_F(ClientInsightsUnitTest, StatusData) {
  mockssandra::SimpleCluster cluster(simple_dse_with_rpc_call(2), 2);
  ASSERT_EQ(cluster.start_all(), 0);

  config_.contact_points().push_back(Address("localhost", 9042));
  config_.set_core_connections_per_host(2);
  config_.set_thread_count_io(5);
  config_.set_use_randomized_contact_points(false);
  connect();

  String message = status_message();
  json::Document document;
  document.Parse(message.c_str());

  ASSERT_TRUE(document.IsObject());
  ASSERT_TRUE(document.HasMember("data"));
  const json::Value& data = document["data"];
  ASSERT_TRUE(data.IsObject());

  { // client ID
    ASSERT_TRUE(data.HasMember("clientId"));
    ASSERT_STREQ(to_string(session_.client_id()).c_str(), data["clientId"].GetString());
  }
  { // session ID
    ASSERT_TRUE(data.HasMember("sessionId"));
    ASSERT_STREQ(to_string(session_.session_id()).c_str(), data["sessionId"].GetString());
  }
  { // control connection
    ASSERT_TRUE(data.HasMember("controlConnection"));
    OStringStream ip_with_port;
    ip_with_port << "127.0.0.1:" << config_.port();
    ASSERT_EQ(ip_with_port.str(), data["controlConnection"].GetString());
  }
  { // connected nodes
    ASSERT_TRUE(data.HasMember("conntectedNodes"));
    const json::Value& value = data["conntectedNodes"];
    ASSERT_TRUE(value.IsObject());
    ASSERT_EQ(2u, value.MemberCount());
    for (unsigned i = 1; i <= value.MemberCount(); ++i) {
      OStringStream ip_with_port;
      ip_with_port << "127.0.0." << i << ":" << config_.port();
      ASSERT_TRUE(value.HasMember(ip_with_port.str().c_str()));
      const json::Value& node = value[ip_with_port.str().c_str()];
      ASSERT_TRUE(node.IsObject());
      ASSERT_EQ(2u, node.MemberCount());
      ASSERT_TRUE(node.HasMember("connections"));
      int expected_connections(i == 1 ? 11 : 10); // Handle control connection
      ASSERT_EQ(expected_connections, node["connections"].GetInt());
      ASSERT_TRUE(node.HasMember("inFlightQueries"));
      ASSERT_NEAR(0, node["inFlightQueries"].GetInt(),
                  5); // Relaxed memory ordering for inflight request count
    }
  }
}

TEST_F(ClientInsightsUnitTest, StatusDataConnectedNodesRemovedNode) {
  mockssandra::SimpleCluster cluster(simple_dse_with_rpc_call(2), 3);
  ASSERT_EQ(cluster.start_all(), 0);

  config_.set_use_randomized_contact_points(false);
  connect();

  String message = status_message();
  reset_latch();
  json::Document document;
  document.Parse(message.c_str());

  { // connected nodes (all nodes should be connected)
    const json::Value& value = document["data"]["conntectedNodes"];
    ASSERT_EQ(3u, value.MemberCount());
    for (unsigned i = 1; i <= value.MemberCount(); ++i) {
      OStringStream ip_with_port;
      ip_with_port << "127.0.0." << i << ":" << config_.port();
      int expected_connections(i == 1 ? 2 : 1); // Handle control connection
      ASSERT_EQ(expected_connections, value[ip_with_port.str().c_str()]["connections"].GetInt());
    }
  }

  cluster.remove(2);
  message = status_message(2);
  document.Parse(message.c_str());

  { // connected nodes (node 2 should be missing)
    const json::Value& value = document["data"]["conntectedNodes"];
    ASSERT_EQ(2u, value.MemberCount());
    {
      OStringStream ip_with_port;
      ip_with_port << "127.0.0.1:" << config_.port();
      const json::Value& node = value[ip_with_port.str().c_str()]["connections"];
      EXPECT_EQ(2, node.GetInt()); // Ensure the control connection has moved to node 2
    }
    {
      OStringStream ip_with_port;
      ip_with_port << "127.0.0.3:" << config_.port();
      const json::Value& node = value[ip_with_port.str().c_str()]["connections"];
      ASSERT_EQ(1, node.GetInt());
    }
  }
}

TEST_F(ClientInsightsUnitTest, StatusDataUpdatedControlConnection) {
  mockssandra::SimpleCluster cluster(simple_dse_with_rpc_call(1), 2);
  ASSERT_EQ(cluster.start_all(), 0);

  config_.set_constant_reconnect(100u); // Reconnect immediately
  connect();

  String message = startup_message();
  cluster.stop(1);
  reset_latch();
  json::Document document;
  document.Parse(message.c_str());
  String initial_control_connection = document["data"]["initialControlConnection"].GetString();
  {
    OStringStream ip_with_port;
    ip_with_port << "127.0.0.1:" << config_.port();
    ASSERT_EQ(ip_with_port.str(), initial_control_connection);
  }

  message = status_message();
  document.Parse(message.c_str());
  String control_connection = document["data"]["controlConnection"].GetString();
  {
    OStringStream ip_with_port;
    ip_with_port << "127.0.0.2:" << config_.port();
    ASSERT_EQ(ip_with_port.str(), control_connection);
  }
}

TEST_F(ClientInsightsUnitTest, StatusDataInFlightQueries) {
  mockssandra::SimpleCluster cluster(simple_dse_with_rpc_call(4));
  ASSERT_EQ(cluster.start_all(), 0);
  connect();

  for (int i = 0; i < 37; ++i) {
    session_.execute(Request::ConstPtr(new QueryRequest("wait", 0)));
  }

  OStringStream ip_with_port;
  ip_with_port << "127.0.0.1:" << config_.port();

  {
    String message = status_message();
    json::Document document;
    document.Parse(message.c_str());
    ASSERT_EQ(37, document["data"]["conntectedNodes"][ip_with_port.str().c_str()]["inFlightQueries"]
                      .GetInt());
  }

  {
    String message = status_message(3);
    json::Document document;
    document.Parse(message.c_str());
    ASSERT_EQ(0, document["data"]["conntectedNodes"][ip_with_port.str().c_str()]["inFlightQueries"]
                     .GetInt());
  }
}

TEST_F(ClientInsightsUnitTest, DisableClientInsights) {
  mockssandra::SimpleCluster cluster(simple_dse_with_rpc_call(0));
  ASSERT_EQ(cluster.start_all(), 0);
  connect(0); // Disable client insights

  String message = startup_message();
  ASSERT_TRUE(message.empty());
  message = status_message();
  ASSERT_TRUE(message.empty());
}
