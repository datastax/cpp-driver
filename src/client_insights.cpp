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

#include "client_insights.hpp"

#include "config.hpp"
#include "driver_info.hpp"
#include "get_time.hpp"
#include "logger.hpp"
#include "session.hpp"
#include "ssl.hpp"
#include "string.hpp"
#include "utils.hpp"

#include <cassert>
#include <uv.h>

#ifdef _WIN32
#include <windows.h>
#include <winsock2.h>
#else
#include <sys/utsname.h>
#include <unistd.h>
#endif
#define HOSTNAME_MAX_LENGTH 256

#define METADATA_STARTUP_NAME "driver.startup"
#define METADATA_STATUS_NAME "driver.status"
#define METADATA_INSIGHTS_MAPPING_ID "v1"
#define METADATA_LANGUAGE "C/C++"

#define CONFIG_ANTIPATTERN_MSG_MULTI_DC_HOSTS \
  "Contact points contain hosts from "        \
  "multiple data centers but only one "       \
  "is going to be used"
#define CONFIG_ANTIPATTERN_MSG_REMOTE_HOSTS "Using remote hosts for failover"
#define CONFIG_ANTIPATTERN_MSG_DOWNGRADING \
  "Downgrading consistency retry "         \
  "policy in use"
#define CONFIG_ANTIPATTERN_MSG_CERT_VALIDATION \
  "Client-to-node encryption is "              \
  "enabled but server certificate "            \
  "validation is disabled"
#define CONFIG_ANTIPATTERN_MSG_PLAINTEXT_NO_SSL \
  "Plain text authentication is "               \
  "enabled without client-to-node "             \
  "encryption"                                  \
  ""

namespace datastax { namespace internal { namespace core {

MonitorReporting* create_monitor_reporting(const String& client_id, const String& session_id,
                                           const Config& config) {
  // Ensure the client monitor events should be enabled
  unsigned interval_secs = config.monitor_reporting_interval_secs();
  if (interval_secs > 0) {
    return new enterprise::ClientInsights(client_id, session_id, interval_secs);
  }
  return new NopMonitorReporting();
}

}}} // namespace datastax::internal::core

using namespace datastax::internal::core;
using namespace datastax::internal;

namespace datastax { namespace internal { namespace enterprise {

#ifdef _WIN32
#define ERROR_BUFFER_MAX_LENGTH 1024
String get_last_error() {
  DWORD rc = GetLastError();

  char buf[ERROR_BUFFER_MAX_LENGTH];
  size_t size = FormatMessageA(FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS, NULL, rc,
                               MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
                               reinterpret_cast<LPSTR>(&buf[0]), ERROR_BUFFER_MAX_LENGTH, NULL);
  String str(buf, size);
  trim(str);
  return str;
}
#endif

String get_hostname() {
#ifdef _WIN32
  WSADATA data;
  WORD version_required = MAKEWORD(2, 2);
  if (WSAStartup(version_required, &data) != 0) {
    LOG_WARN("Unable to determine hostname: Failed to initialize WinSock2");
    return String();
  }
#endif

  char buf[HOSTNAME_MAX_LENGTH + 1];
  size_t size = HOSTNAME_MAX_LENGTH + 1;
  if (int rc = gethostname(buf, size) != 0) {
    LOG_WARN("Unable to determine hostname: Error code %d", rc);
    return "UNKNOWN";
  }
  return String(buf, size);
}

struct Os {
  String name;
  String version;
  String arch;
};
Os get_os() {
  Os os;
#ifdef _WIN32
  os.name = "Microsoft Windows";

  DWORD size = GetFileVersionInfoSize(TEXT("kernel32.dll"), NULL);
  if (size) {
    Vector<BYTE> version_info(size);
    if (GetFileVersionInfo(TEXT("kernel32.dll"), 0, size, &version_info[0])) {
      VS_FIXEDFILEINFO* file_info = NULL;
      UINT file_info_length = 0;
      if (VerQueryValue(&version_info[0], TEXT("\\"), reinterpret_cast<LPVOID*>(&file_info),
                        &file_info_length)) {
        OStringStream oss;
        oss << static_cast<int>(HIWORD(file_info->dwProductVersionMS)) << "."
            << static_cast<int>(LOWORD(file_info->dwProductVersionMS)) << "."
            << static_cast<int>(HIWORD(file_info->dwProductVersionLS));
        os.version = oss.str();
      } else {
        LOG_DEBUG("Unable to retrieve Windows version: %s\n", get_last_error().c_str());
      }
    } else {
      LOG_DEBUG("Unable to retrieve Windows version (GetFileVersionInfo): %s\n",
                get_last_error().c_str());
    }
  } else {
    LOG_DEBUG("Unable to retrieve Windows version (GetFileVersionInfoSize): %s\n",
              get_last_error().c_str());
  }

#ifdef _WIN64
  os.arch = "x64";
#else
  os.arch = "x86";
#endif
#else
  struct utsname client_info;
  uname(&client_info);
  os.name = client_info.sysname;
  os.version = client_info.release;
  os.arch = client_info.machine;
#endif

  return os;
}

struct Cpus {
  int length;
  String model;
};
Cpus get_cpus() {
  Cpus cpus;

  uv_cpu_info_t* cpus_infos;
  int cpus_count;
  int rc = uv_cpu_info(&cpus_infos, &cpus_count);
  if (rc == 0) {
    uv_cpu_info_t cpus_info = cpus_infos[0];
    cpus.length = cpus_count;
    cpus.model = cpus_info.model;
    uv_free_cpu_info(cpus_infos, cpus_count);
  } else {
    LOG_DEBUG("Unable to determine CPUs information: %s\n", uv_strerror(rc));
  }

  return cpus;
}

class ClientInsightsRequestCallback : public SimpleRequestCallback {
public:
  typedef SharedRefPtr<ClientInsightsRequestCallback> Ptr;

  ClientInsightsRequestCallback(const String& json, const String& event_type)
      : SimpleRequestCallback("CALL InsightsRpc.reportInsight('" + json + "')")
      , event_type_(event_type) {}

  virtual void on_internal_set(ResponseMessage* response) {
    if (response->opcode() != CQL_OPCODE_RESULT) {
      LOG_DEBUG("Failed to send %s event message: Invalid response [%s]", event_type_.c_str(),
                opcode_to_string(response->opcode()).c_str());
    }
  }

  virtual void on_internal_error(CassError code, const String& message) {
    LOG_DEBUG("Failed to send %s event message: %s", event_type_.c_str(), message.c_str());
  }

  virtual void on_internal_timeout() {
    LOG_DEBUG("Failed to send %s event message: Timed out waiting for response",
              event_type_.c_str());
  }

private:
  String event_type_;
};

void metadata(ClientInsights::Writer& writer, const String& name) {
  writer.Key("metadata");
  writer.StartObject();

  writer.Key("name");
  writer.String(name.c_str());
  writer.Key("insightMappingId");
  writer.String(METADATA_INSIGHTS_MAPPING_ID);
  writer.Key("insightType");
  writer.String("EVENT"); // TODO: Make this an enumeration in the future
  writer.Key("timestamp");
  writer.Uint64(get_time_since_epoch_ms());
  writer.Key("tags");
  writer.StartObject();
  writer.Key("language");
  writer.String(METADATA_LANGUAGE);
  writer.EndObject(); // tags

  writer.EndObject();
}

class StartupMessageHandler : public RefCounted<StartupMessageHandler> {
public:
  typedef SharedRefPtr<StartupMessageHandler> Ptr;

  StartupMessageHandler(const Connection::Ptr& connection, const String& client_id,
                        const String& session_id, const Config& config, const HostMap& hosts,
                        const LoadBalancingPolicy::Vec& initialized_policies)
      : connection_(connection)
      , client_id_(client_id)
      , session_id_(session_id)
      , config_(config)
      , hosts_(hosts)
      , initialized_policies_(initialized_policies) {}

  ~StartupMessageHandler() {
    ClientInsights::StringBuffer buffer;
    ClientInsights::Writer writer(buffer);

    writer.StartObject();
    metadata(writer, METADATA_STARTUP_NAME);
    startup_message_data(writer);
    writer.EndObject();

    assert(writer.IsComplete() && "Startup JSON is incomplete");
    connection_->write_and_flush(RequestCallback::Ptr(
        new ClientInsightsRequestCallback(buffer.GetString(), METADATA_STARTUP_NAME)));
  }

  void send_message() { resolve_contact_points(); }

private:
  // Startup message associated methods
  void startup_message_data(ClientInsights::Writer& writer) {
    writer.Key("data");
    writer.StartObject();

    writer.Key("clientId");
    writer.String(client_id_.c_str());
    writer.Key("sessionId");
    writer.String(session_id_.c_str());
    bool is_application_name_generated = false;
    writer.Key("applicationName");
    if (!config_.application_name().empty()) {
      writer.String(config_.application_name().c_str());
    } else {
      writer.String(driver_name());
      is_application_name_generated = true;
    }
    writer.Key("applicationNameWasGenerated");
    writer.Bool(is_application_name_generated);
    if (!config_.application_version().empty()) {
      writer.Key("applicationVersion");
      writer.String(config_.application_version().c_str());
    }
    writer.Key("driverName");
    writer.String(driver_name());
    writer.Key("driverVersion");
    writer.String(driver_version());
    contact_points(writer);
    data_centers(writer);
    writer.Key("initialControlConnection");
    writer.String(connection_->resolved_address().to_string(true).c_str());
    writer.Key("protocolVersion");
    writer.Int(connection_->protocol_version().value());
    writer.Key("localAddress");
    writer.String(get_local_address(connection_->handle()).c_str());
    writer.Key("hostName");
    writer.String(get_hostname().c_str());
    execution_profiles(writer);
    pool_size_by_host_distance(writer);
    writer.Key("heartbeatInterval");
    writer.Uint64(config_.connection_heartbeat_interval_secs() * 1000); // in milliseconds
    writer.Key("compression");
    writer.String("NONE"); // TODO: Update once compression is added
    reconnection_policy(writer);
    ssl(writer);
    auth_provider(writer);
    other_options(writer);
    platform_info(writer);
    config_anti_patterns(writer);
    writer.Key("periodicStatusInterval");
    writer.Uint(config_.monitor_reporting_interval_secs());

    writer.EndObject();
  }

  void contact_points(ClientInsights::Writer& writer) {
    writer.Key("contactPoints");
    writer.StartObject();

    for (ResolvedHostMap::const_iterator map_it = contact_points_resolved_.begin(),
                                         map_end = contact_points_resolved_.end();
         map_it != map_end; ++map_it) {
      writer.Key(map_it->first.c_str());
      writer.StartArray();
      for (AddressSet::const_iterator vec_it = map_it->second.begin(),
                                      vec_end = map_it->second.end();
           vec_it != vec_end; ++vec_it) {
        writer.String(vec_it->to_string(true).c_str());
      }
      writer.EndArray();
    }

    writer.EndObject();
  }

  void data_centers(ClientInsights::Writer& writer) {
    writer.Key("dataCenters");
    writer.StartArray();

    Set<String> data_centers;
    for (HostMap::const_iterator it = hosts_.begin(), end = hosts_.end(); it != end; ++it) {
      const String& data_center = it->second->dc();
      if (data_centers.insert(data_center).second) {
        writer.String(data_center.c_str());
      }
    }

    writer.EndArray();
  }

  void execution_profiles(ClientInsights::Writer& writer) {
    writer.Key("executionProfiles");
    writer.StartObject();

    const ExecutionProfile& default_profile = config_.default_profile();
    const ExecutionProfile::Map& profiles = config_.profiles();
    writer.Key("default");
    execution_profile_as_json(writer, default_profile);
    for (ExecutionProfile::Map::const_iterator it = profiles.begin(), end = profiles.end();
         it != end; ++it) {
      writer.Key(it->first.c_str());
      execution_profile_as_json(writer, it->second, &default_profile);
    }

    writer.EndObject();
  }

  void pool_size_by_host_distance(ClientInsights::Writer& writer) {
    writer.Key("poolSizeByHostDistance");
    writer.StartObject();

    writer.Key("local");
    writer.Uint(config_.core_connections_per_host() * hosts_.size());
    // NOTE: Remote does not pertain to DataStax C/C++ driver pool
    writer.Key("remote");
    writer.Uint(0);

    writer.EndObject();
  }

  void reconnection_policy(ClientInsights::Writer& writer) {
    writer.Key("reconnectionPolicy");
    writer.StartObject();

    ReconnectionPolicy::Ptr reconnection_policy = config_.reconnection_policy();
    writer.Key("type");
    if (reconnection_policy->type() == ReconnectionPolicy::CONSTANT) {
      writer.String("ConstantReconnectionPolicy");
    } else if (reconnection_policy->type() == ReconnectionPolicy::EXPONENTIAL) {
      writer.String("ExponentialReconnectionPolicy");
    } else {
      assert(false && "Reconnection policy needs to be added");
      writer.String("UnknownReconnectionPolicy");
    }
    writer.Key("options");
    writer.StartObject();
    if (reconnection_policy->type() == ReconnectionPolicy::CONSTANT) {
      ConstantReconnectionPolicy::Ptr crp =
          static_cast<ConstantReconnectionPolicy::Ptr>(reconnection_policy);
      writer.Key("delayMs");
      writer.Uint(crp->delay_ms());
    } else if (reconnection_policy->type() == ReconnectionPolicy::EXPONENTIAL) {
      ExponentialReconnectionPolicy::Ptr erp =
          static_cast<ExponentialReconnectionPolicy::Ptr>(reconnection_policy);
      writer.Key("baseDelayMs");
      writer.Uint(erp->base_delay_ms());
      writer.Key("maxDelayMs");
      writer.Uint(erp->max_delay_ms());
    }
    writer.EndObject(); // options

    writer.EndObject();
  }

  void ssl(ClientInsights::Writer& writer) {
    writer.Key("ssl");
    writer.StartObject();

    const SslContext::Ptr& ssl_context = config_.ssl_context();
    writer.Key("enabled");
    if (ssl_context) {
      writer.Bool(true);
    } else {
      writer.Bool(false);
    }
    writer.Key("certValidation");
    if (ssl_context) {
      writer.Bool(ssl_context->is_cert_validation_enabled());
    } else {
      writer.Bool(false);
    }

    writer.EndObject();
  }

  void auth_provider(ClientInsights::Writer& writer) {
    const AuthProvider::Ptr& auth_provider = config_.auth_provider();
    if (auth_provider) {
      writer.Key("authProvider");
      writer.StartObject();

      writer.Key("type");
      writer.String(auth_provider->name().c_str());

      writer.EndObject();
    }
  }

  void other_options(ClientInsights::Writer& writer) {
    writer.Key("otherOptions");
    writer.StartObject();

    writer.Key("configuration");
    writer.StartObject();
    writer.Key("protocolVersion");
    writer.Uint(config_.protocol_version().value());
    writer.Key("useBetaProtocol");
    writer.Bool(config_.use_beta_protocol_version());
    writer.Key("threadCountIo");
    writer.Uint(config_.thread_count_io());
    writer.Key("queueSizeIo");
    writer.Uint(config_.queue_size_io());
    writer.Key("coreConnectionsPerHost");
    writer.Uint(config_.core_connections_per_host());
    writer.Key("connectTimeoutMs");
    writer.Uint(config_.connect_timeout_ms());
    writer.Key("resolveTimeoutMs");
    writer.Uint(config_.resolve_timeout_ms());
    writer.Key("maxSchemaWaitTimeMs");
    writer.Uint(config_.max_schema_wait_time_ms());
    writer.Key("maxTracingWaitTimeMs");
    writer.Uint(config_.max_tracing_wait_time_ms());
    writer.Key("tracingConsistency");
    writer.String(cass_consistency_string(config_.tracing_consistency()));
    writer.Key("coalesceDelayUs");
    writer.Uint64(config_.coalesce_delay_us());
    writer.Key("newRequestRatio");
    writer.Uint(config_.new_request_ratio());
    writer.Key("logLevel");
    writer.String(cass_log_level_string(config_.log_level()));
    writer.Key("tcpNodelayEnable");
    writer.Bool(config_.tcp_nodelay_enable());
    writer.Key("tcpKeepaliveEnable");
    writer.Bool(config_.tcp_keepalive_enable());
    writer.Key("tcpKeepaliveDelaySecs");
    writer.Uint(config_.tcp_keepalive_delay_secs());
    writer.Key("connectionIdleTimeoutSecs");
    writer.Uint(config_.connection_idle_timeout_secs());
    writer.Key("useSchema");
    writer.Bool(config_.use_schema());
    writer.Key("useHostnameResolution");
    writer.Bool(config_.use_hostname_resolution());
    writer.Key("useRandomizedContactPoints");
    writer.Bool(config_.use_randomized_contact_points());
    writer.Key("maxReusableWriteObjects");
    writer.Uint(config_.max_reusable_write_objects());
    writer.Key("prepareOnAllHosts");
    writer.Bool(config_.prepare_on_all_hosts());
    writer.Key("prepareOnUpOrAddHost");
    writer.Bool(config_.prepare_on_up_or_add_host());
    writer.Key("noCompact");
    writer.Bool(config_.no_compact());
    writer.Key("cloudSecureConnectBundleLoaded");
    writer.Bool(config_.cloud_secure_connection_config().is_loaded());
    writer.Key("clusterMetadataResolver");
    writer.String(config_.cluster_metadata_resolver_factory()->name());
    writer.EndObject(); // configuration

    writer.EndObject(); // otherOptions
  }

  void platform_info(ClientInsights::Writer& writer) {
    writer.Key("platformInfo");
    writer.StartObject();

    writer.Key("os");
    writer.StartObject();
    Os os = get_os();
    writer.Key("name");
    writer.String(os.name.c_str());
    writer.Key("version");
    writer.String(os.version.c_str());
    writer.Key("arch");
    writer.String(os.arch.c_str());
    writer.EndObject(); // os

    writer.Key("cpus");
    writer.StartObject();
    Cpus cpus = get_cpus();
    writer.Key("length");
    writer.Int(cpus.length);
    writer.Key("model");
    writer.String(cpus.model.c_str());
    writer.EndObject(); // cpus

    writer.Key("runtime");
    writer.StartObject();
#if defined(__clang__) || defined(__APPLE_CC__)
    writer.Key("Clang/LLVM");
    writer.String(STRINGIFY(__clang_major__) "." STRINGIFY(__clang_minor__) "." STRINGIFY(
        __clang_patchlevel__));
#elif defined(__INTEL_COMPILER)
    writer.Key("Intel ICC/ICPC");
    writer.String(STRINGIFY(__INTEL_COMPILER));
#elif defined(__GNUC__) || defined(__GNUG__)
    writer.Key("GNU GCC/G++");
    writer.String(
        STRINGIFY(__GNUC__) "." STRINGIFY(__GNUC_MINOR__) "." STRINGIFY(__GNUC_PATCHLEVEL__));
#elif defined(__HP_aCC)
    writer.Key("Hewlett-Packard C/aC++");
    writer.String(STRINGIFY(__HP_aCC));
#elif defined(__IBMCPP__)
    writer.Key("IBM XL C/C++");
    writer.String(STRINGIFY(__xlc__));
#elif defined(_MSC_VER)
    writer.Key("Microsoft Visual Studio");
    writer.String(STRINGIFY(_MSC_FULL_VER));
#elif defined(__PGI)
    writer.Key("Portland Group PGCC/PGCPP");
    writer.String(
        STRINGIFY(__PGIC__) "." STRINGIFY(__PGIC_MINOR__) "." STRINGIFY(__PGIC_PATCHLEVEL__));
#elif defined(__SUNPRO_CC)
    writer.Key("Oracle Solaris Studio");
    writer.String(STRINGIFY(__SUNPRO_CC));
#else
    writer.Key("Unknown");
    writer.String("Unknown");
#endif
    writer.Key("uv");
    writer.String(STRINGIFY(UV_VERSION_MAJOR) "." STRINGIFY(UV_VERSION_MINOR) "." STRINGIFY(
        UV_VERSION_PATCH));
    writer.Key("openssl");
#ifdef OPENSSL_VERSION_TEXT
    writer.String(OPENSSL_VERSION_TEXT);
#else
#ifdef LIBRESSL_VERSION_NUMBER
    writer.String("LibreSSL " STRINGIFY(LIBRESSL_VERSION_NUMBER));
#else
    writer.String("OpenSSL " STRINGIFY(OPENSSL_VERSION_NUMBER));
#endif
#endif
    writer.EndObject(); // runtime

    writer.EndObject(); // platformInfo
  }

  void config_anti_patterns(ClientInsights::Writer& writer) {
    StringPairVec config_anti_patterns = get_config_anti_patterns(
        config_.default_profile(), config_.profiles(), initialized_policies_, hosts_,
        config_.ssl_context(), config_.auth_provider());
    if (!config_anti_patterns.empty()) {
      writer.Key("configAntiPatterns");
      writer.StartObject();

      for (StringPairVec::const_iterator it = config_anti_patterns.begin(),
                                         end = config_anti_patterns.end();
           it != end; ++it) {
        writer.Key(it->first.c_str());
        writer.String(it->second.c_str());
      }

      writer.EndObject();
    }
  }

private:
  // Startup message helper methods
  void resolve_contact_points() {
    const AddressVec& contact_points = config_.contact_points();
    const int port = config_.port();
    MultiResolver::Ptr resolver;

    for (AddressVec::const_iterator it = contact_points.begin(), end = contact_points.end();
         it != end; ++it) {
      const Address& contact_point = *it;
      // Attempt to parse the contact point string. If it's an IP address
      // then immediately add it to our resolved contact points, otherwise
      // attempt to resolve the string as a hostname.
      if (contact_point.is_resolved()) {
        AddressSet addresses;
        addresses.insert(contact_point);
        contact_points_resolved_[contact_point.hostname_or_address()] = addresses;
      } else {
        if (!resolver) {
          inc_ref();
          resolver.reset(
              new MultiResolver(bind_callback(&StartupMessageHandler::on_resolve, this)));
        }
        resolver->resolve(connection_->loop(), contact_point.hostname_or_address(), port,
                          config_.resolve_timeout_ms());
      }
    }

    // NOTE: If no resolution is performed the startup message will be sent in
    //       the destructor
  }

  void on_resolve(MultiResolver* resolver) {
    const Resolver::Vec& resolvers = resolver->resolvers();
    for (Resolver::Vec::const_iterator it = resolvers.begin(), end = resolvers.end(); it != end;
         ++it) {
      const Resolver::Ptr resolver(*it);
      AddressSet addresses;
      if (resolver->is_success()) {
        if (!resolver->addresses().empty()) {
          for (AddressVec::const_iterator it = resolver->addresses().begin(),
                                          end = resolver->addresses().end();
               it != end; ++it) {
            addresses.insert(*it);
          }
        }
      }
      contact_points_resolved_[resolver->hostname()] = addresses; // Empty resolved addresses are OK
    }

    dec_ref(); // Send startup message in destructor
  }

  String get_local_address(const uv_tcp_t* tcp) const {
    Address::SocketStorage name;
    int namelen = sizeof(name);
    if (uv_tcp_getsockname(tcp, name.addr(), &namelen) == 0) {
      Address address(name.addr());
      if (address.is_valid_and_resolved()) {
        return address.to_string();
      }
    }
    return "unknown";
  }

  void execution_profile_as_json(ClientInsights::Writer& writer, const ExecutionProfile& profile,
                                 const ExecutionProfile* default_profile = NULL) {
    writer.StartObject();

    if (!default_profile || (default_profile && profile.request_timeout_ms() !=
                                                    default_profile->request_timeout_ms())) {
      writer.Key("requestTimeoutMs");
      writer.Uint64(profile.request_timeout_ms());
    }
    if (!default_profile ||
        (default_profile && profile.consistency() != default_profile->consistency())) {
      writer.Key("consistency");
      writer.String(cass_consistency_string(profile.consistency()));
    }
    if (!default_profile || (default_profile && profile.serial_consistency() !=
                                                    default_profile->serial_consistency())) {
      writer.Key("serialConsistency");
      writer.String(cass_consistency_string(profile.serial_consistency()));
    }
    if (!default_profile ||
        (default_profile && profile.retry_policy() != default_profile->retry_policy())) {
      const RetryPolicy::Ptr& retry_policy = profile.retry_policy();
      if (retry_policy) {
        writer.Key("retryPolicy");
        if (retry_policy->type() == RetryPolicy::DEFAULT) {
          writer.String("DefaultRetryPolicy");
        } else if (retry_policy->type() == RetryPolicy::DOWNGRADING) {
          writer.String("DowngradingConsistencyRetryPolicy");
        } else if (retry_policy->type() == RetryPolicy::FALLTHROUGH) {
          writer.String("FallthroughRetryPolicy");
        } else if (retry_policy->type() == RetryPolicy::LOGGING) {
          writer.String("LoggingRetryPolicy");
        } else {
          LOG_DEBUG("Invalid retry policy: %d", retry_policy->type());
          writer.String("unknown");
        }
      }
    }

    if (profile.load_balancing_policy()) {
      writer.Key("loadBalancing");
      writer.StartObject();
      writer.Key("type");
      LoadBalancingPolicy* current_lbp = profile.load_balancing_policy().get();
      do {
        // NOTE: DCAware and RoundRobin are leaf policies (e.g. not chainable)
        if (dynamic_cast<DCAwarePolicy*>(current_lbp)) {
          writer.String("DCAwarePolicy");
          break;
        } else if (dynamic_cast<RoundRobinPolicy*>(current_lbp)) {
          writer.String("RoundRobinPolicy");
          break;
        }

        if (ChainedLoadBalancingPolicy* chained_lbp =
                dynamic_cast<ChainedLoadBalancingPolicy*>(current_lbp)) {
          current_lbp = chained_lbp->child_policy().get();
        } else {
          current_lbp = NULL;
        }
      } while (current_lbp);
      writer.Key("options");
      writer.StartObject();
      if (DCAwarePolicy* dc_lbp = dynamic_cast<DCAwarePolicy*>(current_lbp)) {
        writer.Key("localDc");
        if (dc_lbp->local_dc().empty()) {
          writer.Null();
        } else {
          writer.String(dc_lbp->local_dc().c_str());
        }
        writer.Key("usedHostsPerRemoteDc");
        writer.Uint64(dc_lbp->used_hosts_per_remote_dc());
        writer.Key("allowRemoteDcsForLocalCl");
        writer.Bool(!dc_lbp->skip_remote_dcs_for_local_cl());
      }
      if (!profile.blacklist().empty()) {
        writer.Key("blacklist");
        writer.String(implode(profile.blacklist()).c_str());
      }
      if (!profile.blacklist_dc().empty()) {
        writer.Key("blacklistDc");
        writer.String(implode(profile.blacklist_dc()).c_str());
      }
      if (!profile.whitelist().empty()) {
        writer.Key("whitelist");
        writer.String(implode(profile.whitelist()).c_str());
      }
      if (!profile.whitelist_dc().empty()) {
        writer.Key("whitelistDc");
        writer.String(implode(profile.whitelist_dc()).c_str());
      }
      if (profile.token_aware_routing()) {
        writer.Key("tokenAwareRouting");
        writer.StartObject();
        writer.Key("shuffleReplicas");
        writer.Bool(profile.token_aware_routing_shuffle_replicas());
        writer.EndObject(); // tokenAwareRouting
      }
      if (profile.latency_aware()) {
        writer.Key("latencyAwareRouting");
        writer.StartObject();
        writer.Key("exclusionThreshold");
        writer.Double(profile.latency_aware_routing_settings().exclusion_threshold);
        writer.Key("scaleNs");
        writer.Uint64(profile.latency_aware_routing_settings().scale_ns);
        writer.Key("retryPeriodNs");
        writer.Uint64(profile.latency_aware_routing_settings().retry_period_ns);
        writer.Key("updateRateMs");
        writer.Uint64(profile.latency_aware_routing_settings().update_rate_ms);
        writer.Key("minMeasured");
        writer.Uint64(profile.latency_aware_routing_settings().min_measured);
        writer.EndObject(); // latencyAwareRouting
      }
      writer.EndObject(); // options

      writer.EndObject(); // loadBalancingPolicy
    }

    typedef ConstantSpeculativeExecutionPolicy CSEP;
    CSEP* default_csep =
        default_profile ? dynamic_cast<CSEP*>(default_profile->speculative_execution_policy().get())
                        : NULL;
    CSEP* csep = dynamic_cast<CSEP*>(profile.speculative_execution_policy().get());
    if (csep) {
      if (!default_csep ||
          (default_csep->constant_delay_ms_ != csep->constant_delay_ms_ &&
           default_csep->max_speculative_executions_ != csep->max_speculative_executions_)) {
        writer.Key("speculativeExecutionPolicy");
        writer.StartObject();
        writer.Key("type");
        writer.String("ConstantSpeculativeExecutionPolicy");

        writer.Key("options");
        writer.StartObject();
        writer.Key("constantDelayMs");
        writer.Uint64(csep->constant_delay_ms_);
        writer.Key("maxSpeculativeExecutions");
        writer.Int(csep->max_speculative_executions_);
        writer.EndObject(); // options

        writer.EndObject(); // speculativeExecutionPolicy
      }
    }

    writer.EndObject(); // executionProfile
  }

  typedef std::pair<String, String> StringPair;
  typedef Vector<StringPair> StringPairVec;
  StringPairVec get_config_anti_patterns(const ExecutionProfile& default_profile,
                                         const ExecutionProfile::Map& profiles,
                                         const LoadBalancingPolicy::Vec& policies,
                                         const HostMap& hosts, const SslContext::Ptr& ssl_context,
                                         const AuthProvider::Ptr& auth_provider) {
    StringPairVec config_anti_patterns;

    if (is_contact_points_multiple_dcs(policies, hosts)) {
      config_anti_patterns.push_back(
          StringPair("contactPointsMultipleDCs", CONFIG_ANTIPATTERN_MSG_MULTI_DC_HOSTS));
      LOG_WARN("Configuration anti-pattern detected: %s", CONFIG_ANTIPATTERN_MSG_MULTI_DC_HOSTS);
    }

    for (LoadBalancingPolicy::Vec::const_iterator it = policies.begin(), end = policies.end();
         it != end; ++it) {
      DCAwarePolicy* dc_lbp = get_dc_aware_policy(*it);
      if (dc_lbp && !dc_lbp->skip_remote_dcs_for_local_cl()) {
        config_anti_patterns.push_back(
            StringPair("useRemoteHosts", CONFIG_ANTIPATTERN_MSG_REMOTE_HOSTS));
        LOG_WARN("Configuration anti-pattern detected: %s", CONFIG_ANTIPATTERN_MSG_REMOTE_HOSTS);
        break;
      }
    }

    bool is_downgrading_consistency_enabled =
        is_downgrading_retry_anti_pattern(default_profile.retry_policy());
    if (!is_downgrading_consistency_enabled) {
      for (ExecutionProfile::Map::const_iterator it = profiles.begin(), end = profiles.end();
           it != end; ++it) {
        if (is_downgrading_retry_anti_pattern(it->second.retry_policy())) {
          is_downgrading_consistency_enabled = true;
          break;
        }
      }
    }
    if (is_downgrading_consistency_enabled) {
      config_anti_patterns.push_back(
          StringPair("downgradingConsistency", CONFIG_ANTIPATTERN_MSG_DOWNGRADING));
      LOG_WARN("Configuration anti-pattern detected: %s", CONFIG_ANTIPATTERN_MSG_DOWNGRADING);
    }

    if (ssl_context && !ssl_context->is_cert_validation_enabled()) {
      config_anti_patterns.push_back(
          StringPair("sslWithoutCertValidation", CONFIG_ANTIPATTERN_MSG_CERT_VALIDATION));
      LOG_WARN("Configuration anti-pattern detected: %s", CONFIG_ANTIPATTERN_MSG_CERT_VALIDATION);
    }

    if (auth_provider && auth_provider->name().find("PlainTextAuthProvider") != String::npos &&
        !ssl_context) {
      config_anti_patterns.push_back(
          StringPair("plainTextAuthWithoutSsl", CONFIG_ANTIPATTERN_MSG_PLAINTEXT_NO_SSL));
      LOG_WARN("Configuration anti-pattern detected: %s", CONFIG_ANTIPATTERN_MSG_PLAINTEXT_NO_SSL);
    }

    return config_anti_patterns;
  }

  DCAwarePolicy* get_dc_aware_policy(const LoadBalancingPolicy::Ptr& policy) {
    LoadBalancingPolicy* current_lbp = policy.get();
    do {
      if (DCAwarePolicy* dc_lbp = dynamic_cast<DCAwarePolicy*>(current_lbp)) {
        return dc_lbp;
      }
      if (ChainedLoadBalancingPolicy* chained_lbp =
              dynamic_cast<ChainedLoadBalancingPolicy*>(current_lbp)) {
        current_lbp = chained_lbp->child_policy().get();
      } else {
        break;
      }
    } while (current_lbp);

    return NULL;
  }

  bool is_contact_points_multiple_dcs(const LoadBalancingPolicy::Vec& policies,
                                      const HostMap& hosts) {
    // Get the DC aware load balancing policy if it is the only policy that
    // exists. If found this policy will be used after the contact points have
    // been resolved in order to determine if there are contacts points that exist
    // in multiple DCs using a copy of the discovered hosts.
    if (policies.size() == 1) {
      DCAwarePolicy* policy = get_dc_aware_policy(policies[0]);
      if (policy) {
        // Loop through the resolved contacts, find the correct initialized host
        // and if the contact point is a remote host identify as an anti-pattern
        for (ResolvedHostMap::const_iterator resolved_it = contact_points_resolved_.begin(),
                                             hosts_end = contact_points_resolved_.end();
             resolved_it != hosts_end; ++resolved_it) {
          const AddressSet& addresses = resolved_it->second;
          for (AddressSet::const_iterator addresses_it = addresses.begin(),
                                          addresses_end = addresses.end();
               addresses_it != addresses_end; ++addresses_it) {
            const Address& address = *addresses_it;
            for (HostMap::const_iterator hosts_it = hosts.begin(), hosts_end = hosts.end();
                 hosts_it != hosts_end; ++hosts_it) {
              const Host::Ptr& host = hosts_it->second;
              if (host->address() == address &&
                  policy->distance(host) == CASS_HOST_DISTANCE_REMOTE) {
                return true;
              }
            }
          }
        }
      }
    }

    return false;
  }

  bool is_downgrading_retry_anti_pattern(const RetryPolicy::Ptr& policy) {
    if (policy && policy->type() == RetryPolicy::DOWNGRADING) {
      return true;
    }
    return false;
  }

private:
  const Connection::Ptr connection_;
  const String client_id_;
  const String session_id_;
  const Config config_;
  const HostMap hosts_;
  const LoadBalancingPolicy::Vec initialized_policies_;

private:
  typedef Map<String, AddressSet> ResolvedHostMap;
  ResolvedHostMap contact_points_resolved_;
};

ClientInsights::ClientInsights(const String& client_id, const String& session_id,
                               unsigned interval_secs)
    : client_id_(client_id)
    , session_id_(session_id)
    , interval_ms_(interval_secs * 1000) {}

uint64_t ClientInsights::interval_ms(const VersionNumber& dse_server_version) const {
  // DSE v5.1.13+ (backported)
  // DSE v6.0.5+ (backported)
  // DSE v6.7.0 was the first to supported the Insights RPC call
  if ((dse_server_version >= VersionNumber(5, 1, 13) &&
       dse_server_version < VersionNumber(6, 0, 0)) ||
      dse_server_version >= VersionNumber(6, 0, 5)) {
    return interval_ms_;
  }
  return 0;
}

void ClientInsights::send_startup_message(const Connection::Ptr& connection, const Config& config,
                                          const HostMap& hosts,
                                          const LoadBalancingPolicy::Vec& initialized_policies) {
  StartupMessageHandler::Ptr handler = StartupMessageHandler::Ptr(new StartupMessageHandler(
      connection, client_id_, session_id_, config, hosts, initialized_policies));
  handler->send_message();
}

void ClientInsights::send_status_message(const Connection::Ptr& connection, const HostMap& hosts) {
  StringBuffer buffer;
  Writer writer(buffer);

  writer.StartObject();
  metadata(writer, METADATA_STATUS_NAME);

  writer.Key("data");
  writer.StartObject();

  writer.Key("clientId");
  writer.String(client_id_.c_str());
  writer.Key("sessionId");
  writer.String(session_id_.c_str());
  writer.Key("controlConnection");
  writer.String(connection->resolved_address().to_string(true).c_str());

  writer.Key("conntectedNodes");
  writer.StartObject();
  for (HostMap::const_iterator it = hosts.begin(), end = hosts.end(); it != end; ++it) {
    String address_with_port = it->first.to_string(true);
    const Host::Ptr& host = it->second;
    writer.Key(address_with_port.c_str());
    writer.StartObject();
    writer.Key("connections");
    writer.Int(host->connection_count());
    writer.Key("inFlightQueries");
    writer.Int(host->inflight_request_count());
    writer.EndObject(); // address_with_port
  }
  writer.EndObject(); // connectedNodes

  writer.EndObject(); // data
  writer.EndObject();

  assert(writer.IsComplete() && "Status JSON is incomplete");
  connection->write_and_flush(RequestCallback::Ptr(
      new ClientInsightsRequestCallback(buffer.GetString(), METADATA_STATUS_NAME)));
}

}}} // namespace datastax::internal::enterprise
