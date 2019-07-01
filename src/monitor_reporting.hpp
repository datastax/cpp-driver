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

#ifndef DATASTAX_INTERNAL_MONITOR_REPORTING_HPP
#define DATASTAX_INTERNAL_MONITOR_REPORTING_HPP

#include "connection.hpp"
#include "host.hpp"
#include "load_balancing.hpp"
#include "string.hpp"

namespace datastax { namespace internal { namespace core {

class Config;

class MonitorReporting {
public:
  virtual ~MonitorReporting() {}
  virtual uint64_t interval_ms(const VersionNumber& dse_server_version) const = 0;
  virtual void send_startup_message(const Connection::Ptr& connection, const Config& config,
                                    const HostMap& hosts,
                                    const LoadBalancingPolicy::Vec& initialized_policies) = 0;
  virtual void send_status_message(const Connection::Ptr& connection, const HostMap& hosts) = 0;
};

class NopMonitorReporting : public MonitorReporting {
public:
  uint64_t interval_ms(const VersionNumber& dse_server_version) const { return 0; }
  void send_startup_message(const Connection::Ptr& connection, const Config& config,
                            const HostMap& hosts,
                            const LoadBalancingPolicy::Vec& initialized_policies) {}
  void send_status_message(const Connection::Ptr& connection, const HostMap& hosts) {}
};

MonitorReporting* create_monitor_reporting(const String& client_id, const String& session_id,
                                           const Config& config);

}}} // namespace datastax::internal::core

#endif // DATASTAX_INTERNAL_MONITOR_REPORTING_HPP
