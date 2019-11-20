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

#ifndef DATASTAX_ENTERPRISE_INTERNAL_CLIENT_INSIGHTS_HPP
#define DATASTAX_ENTERPRISE_INTERNAL_CLIENT_INSIGHTS_HPP

#include "config.hpp"
#include "json.hpp"
#include "monitor_reporting.hpp"
#include "resolver.hpp"

namespace datastax { namespace internal { namespace enterprise {

class ClientInsightsRequestCallback;
class StartupMessageHandler;

class ClientInsights : public core::MonitorReporting {
public:
  typedef json::StringBuffer StringBuffer;
  typedef json::Writer<StringBuffer> Writer;

  ClientInsights(const String& client_id, const String& session_id, unsigned interval_secs);
  virtual ~ClientInsights() {}

  virtual uint64_t interval_ms(const core::VersionNumber& dse_server_version) const;
  virtual void send_startup_message(const core::Connection::Ptr& connection,
                                    const core::Config& config, const core::HostMap& hosts,
                                    const core::LoadBalancingPolicy::Vec& initialized_policies);
  virtual void send_status_message(const core::Connection::Ptr& connection,
                                   const core::HostMap& hosts);

private:
  const String client_id_;
  const String session_id_;
  const uint64_t interval_ms_;
};

}}} // namespace datastax::internal::enterprise

#endif // DATASTAX_ENTERPRISE_INTERNAL_CLIENT_INSIGHTS_HPP
