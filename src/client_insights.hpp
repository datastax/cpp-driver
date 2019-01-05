/*
  Copyright (c) DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __DSE_CLIENT_INSIGHTS_HPP_INCLUDED__
#define __DSE_CLIENT_INSIGHTS_HPP_INCLUDED__

#include "config.hpp"
#include "json.hpp"
#include "monitor_reporting.hpp"
#include "resolver.hpp"

namespace cass {

class Session;

} // namespace cass

namespace dse {

class ClientInsightsRequestCallback;
class StartupMessageHandler;

class ClientInsights : public cass::MonitorReporting {
public:
  typedef cass::json::StringBuffer StringBuffer;
  typedef cass::json::Writer<StringBuffer> Writer;

  ClientInsights(const cass::String& client_id,
                 const cass::String& session_id,
                 const cass::Config& config);
  virtual ~ClientInsights() { }

  virtual uint64_t interval_ms(const cass::VersionNumber& dse_server_version) const;
  virtual void send_startup_message(const cass::Connection::Ptr& connection,
                                    const cass::Config& config,
                                    const cass::HostMap& hosts,
                                    const cass::LoadBalancingPolicy::Vec& initialized_policies);
  virtual void send_status_message(const cass::Connection::Ptr& connection,
                                   const cass::HostMap& hosts);

private:
  const cass::String client_id_;
  const cass::String session_id_;
  const unsigned core_connections_per_host_;
  const unsigned thread_count_io_;
  const uint64_t interval_ms_;
};

} // namespace dse

#endif // __DSE_CLIENT_INSIGHTS_HPP_INCLUDED__
