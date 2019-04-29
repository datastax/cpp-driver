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

namespace datastax { namespace internal { namespace enterprise {

class ClientInsightsRequestCallback;
class StartupMessageHandler;

class ClientInsights : public core::MonitorReporting {
public:
  typedef json::StringBuffer StringBuffer;
  typedef json::Writer<StringBuffer> Writer;

  ClientInsights(const String& client_id,
                 const String& session_id,
                 unsigned interval_secs);
  virtual ~ClientInsights() { }

  virtual uint64_t interval_ms(const core::VersionNumber& dse_server_version) const;
  virtual void send_startup_message(const core::Connection::Ptr& connection,
                                    const core::Config& config,
                                    const core::HostMap& hosts,
                                    const core::LoadBalancingPolicy::Vec& initialized_policies);
  virtual void send_status_message(const core::Connection::Ptr& connection,
                                   const core::HostMap& hosts);

private:
  const String client_id_;
  const String session_id_;
  const uint64_t interval_ms_;
};

} } } // namespace datastax::internal::enterprise

#endif // __DSE_CLIENT_INSIGHTS_HPP_INCLUDED__
