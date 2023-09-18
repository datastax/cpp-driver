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

#include "execution_profile.hpp"

using namespace datastax;
using namespace datastax::internal::core;

extern "C" {

CassExecProfile* cass_execution_profile_new() {
  ExecutionProfile* profile = new ExecutionProfile();
  return CassExecProfile::to(profile);
}

void cass_execution_profile_free(CassExecProfile* profile) { delete profile->from(); }

CassError cass_execution_profile_set_request_timeout(CassExecProfile* profile,
                                                     cass_uint64_t timeout_ms) {
  profile->set_request_timeout(timeout_ms);
  return CASS_OK;
}

CassError cass_execution_profile_set_consistency(CassExecProfile* profile,
                                                 CassConsistency consistency) {
  profile->set_consistency(consistency);
  return CASS_OK;
}

CassError cass_execution_profile_set_serial_consistency(CassExecProfile* profile,
                                                        CassConsistency serial_consistency) {
  profile->set_serial_consistency(serial_consistency);
  return CASS_OK;
}

CassError cass_execution_profile_set_load_balance_round_robin(CassExecProfile* profile) {
  profile->set_load_balancing_policy(new RoundRobinPolicy());
  return CASS_OK;
}

CassError
cass_execution_profile_set_load_balance_dc_aware(CassExecProfile* profile, const char* local_dc,
                                                 unsigned used_hosts_per_remote_dc,
                                                 cass_bool_t allow_remote_dcs_for_local_cl) {
  if (local_dc == NULL) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  return cass_execution_profile_set_load_balance_dc_aware_n(
      profile, local_dc, SAFE_STRLEN(local_dc), used_hosts_per_remote_dc,
      allow_remote_dcs_for_local_cl);
}

CassError cass_execution_profile_set_load_balance_dc_aware_n(
    CassExecProfile* profile, const char* local_dc, size_t local_dc_length,
    unsigned used_hosts_per_remote_dc, cass_bool_t allow_remote_dcs_for_local_cl) {
  if (local_dc == NULL || local_dc_length == 0) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  profile->set_load_balancing_policy(new DCAwarePolicy(
      String(local_dc, local_dc_length), used_hosts_per_remote_dc, !allow_remote_dcs_for_local_cl));
  return CASS_OK;
}

CassError cass_execution_profile_set_token_aware_routing(CassExecProfile* profile,
                                                         cass_bool_t enabled) {
  profile->set_token_aware_routing(enabled == cass_true);
  return CASS_OK;
}

CassError cass_execution_profile_set_token_aware_routing_shuffle_replicas(CassExecProfile* profile,
                                                                          cass_bool_t enabled) {
  profile->set_token_aware_routing_shuffle_replicas(enabled == cass_true);
  return CASS_OK;
}

CassError cass_execution_profile_set_latency_aware_routing(CassExecProfile* profile,
                                                           cass_bool_t enabled) {
  profile->set_latency_aware_routing(enabled == cass_true);
  return CASS_OK;
}

CassError cass_execution_profile_set_latency_aware_routing_settings(
    CassExecProfile* profile, cass_double_t exclusion_threshold, cass_uint64_t scale_ms,
    cass_uint64_t retry_period_ms, cass_uint64_t update_rate_ms, cass_uint64_t min_measured) {
  LatencyAwarePolicy::Settings settings;
  settings.exclusion_threshold = exclusion_threshold;
  settings.scale_ns = scale_ms * 1000 * 1000;
  settings.retry_period_ns = retry_period_ms * 1000 * 1000;
  settings.update_rate_ms = update_rate_ms;
  settings.min_measured = min_measured;
  profile->set_latency_aware_routing_settings(settings);
  return CASS_OK;
}

CassError cass_execution_profile_set_whitelist_filtering(CassExecProfile* profile,
                                                         const char* hosts) {
  return cass_execution_profile_set_whitelist_filtering_n(profile, hosts, SAFE_STRLEN(hosts));
}

CassError cass_execution_profile_set_whitelist_filtering_n(CassExecProfile* profile,
                                                           const char* hosts, size_t hosts_length) {
  if (hosts_length == 0) {
    profile->whitelist().clear();
  } else {
    explode(String(hosts, hosts_length), profile->whitelist());
  }
  return CASS_OK;
}

CassError cass_execution_profile_set_blacklist_filtering(CassExecProfile* profile,
                                                         const char* hosts) {
  return cass_execution_profile_set_blacklist_filtering_n(profile, hosts, SAFE_STRLEN(hosts));
}

CassError cass_execution_profile_set_blacklist_filtering_n(CassExecProfile* profile,
                                                           const char* hosts, size_t hosts_length) {
  if (hosts_length == 0) {
    profile->blacklist().clear();
  } else {
    explode(String(hosts, hosts_length), profile->blacklist());
  }
  return CASS_OK;
}

CassError cass_execution_profile_set_whitelist_dc_filtering(CassExecProfile* profile,
                                                            const char* dcs) {
  return cass_execution_profile_set_whitelist_dc_filtering_n(profile, dcs, SAFE_STRLEN(dcs));
}

CassError cass_execution_profile_set_whitelist_dc_filtering_n(CassExecProfile* profile,
                                                              const char* dcs, size_t dcs_length) {
  if (dcs_length == 0) {
    profile->whitelist_dc().clear();
  } else {
    explode(String(dcs, dcs_length), profile->whitelist_dc());
  }
  return CASS_OK;
}

CassError cass_execution_profile_set_blacklist_dc_filtering(CassExecProfile* profile,
                                                            const char* dcs) {
  return cass_execution_profile_set_blacklist_dc_filtering_n(profile, dcs, SAFE_STRLEN(dcs));
}

CassError cass_execution_profile_set_blacklist_dc_filtering_n(CassExecProfile* profile,
                                                              const char* dcs, size_t dcs_length) {
  if (dcs_length == 0) {
    profile->blacklist_dc().clear();
  } else {
    explode(String(dcs, dcs_length), profile->blacklist_dc());
  }
  return CASS_OK;
}

CassError cass_execution_profile_set_retry_policy(CassExecProfile* profile,
                                                  CassRetryPolicy* retry_policy) {
  profile->set_retry_policy(retry_policy);
  return CASS_OK;
}

CassError cass_execution_profile_set_constant_speculative_execution_policy(
    CassExecProfile* profile, cass_int64_t constant_delay_ms, int max_speculative_executions) {
  if (constant_delay_ms < 0 || max_speculative_executions < 0) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  profile->set_speculative_execution_policy(
      new ConstantSpeculativeExecutionPolicy(constant_delay_ms, max_speculative_executions));
  return CASS_OK;
}

CassError cass_execution_profile_set_no_speculative_execution_policy(CassExecProfile* profile) {
  profile->set_speculative_execution_policy(new NoSpeculativeExecutionPolicy());
  return CASS_OK;
}

} // extern "C"
