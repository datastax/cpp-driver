/*
  Copyright (c) 2014 DataStax

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

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <uv.h>

#include "cassandra.h"

void print_error(const char* message, int err) {
  printf("%s: %s (%d)\n", message, cass_error_desc(err), err);
}

/* Example round-robin LB policy */

CassHostDistance rr_host_distance(CassLoadBalancingPolicy* policy, const CassHost* host);
const char* rr_next_host(CassLoadBalancingPolicy* policy, int is_initial);

typedef struct {
  size_t index;
  size_t next_host_index;
  size_t remaining;
} RoundRobinPolicy;

RoundRobinPolicy* rr_policy_new();
void rr_policy_free(RoundRobinPolicy* rr_policy);

CassLoadBalancingPolicyImpl rr_policy_impl = {
  NULL,
  &rr_host_distance,
  &rr_next_host
};

CassHostDistance rr_host_distance(CassLoadBalancingPolicy* policy, const CassHost* host) {
  return CASS_HOST_DISTANCE_LOCAL;
}

const char* rr_next_host(CassLoadBalancingPolicy* policy, int is_initial) {
  RoundRobinPolicy* rr_policy = (RoundRobinPolicy*)cass_lb_policy_get_data(policy);
  
  size_t hosts_count = cass_lb_policy_hosts_count(policy);

  if(is_initial) {
    rr_policy->remaining = hosts_count;
    rr_policy->next_host_index = rr_policy->index++;
  }

  if(rr_policy->remaining != 0) {
    CassHost* host = cass_lb_policy_get_host(policy, rr_policy->next_host_index++ % hosts_count);
    rr_policy->remaining--;
    return cass_host_get_address(host);
  }

  return NULL;
}

RoundRobinPolicy* rr_policy_new() {
  RoundRobinPolicy* rr_policy = (RoundRobinPolicy*)malloc(sizeof(RoundRobinPolicy));
  rr_policy->index = 0;
  return rr_policy;
}

void rr_policy_free(RoundRobinPolicy* rr_policy) {
  free(rr_policy);
}

int
main() {
  CassSession* session = cass_session_new();
  CassFuture* session_future = NULL;
  /*CassFuture* shutdown_future = NULL;*/
  int err;

  const char* cp1 = "127.0.0.2";
  const char* cp2 = "localhost";

  cass_session_setopt(session, CASS_OPTION_CONTACT_POINT_ADD, cp1, strlen(cp1));
  cass_session_setopt(session, CASS_OPTION_CONTACT_POINT_ADD, cp2, strlen(cp2));

  err = cass_session_connect(session, &session_future);
  if (err != 0) {
    print_error("Error creating session", err);
    goto cleanup;
  }

  cass_future_wait(session_future);
  cass_future_free(session_future);

/*
  session = cass_future_get_session(session_future);

  err = cass_session_shutdown(session, &shutdown_future);
  if(err != 0) {
    print_error("Error on shutdown", err);
    goto cleanup;
  }

  cass_future_wait(shutdown_future);
  cass_future_free(shutdown_future);

*/
cleanup:
  cass_session_free(session);
}
