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

#include "dse.h"

#include "auth.hpp"
#include "cluster_config.hpp"
#include "dse_auth.hpp"
#include "string.hpp"

using namespace datastax;
using namespace datastax::internal::enterprise;

static void dse_gssapi_authenticator_cleanup(void* data) {
  delete static_cast<GssapiAuthenticatorData*>(data);
}

extern "C" {

CassCluster* cass_cluster_new_dse() {
  CassCluster* cluster = cass_cluster_new();
  cluster->config().set_host_targeting(true);
  return cluster;
}

CassError cass_cluster_set_dse_plaintext_authenticator(CassCluster* cluster, const char* username,
                                                       const char* password) {
  return cass_cluster_set_dse_plaintext_authenticator_n(cluster, username, SAFE_STRLEN(username),
                                                        password, SAFE_STRLEN(password));
}

CassError cass_cluster_set_dse_plaintext_authenticator_n(CassCluster* cluster, const char* username,
                                                         size_t username_length,
                                                         const char* password,
                                                         size_t password_length) {
  return cass_cluster_set_dse_plaintext_authenticator_proxy_n(cluster, username, username_length,
                                                              password, password_length, "", 0);
}

CassError cass_cluster_set_dse_plaintext_authenticator_proxy(CassCluster* cluster,
                                                             const char* username,
                                                             const char* password,
                                                             const char* authorization_id) {
  return cass_cluster_set_dse_plaintext_authenticator_proxy_n(
      cluster, username, SAFE_STRLEN(username), password, SAFE_STRLEN(password), authorization_id,
      SAFE_STRLEN(authorization_id));
}

CassError cass_cluster_set_dse_plaintext_authenticator_proxy_n(
    CassCluster* cluster, const char* username, size_t username_length, const char* password,
    size_t password_length, const char* authorization_id, size_t authorization_id_length) {
  cluster->config().set_auth_provider(internal::core::AuthProvider::Ptr(
      new DsePlainTextAuthProvider(String(username, username_length),
                                   String(password, password_length),
                                   String(authorization_id, authorization_id_length))));
  return CASS_OK;
}

CassError cass_cluster_set_dse_gssapi_authenticator(CassCluster* cluster, const char* service,
                                                    const char* principal) {
  return cass_cluster_set_dse_gssapi_authenticator_n(cluster, service, SAFE_STRLEN(service),
                                                     principal, SAFE_STRLEN(principal));
}

CassError cass_cluster_set_dse_gssapi_authenticator_n(CassCluster* cluster, const char* service,
                                                      size_t service_length, const char* principal,
                                                      size_t principal_length) {
  return cass_cluster_set_dse_gssapi_authenticator_proxy_n(cluster, service, service_length,
                                                           principal, principal_length, "", 0);
}

CassError cass_cluster_set_dse_gssapi_authenticator_proxy(CassCluster* cluster, const char* service,
                                                          const char* principal,
                                                          const char* authorization_id) {
  return cass_cluster_set_dse_gssapi_authenticator_proxy_n(
      cluster, service, SAFE_STRLEN(service), principal, SAFE_STRLEN(principal), authorization_id,
      SAFE_STRLEN(authorization_id));
}

CassError cass_cluster_set_dse_gssapi_authenticator_proxy_n(
    CassCluster* cluster, const char* service, size_t service_length, const char* principal,
    size_t principal_length, const char* authorization_id, size_t authorization_id_length) {
  CassError rc = cass_cluster_set_authenticator_callbacks(
      cluster, GssapiAuthenticatorData::callbacks(), dse_gssapi_authenticator_cleanup,
      new GssapiAuthenticatorData(String(service, service_length),
                                  String(principal, principal_length),
                                  String(authorization_id, authorization_id_length)));
  if (rc == CASS_OK) {
    String name = "DSEGSSAPIAuthProvider";
    if (authorization_id_length > 0) {
      name.append(" (Proxy)");
    }
    cluster->config().auth_provider()->set_name(name);
  }

  return rc;
}

void cass_cluster_set_application_name(CassCluster* cluster, const char* application_name) {
  cass_cluster_set_application_name_n(cluster, application_name, SAFE_STRLEN(application_name));
}

void cass_cluster_set_application_name_n(CassCluster* cluster, const char* application_name,
                                         size_t application_name_length) {
  cluster->config().set_application_name(String(application_name, application_name_length));
}

void cass_cluster_set_application_version(CassCluster* cluster, const char* application_version) {
  cass_cluster_set_application_version_n(cluster, application_version,
                                         SAFE_STRLEN(application_version));
}

void cass_cluster_set_application_version_n(CassCluster* cluster, const char* application_version,
                                            size_t application_version_length) {
  cluster->config().set_application_version(
      String(application_version, application_version_length));
}

void cass_cluster_set_client_id(CassCluster* cluster, CassUuid client_id) {
  cluster->config().set_client_id(client_id);
}

void cass_cluster_set_monitor_reporting_interval(CassCluster* cluster, unsigned interval_secs) {
  cluster->config().set_monitor_reporting_interval_secs(interval_secs);
}

} // extern "C"
