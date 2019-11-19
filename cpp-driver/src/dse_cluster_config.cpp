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

#include "cluster_config.hpp"
#include "dse_auth.hpp"

using namespace datastax;
using namespace datastax::internal::core;
using namespace datastax::internal::enterprise;

extern "C" {

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
  cluster->config().set_auth_provider(AuthProvider::Ptr(new DsePlainTextAuthProvider(
      String(username, username_length), String(password, password_length),
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
#ifdef HAVE_KERBEROS
  cluster->config().set_auth_provider(AuthProvider::Ptr(new DseGssapiAuthProvider(
      String(service, service_length), String(principal, principal_length),
      String(authorization_id, authorization_id_length))));
  return CASS_OK;
#else
  return CASS_ERROR_LIB_NOT_IMPLEMENTED;
#endif
}

} // extern "C"
