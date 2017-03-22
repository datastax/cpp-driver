/*
  Copyright (c) 2017 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#include "dse.h"

#include "auth.hpp"

#include <cluster.hpp>

static void dse_plaintext_authenticator_cleanup(void* data) {
  delete static_cast<dse::PlaintextAuthenticatorData*>(data);
}

static void dse_gssapi_authenticator_cleanup(void* data) {
  delete static_cast<dse::GssapiAuthenticatorData*>(data);
}

extern "C" {

CassCluster* cass_cluster_new_dse() {
  CassCluster* cluster = cass_cluster_new();
  cluster->config().set_host_targeting(true);
  return cluster;
}

CassError cass_cluster_set_dse_plaintext_authenticator(CassCluster* cluster,
                                                       const char* username,
                                                       const char* password) {
  return cass_cluster_set_dse_plaintext_authenticator_n(cluster,
                                                        username, strlen(username),
                                                        password, strlen(password));
}

CassError cass_cluster_set_dse_plaintext_authenticator_n(CassCluster* cluster,
                                                         const char* username, size_t username_length,
                                                         const char* password, size_t password_length) {
  return cass_cluster_set_dse_plaintext_authenticator_proxy_n(cluster,
                                                              username, username_length,
                                                              password, password_length,
                                                              "", 0);
}

CassError cass_cluster_set_dse_plaintext_authenticator_proxy(CassCluster* cluster,
                                                             const char* username,
                                                             const char* password,
                                                             const char* authorization_id) {
  return cass_cluster_set_dse_plaintext_authenticator_proxy_n(cluster,
                                                              username, strlen(username),
                                                              password, strlen(password),
                                                              authorization_id, strlen(authorization_id));
}

CassError cass_cluster_set_dse_plaintext_authenticator_proxy_n(CassCluster* cluster,
                                                               const char* username, size_t username_length,
                                                               const char* password, size_t password_length,
                                                               const char* authorization_id, size_t authorization_id_length) {
  return cass_cluster_set_authenticator_callbacks(cluster,
                                                  dse::PlaintextAuthenticatorData::callbacks(),
                                                  dse_plaintext_authenticator_cleanup,
                                                  new dse::PlaintextAuthenticatorData(std::string(username, username_length),
                                                                                      std::string(password, password_length),
                                                                                      std::string(authorization_id, authorization_id_length)));
}

CassError cass_cluster_set_dse_gssapi_authenticator(CassCluster* cluster,
                                                    const char* service,
                                                    const char* principal) {
  return cass_cluster_set_dse_gssapi_authenticator_n(cluster,
                                                     service, strlen(service),
                                                     principal, strlen(principal));
}

CassError cass_cluster_set_dse_gssapi_authenticator_n(CassCluster* cluster,
                                                      const char* service, size_t service_length,
                                                      const char* principal, size_t principal_length) {
  return cass_cluster_set_dse_gssapi_authenticator_proxy_n(cluster,
                                                           service, service_length,
                                                           principal, principal_length,
                                                           "", 0);
}

CassError cass_cluster_set_dse_gssapi_authenticator_proxy(CassCluster* cluster,
                                                          const char* service,
                                                          const char* principal,
                                                          const char* authorization_id) {
  return cass_cluster_set_dse_gssapi_authenticator_proxy_n(cluster,
                                                           service, strlen(service),
                                                           principal, strlen(principal),
                                                           authorization_id, strlen(authorization_id));
}

CassError cass_cluster_set_dse_gssapi_authenticator_proxy_n(CassCluster* cluster,
                                                            const char* service, size_t service_length,
                                                            const char* principal, size_t principal_length,
                                                            const char* authorization_id, size_t authorization_id_length) {
  return cass_cluster_set_authenticator_callbacks(cluster,
                                                  dse::GssapiAuthenticatorData::callbacks(),
                                                  dse_gssapi_authenticator_cleanup,
                                                  new dse::GssapiAuthenticatorData(std::string(service, service_length),
                                                                                   std::string(principal, principal_length),
                                                                                   std::string(authorization_id, authorization_id_length)));
}

} // extern "C"
