/*
  Copyright (c) DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#include "dse.h"

#include "auth.hpp"
#include "cluster_config.hpp"
#include "string.hpp"

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
                                                        username, SAFE_STRLEN(username),
                                                        password, SAFE_STRLEN(password));
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
                                                              username, SAFE_STRLEN(username),
                                                              password, SAFE_STRLEN(password),
                                                              authorization_id, SAFE_STRLEN(authorization_id));
}

CassError cass_cluster_set_dse_plaintext_authenticator_proxy_n(CassCluster* cluster,
                                                               const char* username, size_t username_length,
                                                               const char* password, size_t password_length,
                                                               const char* authorization_id, size_t authorization_id_length) {
  return cass_cluster_set_authenticator_callbacks(cluster,
                                                  dse::PlaintextAuthenticatorData::callbacks(),
                                                  dse_plaintext_authenticator_cleanup,
                                                  new dse::PlaintextAuthenticatorData(cass::String(username, username_length),
                                                                                      cass::String(password, password_length),
                                                                                      cass::String(authorization_id, authorization_id_length)));
}

CassError cass_cluster_set_dse_gssapi_authenticator(CassCluster* cluster,
                                                    const char* service,
                                                    const char* principal) {
  return cass_cluster_set_dse_gssapi_authenticator_n(cluster,
                                                     service, SAFE_STRLEN(service),
                                                     principal, SAFE_STRLEN(principal));
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
                                                           service, SAFE_STRLEN(service),
                                                           principal, SAFE_STRLEN(principal),
                                                           authorization_id, SAFE_STRLEN(authorization_id));
}

CassError cass_cluster_set_dse_gssapi_authenticator_proxy_n(CassCluster* cluster,
                                                            const char* service, size_t service_length,
                                                            const char* principal, size_t principal_length,
                                                            const char* authorization_id, size_t authorization_id_length) {
  return cass_cluster_set_authenticator_callbacks(cluster,
                                                  dse::GssapiAuthenticatorData::callbacks(),
                                                  dse_gssapi_authenticator_cleanup,
                                                  new dse::GssapiAuthenticatorData(cass::String(service, service_length),
                                                                                   cass::String(principal, principal_length),
                                                                                   cass::String(authorization_id, authorization_id_length)));
}

void cass_cluster_set_application_name(CassCluster* cluster,
                                       const char* application_name) {
  cass_cluster_set_application_name_n(cluster,
                                      application_name, SAFE_STRLEN(application_name));
}

void cass_cluster_set_application_name_n(CassCluster* cluster,
                                         const char* application_name,
                                         size_t application_name_length) {
  cluster->config().set_application_name(cass::String(application_name,
                                                      application_name_length));
}

void cass_cluster_set_application_version(CassCluster* cluster,
                                          const char* application_version) {
  cass_cluster_set_application_version_n(cluster,
                                         application_version, SAFE_STRLEN(application_version));
}

void cass_cluster_set_application_version_n(CassCluster* cluster,
                                            const char* application_version,
                                            size_t application_version_length) {
  cluster->config().set_application_version(cass::String(application_version,
                                                         application_version_length));
}


} // extern "C"
