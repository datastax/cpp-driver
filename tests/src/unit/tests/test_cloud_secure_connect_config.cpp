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

#include "driver_config.hpp"

#ifdef HAVE_ZLIB
#include "http_test.hpp"

#include "cloud_secure_connection_config.hpp"
#include "cluster_config.hpp"
#include "cluster_connector.hpp"
#include "cluster_metadata_resolver.hpp"
#include "config.hpp"
#include "dse_auth.hpp"
#include "http_client.hpp"
#include "json.hpp"
#include "string.hpp"

#include "zip.h"

#include <time.h>
#include <uv.h>

#define CONFIGURATION_FILE "config.json"
#define CERTIFICATE_AUTHORITY_FILE "ca.crt"
#define CERTIFICATE_FILE "cert"
#define KEY_FILE "key"

#define CREDS_V1_ZIP_FILE "creds-v1.zip"

#ifdef _WIN32
#define PATH_SEPARATOR '\\'
#else
#define PATH_SEPARATOR '/'
#endif

#define SNI_LOCAL_DC "dc1"
#define SNI_HOST HTTP_MOCK_HOSTNAME
#define SNI_PORT 30002
#define SNI_HOST_AND_PORT HTTP_MOCK_HOSTNAME ":30002"
#define SNI_HOST_ID_1 "276b1694-64c4-4ba8-afb4-e33915a02f1e"
#define SNI_HOST_ID_2 "8c29f723-5c1c-4ffd-a4ef-8c683a7fc02b"
#define SNI_HOST_ID_3 "fb91d3ff-47cb-447d-b31d-c5721ca8d7ab"
#define METADATA_SERVICE_PORT 30443

using datastax::String;
using datastax::internal::core::AddressVec;
using datastax::internal::core::CloudSecureConnectionConfig;
using datastax::internal::core::ClusterConfig;
using datastax::internal::core::ClusterMetadataResolver;
using datastax::internal::core::ClusterSettings;
using datastax::internal::core::Config;
using datastax::internal::core::HttpClient;
using datastax::internal::core::SslContext;
using datastax::internal::core::SslContextFactory;
using datastax::internal::enterprise::DsePlainTextAuthProvider;
using datastax::internal::json::StringBuffer;
using datastax::internal::json::Writer;

using mockssandra::Ssl;

class CloudSecureConnectionConfigTest : public HttpTest {
public:
  const String& ca_cert() const { return ca_cert_; }
  void set_invalid_ca_cert() { ca_cert_ = "!!!!!INVALID!!!!!"; }
  const String& ca_key() const { return ca_key_; }
  const String& cert() const { return cert_; }
  void set_invalid_cert() { cert_ = "!!!!!INVALID!!!!!"; }
  const String& key() const { return key_; }
  void set_invalid_key() { key_ = "!!!!!INVALID!!!!!"; }

  void SetUp() {
    HttpTest::SetUp();

    char tmp[260] = { 0 }; // Note: 260 is the maximum path on Windows
    size_t tmp_length = 260;
    uv_os_tmpdir(tmp, &tmp_length);

    tmp_zip_file_ = String(tmp, tmp_length) + PATH_SEPARATOR + CREDS_V1_ZIP_FILE;

    ca_key_ = Ssl::generate_key();
    ca_cert_ = Ssl::generate_cert(ca_key_, "CA");
    key_ = Ssl::generate_key();
    cert_ = Ssl::generate_cert(key_, "", ca_cert_, ca_key_);
  }

  const String& creds_zip_file() const { return tmp_zip_file_; }

  void create_zip_file(const String& config, bool is_configuration = true, bool is_ca = true,
                       bool is_cert = true, bool is_key = true) {
    zipFile zip_file = zipOpen64(tmp_zip_file_.c_str(), 0);

    if (is_configuration && add_zip_file_entry(zip_file, CONFIGURATION_FILE)) {
      zipWriteInFileInZip(zip_file, config.c_str(), config.length());
      zipCloseFileInZip(zip_file);
    }
    if (is_ca && add_zip_file_entry(zip_file, CERTIFICATE_AUTHORITY_FILE)) {
      zipWriteInFileInZip(zip_file, ca_cert_.c_str(), ca_cert_.length());
      zipCloseFileInZip(zip_file);
    }
    if (is_cert && add_zip_file_entry(zip_file, CERTIFICATE_FILE)) {
      zipWriteInFileInZip(zip_file, cert_.c_str(), cert_.length());
      zipCloseFileInZip(zip_file);
    }
    if (is_key && add_zip_file_entry(zip_file, KEY_FILE)) {
      zipWriteInFileInZip(zip_file, key_.c_str(), key_.length());
      zipCloseFileInZip(zip_file);
    }

    zipClose(zip_file, NULL);
  }

  static void full_config_credsv1(StringBuffer& buffer, String host = "cloud.datastax.com",
                                  int port = 1443) {
    Writer<StringBuffer> writer(buffer);
    writer.StartObject();
    writer.Key("username");
    writer.String("DataStax");
    writer.Key("password");
    writer.String("Astra");
    writer.Key("host");
    writer.String(host.c_str());
    writer.Key("port");
    writer.Int(port);
    writer.EndObject();
  }

private:
  bool add_zip_file_entry(zipFile zip_file, const String& zip_filename) {
    zip_fileinfo file_info;
    memset(&file_info, 0, sizeof(file_info));
    time_t tmp;
    time(&tmp);
    struct tm* time_info = localtime(&tmp);
    file_info.tmz_date.tm_sec = time_info->tm_sec;
    file_info.tmz_date.tm_min = time_info->tm_min;
    file_info.tmz_date.tm_hour = time_info->tm_hour;
    file_info.tmz_date.tm_mday = time_info->tm_mday;
    file_info.tmz_date.tm_mon = time_info->tm_mon;
    file_info.tmz_date.tm_year = time_info->tm_year;

    int rc = zipOpenNewFileInZip(zip_file, zip_filename.c_str(), &file_info, NULL, 0, NULL, 0, NULL,
                                 Z_DEFLATED, Z_DEFAULT_COMPRESSION);
    return rc == ZIP_OK;
  }

private:
  String tmp_zip_file_;
  String ca_cert_;
  String ca_key_;
  String cert_;
  String key_;
};

TEST_F(CloudSecureConnectionConfigTest, CredsV1) {
  Config config;
  CloudSecureConnectionConfig cloud_config;

  StringBuffer buffer;
  full_config_credsv1(buffer);
  create_zip_file(buffer.GetString());

  EXPECT_TRUE(cloud_config.load(creds_zip_file(), &config));
  EXPECT_EQ("DataStax", cloud_config.username());
  EXPECT_EQ("Astra", cloud_config.password());
  EXPECT_EQ("cloud.datastax.com", cloud_config.host());
  EXPECT_EQ(1443, cloud_config.port());
  EXPECT_EQ(ca_cert(), cloud_config.ca_cert());
  EXPECT_EQ(cert(), cloud_config.cert());
  EXPECT_EQ(key(), cloud_config.key());

  EXPECT_TRUE(config.ssl_context());
  EXPECT_TRUE(dynamic_cast<DsePlainTextAuthProvider*>(config.auth_provider().get()) != NULL);
}

TEST_F(CloudSecureConnectionConfigTest, CredsV1WithoutCreds) {
  Config config;
  CloudSecureConnectionConfig cloud_config;

  StringBuffer buffer;
  Writer<StringBuffer> writer(buffer);
  writer.StartObject();
  writer.Key("host");
  writer.String("bigdata.datastax.com");
  writer.Key("port");
  writer.Int(2443);
  writer.EndObject();
  create_zip_file(buffer.GetString());

  EXPECT_TRUE(cloud_config.load(creds_zip_file(), &config));
  EXPECT_EQ("", cloud_config.username());
  EXPECT_EQ("", cloud_config.password());
  EXPECT_EQ("bigdata.datastax.com", cloud_config.host());
  EXPECT_EQ(2443, cloud_config.port());
  EXPECT_EQ(ca_cert(), cloud_config.ca_cert());
  EXPECT_EQ(cert(), cloud_config.cert());
  EXPECT_EQ(key(), cloud_config.key());

  EXPECT_TRUE(config.ssl_context());
  EXPECT_TRUE(dynamic_cast<DsePlainTextAuthProvider*>(config.auth_provider().get()) ==
              NULL); // Not configured
}

TEST_F(CloudSecureConnectionConfigTest, InvalidCredsV1ConfigMissingHost) {
  CloudSecureConnectionConfig config;

  StringBuffer buffer;
  Writer<StringBuffer> writer(buffer);
  writer.StartObject();
  writer.Key("username");
  writer.String("DataStax");
  writer.Key("password");
  writer.String("Astra");
  writer.Key("port");
  writer.Int(1443);
  writer.EndObject();
  create_zip_file(buffer.GetString());

  EXPECT_FALSE(config.load(creds_zip_file()));
}

TEST_F(CloudSecureConnectionConfigTest, InvalidCredsV1ConfigMissingPort) {
  CloudSecureConnectionConfig config;

  StringBuffer buffer;
  Writer<StringBuffer> writer(buffer);
  writer.StartObject();
  writer.Key("username");
  writer.String("DataStax");
  writer.Key("password");
  writer.String("Astra");
  writer.Key("host");
  writer.String("cloud.datastax.com");
  writer.EndObject();
  create_zip_file(buffer.GetString());

  EXPECT_FALSE(config.load(creds_zip_file()));
}

TEST_F(CloudSecureConnectionConfigTest, InvalidCredsMissingZipFile) {
  CloudSecureConnectionConfig config;

  EXPECT_FALSE(config.load("invalid.zip"));
}

TEST_F(CloudSecureConnectionConfigTest, InvalidCredsV1MissingConfigJson) {
  CloudSecureConnectionConfig config;

  create_zip_file("", false);
  EXPECT_FALSE(config.load(creds_zip_file()));
}

TEST_F(CloudSecureConnectionConfigTest, InvalidCredsV1MissingCA) {
  CloudSecureConnectionConfig config;

  StringBuffer buffer;
  full_config_credsv1(buffer);
  create_zip_file(buffer.GetString(), true, false);
  EXPECT_FALSE(config.load(creds_zip_file()));
}

TEST_F(CloudSecureConnectionConfigTest, InvalidCredsV1MissingCert) {
  CloudSecureConnectionConfig config;

  StringBuffer buffer;
  full_config_credsv1(buffer);
  create_zip_file(buffer.GetString(), true, true, false);
  EXPECT_FALSE(config.load(creds_zip_file()));
}

TEST_F(CloudSecureConnectionConfigTest, InvalidCredsV1MissingKey) {
  CloudSecureConnectionConfig config;

  StringBuffer buffer;
  full_config_credsv1(buffer);
  create_zip_file(buffer.GetString(), true, true, false);
  create_zip_file(buffer.GetString(), true, true, true, false);
  EXPECT_FALSE(config.load(creds_zip_file()));
}

TEST_F(CloudSecureConnectionConfigTest, InvalidCredsV1SslCaCert) {
  Config config;
  CloudSecureConnectionConfig cloud_config;

  StringBuffer buffer;
  full_config_credsv1(buffer);
  set_invalid_ca_cert();
  create_zip_file(buffer.GetString());

  EXPECT_FALSE(cloud_config.load(creds_zip_file(), &config));
  EXPECT_FALSE(config.ssl_context());
}

TEST_F(CloudSecureConnectionConfigTest, InvalidCredsV1SslCert) {
  Config config;
  CloudSecureConnectionConfig cloud_config;

  StringBuffer buffer;
  full_config_credsv1(buffer);
  set_invalid_cert();
  create_zip_file(buffer.GetString());

  EXPECT_FALSE(cloud_config.load(creds_zip_file(), &config));
  EXPECT_FALSE(config.ssl_context());
}

TEST_F(CloudSecureConnectionConfigTest, InvalidCredsV1SslKey) {
  Config config;
  CloudSecureConnectionConfig cloud_config;

  StringBuffer buffer;
  full_config_credsv1(buffer);
  set_invalid_key();
  create_zip_file(buffer.GetString());

  EXPECT_FALSE(cloud_config.load(creds_zip_file(), &config));
  EXPECT_FALSE(config.ssl_context());
}

class CloudMetadataServerTest : public CloudSecureConnectionConfigTest {
public:
  void SetUp() {
    CloudSecureConnectionConfigTest::SetUp();

    StringBuffer buffer;
    full_config_credsv1(buffer, HTTP_MOCK_HOSTNAME, HTTP_MOCK_SERVER_PORT);
    create_zip_file(buffer.GetString());
    cloud_config_.load(creds_zip_file(), &config_);

    use_ssl(ca_key(), ca_cert(), HTTP_MOCK_HOSTNAME); // Ensure HttpServer is configured to use SSL

    ClusterSettings settings(config_);
    resolver_ = config_.cluster_metadata_resolver_factory()->new_instance(settings);
  }

  void start_http_server(bool is_content_type = true, bool is_contact_info = true,
                         bool is_local_dc = true, bool is_contact_points = true,
                         bool is_sni_proxy_address = true, bool is_port = true) {
    set_path("/metadata");

    StringBuffer buffer;
    response_v1(buffer, is_contact_info, is_local_dc, is_contact_points, is_sni_proxy_address,
                is_port);
    set_response_body(buffer.GetString());

    set_content_type(is_content_type ? response_v1_content_type() : "invalid");

    HttpTest::start_http_server();
  }

  const ClusterMetadataResolver::Ptr& resolver() const { return resolver_; }

  static void on_resolve_success(ClusterMetadataResolver* resolver, bool* flag) {
    *flag = true;
    EXPECT_EQ("dc1", resolver->local_dc());

    const AddressVec& contact_points = resolver->resolved_contact_points();
    ASSERT_EQ(3u, contact_points.size());
    EXPECT_EQ(Address(SNI_HOST, SNI_PORT, SNI_HOST_ID_1), contact_points[0]);
    EXPECT_EQ(Address(SNI_HOST, SNI_PORT, SNI_HOST_ID_2), contact_points[1]);
    EXPECT_EQ(Address(SNI_HOST, SNI_PORT, SNI_HOST_ID_3), contact_points[2]);
  }

  static void on_resolve_success_default_port(ClusterMetadataResolver* resolver, bool* flag) {
    *flag = true;
    EXPECT_EQ("dc1", resolver->local_dc());

    const AddressVec& contact_points = resolver->resolved_contact_points();
    ASSERT_EQ(3u, contact_points.size());
    EXPECT_EQ(Address(SNI_HOST, METADATA_SERVICE_PORT, SNI_HOST_ID_1), contact_points[0]);
    EXPECT_EQ(Address(SNI_HOST, METADATA_SERVICE_PORT, SNI_HOST_ID_2), contact_points[1]);
    EXPECT_EQ(Address(SNI_HOST, METADATA_SERVICE_PORT, SNI_HOST_ID_3), contact_points[2]);
  }

  static void on_resolve_failed(ClusterMetadataResolver* resolver, bool* flag) {
    *flag = true;
    EXPECT_EQ(0u, resolver->resolved_contact_points().size());
  }

  static void on_resolve_local_dc_failed(ClusterMetadataResolver* resolver, bool* flag) {
    *flag = true;
    EXPECT_EQ("", resolver->local_dc());
    EXPECT_EQ(0u, resolver->resolved_contact_points().size());
  }

private:
  static void response_v1(StringBuffer& buffer, bool is_contact_info = true,
                          bool is_local_dc = true, bool is_contact_points = true,
                          bool is_sni_proxy_address = true, bool is_port = true) {
    Writer<StringBuffer> writer(buffer);
    writer.StartObject();
    writer.Key("version");
    writer.Int(1);
    writer.Key("region");
    writer.String("local");
    if (is_contact_info) {
      writer.Key("contact_info");
      writer.StartObject();
      writer.Key("type");
      writer.String("sni_proxy");
      if (is_local_dc) {
        writer.Key("local_dc");
        writer.String(SNI_LOCAL_DC);
      }
      if (is_contact_points) {
        writer.Key("contact_points");
        writer.StartArray();
        writer.String(SNI_HOST_ID_1);
        writer.String(SNI_HOST_ID_2);
        writer.String(SNI_HOST_ID_3);
        writer.EndArray();
      }
      if (is_sni_proxy_address) {
        writer.Key("sni_proxy_address");
        if (is_port) {
          writer.String(SNI_HOST_AND_PORT);
        } else {
          writer.String(SNI_HOST);
        }
      }
      writer.EndObject();
    }
    writer.EndObject();
  }

  static const char* response_v1_content_type() { return "application/json"; }

private:
  Config config_;
  CloudSecureConnectionConfig cloud_config_;
  ClusterMetadataResolver::Ptr resolver_;
};

TEST_F(CloudMetadataServerTest, ResolveV1StandardSsl) {
  start_http_server();

  bool is_resolved = false;
  AddressVec contact_points;
  resolver()->resolve(loop(), contact_points, bind_callback(on_resolve_success, &is_resolved));
  uv_run(loop(), UV_RUN_DEFAULT);
  EXPECT_TRUE(is_resolved);

  stop_http_server();
}

TEST_F(CloudMetadataServerTest, ResolveV1DefaultPortSsl) {
  start_http_server(true, true, true, true, true, false);

  bool is_resolved = false;
  AddressVec contact_points;
  resolver()->resolve(loop(), contact_points,
                      bind_callback(on_resolve_success_default_port, &is_resolved));
  uv_run(loop(), UV_RUN_DEFAULT);
  EXPECT_TRUE(is_resolved);

  stop_http_server();
}

TEST_F(CloudMetadataServerTest, InvalidMetadataServer) {
  bool is_resolved = false;
  AddressVec contact_points;
  resolver()->resolve(loop(), contact_points, bind_callback(on_resolve_failed, &is_resolved));
  uv_run(loop(), UV_RUN_DEFAULT);
  EXPECT_TRUE(is_resolved);
}

TEST_F(CloudMetadataServerTest, ResolveV1InvalidContentTypeSsl) {
  start_http_server(false);

  bool is_resolved = false;
  AddressVec contact_points;
  resolver()->resolve(loop(), contact_points, bind_callback(on_resolve_failed, &is_resolved));
  uv_run(loop(), UV_RUN_DEFAULT);
  EXPECT_TRUE(is_resolved);

  stop_http_server();
}

TEST_F(CloudMetadataServerTest, ResolveV1MissingContactInfoSsl) {
  start_http_server(true, false);

  bool is_resolved = false;
  AddressVec contact_points;
  resolver()->resolve(loop(), contact_points, bind_callback(on_resolve_failed, &is_resolved));
  uv_run(loop(), UV_RUN_DEFAULT);
  EXPECT_TRUE(is_resolved);

  stop_http_server();
}

TEST_F(CloudMetadataServerTest, ResolveV1MissingLocalDcSsl) {
  start_http_server(true, true, false);

  bool is_resolved = false;
  AddressVec contact_points;
  resolver()->resolve(loop(), contact_points,
                      bind_callback(on_resolve_local_dc_failed, &is_resolved));
  uv_run(loop(), UV_RUN_DEFAULT);
  EXPECT_TRUE(is_resolved);

  stop_http_server();
}

TEST_F(CloudMetadataServerTest, ResolveV1MissingContactPointsSsl) {
  start_http_server(true, true, true, false);

  bool is_resolved = false;
  AddressVec contact_points;
  resolver()->resolve(loop(), contact_points, bind_callback(on_resolve_failed, &is_resolved));
  uv_run(loop(), UV_RUN_DEFAULT);
  EXPECT_TRUE(is_resolved);

  stop_http_server();
}

TEST_F(CloudMetadataServerTest, ResolveV1MissingSniProxyAddressSsl) {
  start_http_server(true, true, true, true, false);

  bool is_resolved = false;
  AddressVec contact_points;
  resolver()->resolve(loop(), contact_points, bind_callback(on_resolve_failed, &is_resolved));
  uv_run(loop(), UV_RUN_DEFAULT);
  EXPECT_TRUE(is_resolved);

  stop_http_server();
}

TEST_F(CloudMetadataServerTest, ResolveInvalidJsonResponse) {
  add_logging_critera("Unable to configure driver from metadata server: Metadata JSON is invalid");

  set_path("/metadata");
  set_response_body("[]");
  set_content_type("application/json");
  HttpTest::start_http_server();

  bool is_resolve_failed = false;
  AddressVec contact_points;
  resolver()->resolve(loop(), contact_points, bind_callback(on_resolve_failed, &is_resolve_failed));
  uv_run(loop(), UV_RUN_DEFAULT);
  EXPECT_TRUE(is_resolve_failed);
  EXPECT_EQ(logging_criteria_count(), 1);

  stop_http_server();
}

TEST_F(CloudMetadataServerTest, ResolveErrorResponse) {
  add_logging_critera("Unable to configure driver from metadata server: Returned error response "
                      "code 400: 'Invalid version'");

  const char* response_body = "{"
                              "\"code\": 400,"
                              "\"message\": \"Invalid version\""
                              "}";

  set_path("/metadata");
  set_response_body(response_body);
  set_response_status_code(400);
  set_content_type("application/json");
  HttpTest::start_http_server();

  bool is_resolve_failed = false;
  AddressVec contact_points;
  resolver()->resolve(loop(), contact_points, bind_callback(on_resolve_failed, &is_resolve_failed));
  uv_run(loop(), UV_RUN_DEFAULT);
  EXPECT_TRUE(is_resolve_failed);
  EXPECT_EQ(logging_criteria_count(), 1);

  stop_http_server();
}

TEST_F(CloudMetadataServerTest, ResolveInvalidJsonErrorResponse) {
  add_logging_critera("Unable to configure driver from metadata server: Returned error response "
                      "code 400: '[]'");

  set_path("/metadata");
  set_response_body("[]");
  set_response_status_code(400);
  set_content_type("application/json");
  HttpTest::start_http_server();

  bool is_resolve_failed = false;
  AddressVec contact_points;
  resolver()->resolve(loop(), contact_points, bind_callback(on_resolve_failed, &is_resolve_failed));
  uv_run(loop(), UV_RUN_DEFAULT);
  EXPECT_TRUE(is_resolve_failed);
  EXPECT_EQ(logging_criteria_count(), 1);

  stop_http_server();
}

TEST_F(CloudMetadataServerTest, CloudConfiguredInvalidContactPointsOverride) {
  StringBuffer buffer;
  full_config_credsv1(buffer);
  create_zip_file(buffer.GetString());

  ClusterConfig cluster_config;
  CassCluster* cluster = CassCluster::to(&cluster_config);
  EXPECT_EQ(CASS_OK, cass_cluster_set_cloud_secure_connection_bundle_no_ssl_lib_init(
                         cluster, creds_zip_file().c_str()));
  add_logging_critera("Contact points cannot be overridden with cloud secure connection bundle");
  EXPECT_EQ(CASS_ERROR_LIB_BAD_PARAMS,
            cass_cluster_set_contact_points(cluster, "some.contact.point"));
  EXPECT_EQ(logging_criteria_count(), 1);
}

TEST_F(CloudMetadataServerTest, CloudConfiguredInvalidSslContextOverride) {
  StringBuffer buffer;
  full_config_credsv1(buffer);
  create_zip_file(buffer.GetString());

  ClusterConfig cluster_config;
  CassCluster* cluster = CassCluster::to(&cluster_config);
  SslContext::Ptr ssl_context(SslContextFactory::create());
  CassSsl* ssl = CassSsl::to(ssl_context.get());

  EXPECT_EQ(CASS_OK, cass_cluster_set_cloud_secure_connection_bundle_no_ssl_lib_init(
                         cluster, creds_zip_file().c_str()));
  add_logging_critera("SSL context cannot be overridden with cloud secure connection bundle");
  cass_cluster_set_ssl(cluster, ssl);
  EXPECT_EQ(logging_criteria_count(), 1);
}

TEST_F(CloudMetadataServerTest, CloudConfiguredFailureContactPointsExist) {
  StringBuffer buffer;
  full_config_credsv1(buffer);
  create_zip_file(buffer.GetString());

  ClusterConfig cluster_config;
  CassCluster* cluster = CassCluster::to(&cluster_config);
  EXPECT_EQ(CASS_OK, cass_cluster_set_contact_points(cluster, "some.contact.point"));
  add_logging_critera("Contact points must not be specified with cloud secure connection bundle");
  EXPECT_EQ(CASS_ERROR_LIB_BAD_PARAMS,
            cass_cluster_set_cloud_secure_connection_bundle_no_ssl_lib_init(
                cluster, creds_zip_file().c_str()));
  EXPECT_EQ(logging_criteria_count(), 1);
}

TEST_F(CloudMetadataServerTest, CloudConfiguredFailureSslContextExist) {
  StringBuffer buffer;
  full_config_credsv1(buffer);
  create_zip_file(buffer.GetString());

  ClusterConfig cluster_config;
  CassCluster* cluster = CassCluster::to(&cluster_config);
  SslContext::Ptr ssl_context(SslContextFactory::create());
  CassSsl* ssl = CassSsl::to(ssl_context.get());

  cass_cluster_set_ssl(cluster, ssl);
  add_logging_critera("SSL context must not be specified with cloud secure connection bundle");
  EXPECT_EQ(CASS_ERROR_LIB_BAD_PARAMS,
            cass_cluster_set_cloud_secure_connection_bundle_no_ssl_lib_init(
                cluster, creds_zip_file().c_str()));
  EXPECT_EQ(logging_criteria_count(), 1);
}

TEST_F(CloudMetadataServerTest, CloudConfiguredFailureContactPointsAndSslContextExist) {
  StringBuffer buffer;
  full_config_credsv1(buffer);
  create_zip_file(buffer.GetString());

  ClusterConfig cluster_config;
  CassCluster* cluster = CassCluster::to(&cluster_config);
  SslContext::Ptr ssl_context(SslContextFactory::create());
  CassSsl* ssl = CassSsl::to(ssl_context.get());

  EXPECT_EQ(CASS_OK, cass_cluster_set_contact_points(cluster, "some.contact.point"));
  cass_cluster_set_ssl(cluster, ssl);
  add_logging_critera(
      "Contact points and SSL context must not be specified with cloud secure connection bundle");
  EXPECT_EQ(CASS_ERROR_LIB_BAD_PARAMS,
            cass_cluster_set_cloud_secure_connection_bundle_no_ssl_lib_init(
                cluster, creds_zip_file().c_str()));
  EXPECT_EQ(logging_criteria_count(), 1);
}
#endif
