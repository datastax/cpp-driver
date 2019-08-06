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
#include "unit.hpp"

#include "cloud_secure_connection_config.hpp"
#include "config.hpp"
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

using datastax::String;
using datastax::internal::core::CloudSecureConnectionConfig;
using datastax::internal::core::Config;
using datastax::internal::enterprise::DsePlainTextAuthProvider;
using datastax::internal::json::StringBuffer;
using datastax::internal::json::Writer;

using mockssandra::Ssl;

class CloudSecureConnectionConfigTest : public Unit {
public:
  const String& ca_cert() const { return ca_cert_; }
  void set_invalid_ca_cert() { ca_cert_ = "!!!!!INVALID!!!!!"; }
  const String& cert() const { return cert_; }
  void set_invalid_cert() { cert_ = "!!!!!INVALID!!!!!"; }
  const String& key() const { return key_; }
  void set_invalid_key() { key_ = "!!!!!INVALID!!!!!"; }

  void SetUp() {
    Unit::SetUp();
    char tmp[260] = { 0 }; // Note: 260 is the maximum path on Windows
    size_t tmp_length = 260;
    uv_os_tmpdir(tmp, &tmp_length);

    tmp_zip_file_ = String(tmp, tmp_length) + PATH_SEPARATOR + CREDS_V1_ZIP_FILE;

    ca_cert_ = Ssl::generate_cert(Ssl::generate_key());
    key_ = Ssl::generate_key();
    cert_ = Ssl::generate_cert(key_, "localhost");
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

  static StringBuffer full_config_credsv1() {
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    writer.StartObject();
    writer.Key("username");
    writer.String("DataStax");
    writer.Key("password");
    writer.String("Constellation");
    writer.Key("host");
    writer.String("cloud.datastax.com");
    writer.Key("port");
    writer.Int(1443);
    writer.Key("keyspace");
    writer.String("database_as_a_service");
    writer.EndObject();
    return buffer;
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
  String cert_;
  String key_;
};

TEST_F(CloudSecureConnectionConfigTest, CredsV1) {
  Config config;
  CloudSecureConnectionConfig cloud_config;

  StringBuffer buffer(full_config_credsv1());
  create_zip_file(buffer.GetString());

  EXPECT_TRUE(cloud_config.load(creds_zip_file(), &config));
  EXPECT_EQ("DataStax", cloud_config.username());
  EXPECT_EQ("Constellation", cloud_config.password());
  EXPECT_EQ("cloud.datastax.com", cloud_config.host());
  EXPECT_EQ(1443, cloud_config.port());
  EXPECT_EQ("database_as_a_service", cloud_config.keyspace());
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
  writer.Key("keyspace");
  writer.String("datastax");
  writer.EndObject();
  create_zip_file(buffer.GetString());

  EXPECT_TRUE(cloud_config.load(creds_zip_file(), &config));
  EXPECT_EQ("", cloud_config.username());
  EXPECT_EQ("", cloud_config.password());
  EXPECT_EQ("bigdata.datastax.com", cloud_config.host());
  EXPECT_EQ(2443, cloud_config.port());
  EXPECT_EQ("datastax", cloud_config.keyspace());
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
  writer.String("Constellation");
  writer.Key("port");
  writer.Int(1443);
  writer.Key("keyspace");
  writer.String("database_as_a_service");
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
  writer.String("Constellation");
  writer.Key("host");
  writer.String("cloud.datastax.com");
  writer.Key("keyspace");
  writer.String("database_as_a_service");
  writer.EndObject();
  create_zip_file(buffer.GetString());

  EXPECT_FALSE(config.load(creds_zip_file()));
}

TEST_F(CloudSecureConnectionConfigTest, InvalidCredsV1ConfigMissingKeyspace) {
  CloudSecureConnectionConfig config;

  StringBuffer buffer;
  Writer<StringBuffer> writer(buffer);
  writer.StartObject();
  writer.Key("username");
  writer.String("DataStax");
  writer.Key("password");
  writer.String("Constellation");
  writer.Key("host");
  writer.String("cloud.datastax.com");
  writer.Key("port");
  writer.Int(1443);
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

  StringBuffer buffer(full_config_credsv1());
  create_zip_file(buffer.GetString(), true, false);
  EXPECT_FALSE(config.load(creds_zip_file()));
}

TEST_F(CloudSecureConnectionConfigTest, InvalidCredsV1MissingCert) {
  CloudSecureConnectionConfig config;

  StringBuffer buffer(full_config_credsv1());
  create_zip_file(buffer.GetString(), true, true, false);
  EXPECT_FALSE(config.load(creds_zip_file()));
}

TEST_F(CloudSecureConnectionConfigTest, InvalidCredsV1MissingKey) {
  CloudSecureConnectionConfig config;

  StringBuffer buffer(full_config_credsv1());
  create_zip_file(buffer.GetString(), true, true, true, false);
  EXPECT_FALSE(config.load(creds_zip_file()));
}

TEST_F(CloudSecureConnectionConfigTest, InvalidCredsV1SslCaCert) {
  Config config;
  CloudSecureConnectionConfig cloud_config;

  StringBuffer buffer(full_config_credsv1());
  set_invalid_ca_cert();
  create_zip_file(buffer.GetString());

  EXPECT_FALSE(cloud_config.load(creds_zip_file(), &config));
  EXPECT_FALSE(config.ssl_context());
}

TEST_F(CloudSecureConnectionConfigTest, InvalidCredsV1SslCert) {
  Config config;
  CloudSecureConnectionConfig cloud_config;

  StringBuffer buffer(full_config_credsv1());
  set_invalid_cert();
  create_zip_file(buffer.GetString());

  EXPECT_FALSE(cloud_config.load(creds_zip_file(), &config));
  EXPECT_FALSE(config.ssl_context());
}

TEST_F(CloudSecureConnectionConfigTest, InvalidCredsV1SslKey) {
  Config config;
  CloudSecureConnectionConfig cloud_config;

  StringBuffer buffer(full_config_credsv1());
  set_invalid_key();
  create_zip_file(buffer.GetString());

  EXPECT_FALSE(cloud_config.load(creds_zip_file(), &config));
  EXPECT_FALSE(config.ssl_context());
}
#endif
