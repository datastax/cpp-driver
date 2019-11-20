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

#include "cloud_secure_connection_config.hpp"

#include "cluster.hpp"
#include "cluster_metadata_resolver.hpp"
#include "config.hpp"
#include "dse_auth.hpp"
#include "http_client.hpp"
#include "json.hpp"
#include "logger.hpp"
#include "ssl.hpp"
#include "utils.hpp"

using namespace datastax;
using namespace datastax::internal;
using namespace datastax::internal::core;

#define CLOUD_ERROR "Unable to load cloud secure connection configuration: "
#define METADATA_SERVER_ERROR "Unable to configure driver from metadata server: "

// Pinned to v1 because that's what the driver currently handles.
#define METADATA_SERVER_PATH "/metadata?version=1"

#define METADATA_SERVER_PORT 30443
#define RESPONSE_BODY_TRUNCATE_LENGTH 1024

#ifdef HAVE_ZLIB
#include "unzip.h"

#define CONFIGURATION_FILE "config.json"
#define CERTIFICATE_AUTHORITY_FILE "ca.crt"
#define CERTIFICATE_FILE "cert"
#define KEY_FILE "key"

class UnzipFile {
public:
  UnzipFile()
      : file(NULL) {}

  ~UnzipFile() { unzClose(file); }

  bool open(const String& filename) { return (file = unzOpen(filename.c_str())) != NULL; }

  bool read_contents(const String& filename, String* contents) {
    int rc = unzLocateFile(file, filename.c_str(), 0);
    if (rc != UNZ_OK) {
      return false;
    }

    rc = unzOpenCurrentFile(file);
    if (rc != UNZ_OK) {
      return false;
    }

    unz_file_info file_info;
    rc = unzGetCurrentFileInfo(file, &file_info, 0, 0, 0, 0, 0, 0);
    if (rc != UNZ_OK) {
      unzCloseCurrentFile(file);
      return false;
    }

    contents->resize(file_info.uncompressed_size, 0);
    unzReadCurrentFile(file, &(*contents)[0], contents->size());
    unzCloseCurrentFile(file);

    return true;
  }

private:
  unzFile file;
};
#endif

namespace {

class CloudClusterMetadataResolver : public ClusterMetadataResolver {
public:
  CloudClusterMetadataResolver(const String& host, int port, const SocketSettings& settings,
                               uint64_t request_timeout_ms)
      : client_(new HttpClient(Address(host, port), METADATA_SERVER_PATH,
                               bind_callback(&CloudClusterMetadataResolver::on_response, this))) {
    client_->with_settings(settings)->with_request_timeout_ms(request_timeout_ms);
  }

private:
  virtual void internal_resolve(uv_loop_t* loop, const AddressVec& contact_points) {
    inc_ref();
    client_->request(loop);
  }

  virtual void internal_cancel() { client_->cancel(); }

private:
  void on_response(HttpClient* http_client) {
    if (http_client->is_ok()) {
      if (http_client->content_type().find("json") != std::string::npos) {
        parse_metadata(http_client->response_body());
      } else {
        LOG_ERROR(METADATA_SERVER_ERROR "Invalid response content type: '%s'",
                  http_client->content_type().c_str());
      }
    } else if (!http_client->is_canceled()) {
      if (http_client->is_error_status_code()) {
        String error_message =
            http_client->response_body().substr(0, RESPONSE_BODY_TRUNCATE_LENGTH);
        if (http_client->content_type().find("json") != std::string::npos) {
          json::Document document;
          document.Parse(http_client->response_body().c_str());
          if (document.IsObject() && document.HasMember("message") &&
              document["message"].IsString()) {
            error_message = document["message"].GetString();
          }
        }
        LOG_ERROR(METADATA_SERVER_ERROR "Returned error response code %u: '%s'",
                  http_client->status_code(), error_message.c_str());
      } else {
        LOG_ERROR(METADATA_SERVER_ERROR "%s", http_client->error_message().c_str());
      }
    }

    callback_(this);
    dec_ref();
  }

  void parse_metadata(const String& response_body) {
    json::Document document;
    document.Parse(response_body.c_str());

    if (!document.IsObject()) {
      LOG_ERROR(METADATA_SERVER_ERROR "Metadata JSON is invalid");
      return;
    }

    if (!document.HasMember("contact_info") || !document["contact_info"].IsObject()) {
      LOG_ERROR(METADATA_SERVER_ERROR "Contact information is not available");
      return;
    }

    const json::Value& contact_info = document["contact_info"];

    if (!contact_info.HasMember("local_dc") || !contact_info["local_dc"].IsString()) {
      LOG_ERROR(METADATA_SERVER_ERROR "Local DC is not available");
      return;
    }

    local_dc_ = contact_info["local_dc"].GetString();

    if (!contact_info.HasMember("sni_proxy_address") ||
        !contact_info["sni_proxy_address"].IsString()) {
      LOG_ERROR(METADATA_SERVER_ERROR "SNI proxy address is not available");
      return;
    }

    int sni_port = METADATA_SERVER_PORT;
    Vector<String> tokens;
    explode(contact_info["sni_proxy_address"].GetString(), tokens, ':');
    String sni_address = tokens[0];
    if (tokens.size() == 2) {
      IStringStream ss(tokens[1]);
      if ((ss >> sni_port).fail()) {
        LOG_WARN(METADATA_SERVER_ERROR "Invalid port, default %d will be used",
                 METADATA_SERVER_PORT);
      }
    }

    if (!contact_info.HasMember("contact_points") || !contact_info["contact_points"].IsArray()) {
      LOG_ERROR(METADATA_SERVER_ERROR "Contact points are not available");
      return;
    }

    const json::Value& contact_points = contact_info["contact_points"];
    for (rapidjson::SizeType i = 0; i < contact_points.Size(); ++i) {
      if (contact_points[i].IsString()) {
        String host_id = contact_points[i].GetString();
        resolved_contact_points_.push_back(Address(sni_address, sni_port, host_id));
      }
    }
  }

private:
  HttpClient::Ptr client_;
};

class CloudClusterMetadataResolverFactory : public ClusterMetadataResolverFactory {
public:
  CloudClusterMetadataResolverFactory(const String& host, int port)
      : host_(host)
      , port_(port) {}

  virtual ClusterMetadataResolver::Ptr new_instance(const ClusterSettings& settings) const {
    return ClusterMetadataResolver::Ptr(new CloudClusterMetadataResolver(
        host_, port_, settings.control_connection_settings.connection_settings.socket_settings,
        settings.control_connection_settings.connection_settings.connect_timeout_ms));
  }

  virtual const char* name() const { return "Cloud"; }

private:
  String host_;
  int port_;
};

} // namespace

CloudSecureConnectionConfig::CloudSecureConnectionConfig()
    : is_loaded_(false)
    , port_(0) {}

bool CloudSecureConnectionConfig::load(const String& filename, Config* config /* = NULL */) {
#ifndef HAVE_ZLIB
  LOG_ERROR(CLOUD_ERROR "Driver was not built with zlib support");
  return false;
#else
  UnzipFile zip_file;
  if (!zip_file.open(filename.c_str())) {
    LOG_ERROR(CLOUD_ERROR "Unable to open zip file %s; file does not exist or is invalid",
              filename.c_str());
    return false;
  }

  String contents;
  if (!zip_file.read_contents(CONFIGURATION_FILE, &contents)) {
    LOG_ERROR(CLOUD_ERROR "Missing configuration file %s", CONFIGURATION_FILE);
    return false;
  }

  json::MemoryStream memory_stream(contents.c_str(), contents.size());
  json::AutoUTFMemoryInputStream auto_utf_stream(memory_stream);
  json::Document document;
  document.ParseStream(auto_utf_stream);
  if (!document.IsObject()) {
    LOG_ERROR(CLOUD_ERROR "Invalid configuration");
    return false;
  }

  if (document.HasMember("username") && document["username"].IsString()) {
    username_ = document["username"].GetString();
  }
  if (document.HasMember("password") && document["password"].IsString()) {
    password_ = document["password"].GetString();
  }

  if (config && (!username_.empty() || !password_.empty())) {
    config->set_auth_provider(
        AuthProvider::Ptr(new enterprise::DsePlainTextAuthProvider(username_, password_, "")));
  }

  if (!document.HasMember("host") || !document["host"].IsString()) {
    LOG_ERROR(CLOUD_ERROR "Missing host");
    return false;
  }
  if (!document.HasMember("port") || !document["port"].IsInt()) {
    LOG_ERROR(CLOUD_ERROR "Missing port");
    return false;
  }
  host_ = document["host"].GetString();
  port_ = document["port"].GetInt();

  if (!zip_file.read_contents(CERTIFICATE_AUTHORITY_FILE, &ca_cert_)) {
    LOG_ERROR(CLOUD_ERROR "Missing certificate authority file %s", CERTIFICATE_AUTHORITY_FILE);
    return false;
  }

  if (!zip_file.read_contents(CERTIFICATE_FILE, &cert_)) {
    LOG_ERROR(CLOUD_ERROR "Missing certificate file %s", CERTIFICATE_FILE);
    return false;
  }

  if (!zip_file.read_contents(KEY_FILE, &key_)) {
    LOG_ERROR(CLOUD_ERROR "Missing key file %s", KEY_FILE);
    return false;
  }

  if (config) {
    SslContext::Ptr ssl_context(SslContextFactory::create());

    ssl_context->set_verify_flags(CASS_SSL_VERIFY_PEER_CERT | CASS_SSL_VERIFY_PEER_IDENTITY_DNS);

    if (ssl_context->add_trusted_cert(ca_cert_.c_str(), ca_cert_.length()) != CASS_OK) {
      LOG_ERROR(CLOUD_ERROR "Invalid CA certificate %s", CERTIFICATE_AUTHORITY_FILE);
      return false;
    }

    if (ssl_context->set_cert(cert_.c_str(), cert_.length()) != CASS_OK) {
      LOG_ERROR(CLOUD_ERROR "Invalid client certificate %s", CERTIFICATE_FILE);
      return false;
    }

    if (ssl_context->set_private_key(key_.c_str(), key_.length(), NULL, 0) != CASS_OK) {
      LOG_ERROR(CLOUD_ERROR "Invalid client private key %s", KEY_FILE);
      return false;
    }

    config->set_ssl_context(ssl_context);

    config->set_cluster_metadata_resolver_factory(
        ClusterMetadataResolverFactory::Ptr(new CloudClusterMetadataResolverFactory(host_, port_)));
  }

  is_loaded_ = true;
  return true;
#endif
}
