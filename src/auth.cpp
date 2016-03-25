/*
  Copyright (c) 2014-2015 DataStax

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

#include "auth.hpp"

#include "cassandra.h"

#include <algorithm>

#define SASL_AUTH_INIT_RESPONSE_SIZE 128

namespace cass {

void PlainTextAuthenticator::get_credentials(V1Authenticator::Credentials* credentials) {
  credentials->insert(std::pair<std::string, std::string>("username", username_));
  credentials->insert(std::pair<std::string, std::string>("password", password_));

}

bool PlainTextAuthenticator::initial_response(std::string* response) {
  response->reserve(username_.size() + password_.size() + 2);
  response->push_back(0);
  response->append(username_);
  response->push_back(0);
  response->append(password_);
  return true;
}

bool PlainTextAuthenticator::evaluate_challenge(const std::string& challenge, std::string* response) {
  return true;
}

void PlainTextAuthenticator::on_authenticate_success(const std::string& token) {
  // no-op
}

SaslAuthenticator::SaslAuthenticator(const Host::ConstPtr& host,
                                     const CassAuthCallbacks* callbacks,
                                     void* data)
  : callbacks_(callbacks)
  , data_(data) {
  auth_.exchange_data = NULL;
  auth_.host.address_length = host->address().to_inet(auth_.host.address);

  size_t length = std::min(static_cast<size_t>(CASS_HOSTNAME_LENGTH - 1),
                           host->hostname().size());
  memcpy(auth_.hostname, host->hostname().c_str(), length);
  auth_.hostname[length] = '\0';
}

SaslAuthenticator::~SaslAuthenticator() {
  if (callbacks_->cleanup_callback != NULL) {
    callbacks_->cleanup_callback(&auth_, data_);
  }
}

bool SaslAuthenticator::initial_response(std::string* response) {
  if (callbacks_->initial_callback == NULL) {
    return true;
  }

  response->resize(SASL_AUTH_INIT_RESPONSE_SIZE, '\0');
  for (int i = 0; i < 2; ++i) {
    size_t size = callbacks_->initial_callback(&auth_, data_,
                                               &(*response)[0], response->size());
    if (size == CASS_AUTH_ERROR) return false;

    if(size <= response->size()) {
      response->resize(size);
      break;
    }

    response->resize(size, '\0');
  }

  return true;
}

bool SaslAuthenticator::evaluate_challenge(const std::string& challenge, std::string* response) {
  if (callbacks_->challenge_callback == NULL) {
    return true;
  }

  response->resize(SASL_AUTH_INIT_RESPONSE_SIZE, '\0');
  for (int i = 0; i < 2; ++i) {
    size_t size = callbacks_->challenge_callback(&auth_, data_,
                                                 challenge.data(), challenge.size(),
                                                 &(*response)[0], response->size());
    if (size == CASS_AUTH_ERROR) return false;

    if(size <= response->size()) {
      response->resize(size);
      break;
    }

    response->resize(size, '\0');
  }

  return true;
}

void SaslAuthenticator::on_authenticate_success(const std::string& token) {
  if (callbacks_->success_callback != NULL) {
    callbacks_->success_callback(&auth_, data_,
                                 token.data(), token.size());
  }
}

} // namespace cass
