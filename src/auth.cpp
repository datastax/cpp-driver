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

#define SASL_AUTH_INIT_RESPONSE_SIZE 128

namespace cass {

void PlainTextAuthenticator::get_credentials(V1Authenticator::Credentials* credentials) {
  credentials->insert(std::pair<std::string, std::string>("username", username_));
  credentials->insert(std::pair<std::string, std::string>("password", password_));

}

std::string PlainTextAuthenticator::initial_response() {
  std::string token;
  token.reserve(username_.size() + password_.size() + 2);
  token.push_back(0);
  token.append(username_);
  token.push_back(0);
  token.append(password_);
  return token;
}

std::string PlainTextAuthenticator::evaluate_challenge(const std::string& challenge) {
  // no-op
  return std::string();
}

void PlainTextAuthenticator::on_authenticate_success(const std::string& token) {
  // no-op
}

SaslAuthenticator::SaslAuthenticator(const Address& host,
                                     const CassAuthCallbacks* callbacks,
                                     void* data)
  : callbacks_(callbacks)
  , data_(data) {
  auth_.data = NULL;
  auth_.host.address_length = host.to_inet(auth_.host.address);
}

SaslAuthenticator::~SaslAuthenticator() {
  if (callbacks_->cleanup_callback != NULL) {
    callbacks_->cleanup_callback(&auth_, data_);
  }
}

std::string SaslAuthenticator::initial_response() {
  if (callbacks_->initial_callback == NULL) {
    return std::string();
  }

  std::string response(SASL_AUTH_INIT_RESPONSE_SIZE, '\0');
  for (int i = 0; i < 2; ++i) {
    size_t size = callbacks_->initial_callback(&auth_, data_,
                                               &response[0], response.size());
    if(size <= response.size()) {
      response.resize(size);
      break;
    }

    response.resize(size, '\0');
  }
  return response;
}

std::string SaslAuthenticator::evaluate_challenge(const std::string& challenge) {
  if (callbacks_->challenge_callback == NULL) {
    return std::string();
  }

  std::string response(SASL_AUTH_INIT_RESPONSE_SIZE, '\0');
  for (int i = 0; i < 2; ++i) {
    size_t size = callbacks_->challenge_callback(&auth_, data_,
                                                 challenge.data(), challenge.size(),
                                                 &response[0], response.size());
    if(size <= response.size()) {
      response.resize(size);
      break;
    }

    response.resize(size, '\0');
  }
  return response;
}

void SaslAuthenticator::on_authenticate_success(const std::string& token) {
  if (callbacks_->success_callback != NULL) {
    callbacks_->success_callback(&auth_, data_,
                                 token.data(), token.size());
  }
}

} // namespace cass
