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

#include "auth.hpp"
#include "external.hpp"
#include "string.hpp"

#include "cassandra.h"

#include <algorithm>

using namespace datastax;
using namespace datastax::internal::core;

extern "C" {

void cass_authenticator_address(const CassAuthenticator* auth, CassInet* address) {
  address->address_length = auth->address().to_inet(address->address);
}

const char* cass_authenticator_hostname(const CassAuthenticator* auth, size_t* length) {
  if (length != NULL) *length = auth->hostname().length();
  return auth->hostname().c_str();
}

const char* cass_authenticator_class_name(const CassAuthenticator* auth, size_t* length) {
  if (length != NULL) *length = auth->class_name().length();
  return auth->class_name().c_str();
}

void* cass_authenticator_exchange_data(CassAuthenticator* auth) { return auth->exchange_data(); }

void cass_authenticator_set_exchange_data(CassAuthenticator* auth, void* exchange_data) {
  auth->set_exchange_data(exchange_data);
}

char* cass_authenticator_response(CassAuthenticator* auth, size_t size) {
  String* response = auth->response();

  if (response != NULL) {
    response->resize(size, 0);
    return &(*response)[0];
  }

  return NULL;
}

void cass_authenticator_set_response(CassAuthenticator* auth, const char* response,
                                     size_t response_size) {
  if (auth->response() != NULL) {
    auth->response()->assign(response, response_size);
  }
}

void cass_authenticator_set_error(CassAuthenticator* auth, const char* message) {
  cass_authenticator_set_error_n(auth, message, SAFE_STRLEN(message));
}

void cass_authenticator_set_error_n(CassAuthenticator* auth, const char* message,
                                    size_t message_length) {
  auth->set_error(String(message, message_length));
}

} // extern "C"

bool PlainTextAuthenticator::initial_response(String* response) {
  response->reserve(username_.size() + password_.size() + 2);
  response->push_back(0);
  response->append(username_);
  response->push_back(0);
  response->append(password_);
  return true;
}

bool PlainTextAuthenticator::evaluate_challenge(const String& token, String* response) {
  // no-op
  return true;
}

bool PlainTextAuthenticator::success(const String& token) {
  // no-op
  return true;
}

ExternalAuthenticator::ExternalAuthenticator(const Address& address, const String& hostname,
                                             const String& class_name,
                                             const CassAuthenticatorCallbacks* callbacks,
                                             void* data)
    : address_(address)
    , hostname_(hostname)
    , class_name_(class_name)
    , response_(NULL)
    , callbacks_(callbacks)
    , data_(data)
    , exchange_data_(NULL) {}

ExternalAuthenticator::~ExternalAuthenticator() {
  response_ = NULL;
  if (callbacks_->cleanup_callback != NULL) {
    callbacks_->cleanup_callback(CassAuthenticator::to(this), data_);
  }
}

bool ExternalAuthenticator::initial_response(String* response) {
  if (callbacks_->initial_callback == NULL) {
    return true;
  }
  response_ = response;
  error_.clear();
  callbacks_->initial_callback(CassAuthenticator::to(this), data_);
  return error_.empty();
}

bool ExternalAuthenticator::evaluate_challenge(const String& token, String* response) {
  if (callbacks_->challenge_callback == NULL) {
    return true;
  }
  response_ = response;
  error_.clear();
  callbacks_->challenge_callback(CassAuthenticator::to(this), data_, token.data(), token.size());
  return error_.empty();
}

bool ExternalAuthenticator::success(const String& token) {
  if (callbacks_->success_callback == NULL) {
    return true;
  }
  response_ = NULL;
  error_.clear();
  callbacks_->success_callback(CassAuthenticator::to(this), data_, token.data(), token.size());
  return error_.empty();
}
