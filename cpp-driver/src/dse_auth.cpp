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

#include "dse_auth.hpp"
#include "allocated.hpp"
#include "logger.hpp"
#include "scoped_ptr.hpp"
#include "string_ref.hpp"

#include <string.h>

#include <gssapi/gssapi.h>
#include <gssapi/gssapi_generic.h>
#include <gssapi/gssapi_krb5.h>

#define DSE_AUTHENTICATOR "com.datastax.bdp.cassandra.auth.DseAuthenticator"

#define GSSAPI_AUTH_MECHANISM "GSSAPI"
#define GSSAPI_AUTH_SERVER_INITIAL_CHALLENGE "GSSAPI-START"

using namespace datastax;
using namespace datastax::internal;
using namespace datastax::internal::enterprise;

static void dse_gssapi_authenticator_nop_lock(void* data) {}
static void dse_gssapi_authenticator_nop_unlock(void* data) {}

extern "C" {

CassError
dse_gssapi_authenticator_set_lock_callbacks(DseGssapiAuthenticatorLockCallback lock_callback,
                                            DseGssapiAuthenticatorUnlockCallback unlock_callback,
                                            void* data) {
  return GssapiAuthenticatorData::set_lock_callbacks(lock_callback, unlock_callback, data);
}

} // extern "C"

struct GssapiBuffer {
public:
  gss_buffer_desc buffer;

public:
  GssapiBuffer() {
    buffer.value = NULL;
    buffer.length = 0;
  }

  ~GssapiBuffer() { release(); }

  const unsigned char* value() const { return static_cast<const unsigned char*>(buffer.value); }

  const char* data() const { return static_cast<const char*>(buffer.value); }

  size_t length() const { return buffer.length; }

  bool is_empty() const { return buffer.length == 0; }

  void release() {
    if (buffer.value) {
      OM_uint32 min_stat;

      GssapiAuthenticatorData::lock();
      gss_release_buffer(&min_stat, &buffer);
      GssapiAuthenticatorData::unlock();
    }
  }
};

class GssapiName {
public:
  gss_name_t name;

public:
  GssapiName()
      : name(GSS_C_NO_NAME) {}

  ~GssapiName() { release(); }

  void release() {
    if (name != GSS_C_NO_NAME) {
      OM_uint32 min_stat;

      GssapiAuthenticatorData::lock();
      gss_release_name(&min_stat, &name);
      GssapiAuthenticatorData::unlock();
    }
  }
};

class GssapiAuthenticator : public Allocated {
public:
  enum State { NEGOTIATION, AUTHENTICATION, AUTHENTICATED };

  enum Result { RESULT_ERROR, RESULT_CONTINUE, RESULT_COMPLETE };

  enum Type { AUTH_NONE = 1, AUTH_INTEGRITY = 2, AUTH_CONFIDENTIALITY = 3 };

  GssapiAuthenticator(const String& authorization_id);
  ~GssapiAuthenticator();

  const String& response() const { return response_; }
  const String& error() const { return error_; }

  Result init(const String& service, const String& principal);
  Result process(const char* token, size_t token_length);

private:
  Result negotiate(gss_buffer_t challenge_token);
  Result authenticate(gss_buffer_t challenge_token);

  static String display_status(OM_uint32 maj, OM_uint32 min);

private:
  gss_ctx_id_t context_;
  gss_name_t server_name_;
  OM_uint32 gss_flags_;
  gss_cred_id_t client_creds_;
  String username_;
  String response_;
  String error_;
  State state_;
  String authorization_id_;
};

GssapiAuthenticator::GssapiAuthenticator(const String& authorization_id)
    : context_(GSS_C_NO_CONTEXT)
    , server_name_(GSS_C_NO_NAME)
    , gss_flags_(GSS_C_MUTUAL_FLAG | GSS_C_SEQUENCE_FLAG)
    , client_creds_(GSS_C_NO_CREDENTIAL)
    , state_(NEGOTIATION)
    , authorization_id_(authorization_id) {}

GssapiAuthenticator::~GssapiAuthenticator() {
  OM_uint32 min_stat;

  if (context_ != GSS_C_NO_CONTEXT) {
    GssapiAuthenticatorData::lock();
    gss_delete_sec_context(&min_stat, &context_, GSS_C_NO_BUFFER);
    GssapiAuthenticatorData::unlock();
  }

  if (server_name_ != GSS_C_NO_NAME) {
    GssapiAuthenticatorData::lock();
    gss_release_name(&min_stat, &server_name_);
    GssapiAuthenticatorData::unlock();
  }

  if (client_creds_ != GSS_C_NO_CREDENTIAL) {
    GssapiAuthenticatorData::lock();
    gss_release_cred(&min_stat, &client_creds_);
    GssapiAuthenticatorData::unlock();
  }
}

GssapiAuthenticator::Result GssapiAuthenticator::init(const String& service,
                                                      const String& principal) {
  OM_uint32 maj_stat;
  OM_uint32 min_stat;
  gss_buffer_desc name_token = GSS_C_EMPTY_BUFFER;

  name_token.value = const_cast<void*>(static_cast<const void*>(service.c_str()));
  name_token.length = service.size();

  GssapiAuthenticatorData::lock();
  maj_stat = gss_import_name(&min_stat, &name_token, GSS_C_NT_HOSTBASED_SERVICE, &server_name_);
  GssapiAuthenticatorData::unlock();

  if (GSS_ERROR(maj_stat)) {
    error_.assign("Failed to import server name (gss_import_name()): " +
                  display_status(maj_stat, min_stat));
    return RESULT_ERROR;
  }

  GssapiName principal_name; // Initialized to GSS_C_NO_NAME

  if (!principal.empty()) {
    gss_buffer_desc principal_token = GSS_C_EMPTY_BUFFER;

    principal_token.value = const_cast<void*>(static_cast<const void*>(principal.c_str()));
    principal_token.length = principal.size();

    GssapiAuthenticatorData::lock();
    maj_stat =
        gss_import_name(&min_stat, &principal_token, GSS_C_NT_USER_NAME, &principal_name.name);
    GssapiAuthenticatorData::unlock();

    if (GSS_ERROR(maj_stat)) {
      error_.assign("Failed to import principal name (gss_import_name()): " +
                    display_status(maj_stat, min_stat));
      return RESULT_ERROR;
    }
  }

  GssapiAuthenticatorData::lock();
  maj_stat = gss_acquire_cred(&min_stat, principal_name.name, GSS_C_INDEFINITE, GSS_C_NO_OID_SET,
                              GSS_C_INITIATE, &client_creds_, NULL, NULL);
  GssapiAuthenticatorData::unlock();

  if (GSS_ERROR(maj_stat)) {
    error_.assign("Failed to acquire principal credentials (gss_acquire_cred()): " +
                  display_status(maj_stat, min_stat));
    return RESULT_ERROR;
  }

  return RESULT_COMPLETE;
}

GssapiAuthenticator::Result GssapiAuthenticator::negotiate(gss_buffer_t challenge_token) {
  OM_uint32 maj_stat;
  OM_uint32 min_stat;
  GssapiBuffer output_token;
  Result result = RESULT_ERROR;

  GssapiAuthenticatorData::lock();
  maj_stat = gss_init_sec_context(&min_stat, client_creds_, &context_, server_name_, GSS_C_NO_OID,
                                  gss_flags_, 0, GSS_C_NO_CHANNEL_BINDINGS, challenge_token, NULL,
                                  &output_token.buffer, NULL, NULL);
  GssapiAuthenticatorData::unlock();

  if (maj_stat != GSS_S_COMPLETE && maj_stat != GSS_S_CONTINUE_NEEDED) {
    error_.assign("Failed to initalize security context (gss_init_sec_context()): " +
                  display_status(maj_stat, min_stat));
    return RESULT_ERROR;
  }

  result = (maj_stat == GSS_S_COMPLETE) ? RESULT_COMPLETE : RESULT_CONTINUE;

  if (!output_token.is_empty()) {
    response_.assign(output_token.data(), output_token.length());
  }

  if (result == RESULT_COMPLETE) {
    GssapiName user;

    GssapiAuthenticatorData::lock();
    maj_stat =
        gss_inquire_context(&min_stat, context_, &user.name, NULL, NULL, NULL, NULL, NULL, NULL);
    GssapiAuthenticatorData::unlock();

    if (GSS_ERROR(maj_stat)) {
      error_.assign(
          "Failed to inquire security context for user principal (gss_inquire_context()): " +
          display_status(maj_stat, min_stat));
      return RESULT_ERROR;
    }

    GssapiBuffer user_token;

    GssapiAuthenticatorData::lock();
    maj_stat = gss_display_name(&min_stat, user.name, &user_token.buffer, NULL);
    GssapiAuthenticatorData::unlock();

    if (GSS_ERROR(maj_stat)) {
      error_.assign("Failed to get display name for user principal (gss_inquire_context()): " +
                    display_status(maj_stat, min_stat));
      return RESULT_ERROR;
    } else {
      username_.assign(user_token.data(), user_token.length());
      state_ = AUTHENTICATION;
    }
  }

  return result;
}

GssapiAuthenticator::Result GssapiAuthenticator::authenticate(gss_buffer_t challenge_token) {
  OM_uint32 maj_stat;
  OM_uint32 min_stat;
  OM_uint32 req_output_size;
  OM_uint32 max_input_size;
  unsigned char qop;
  String input;
  gss_buffer_desc input_token = GSS_C_EMPTY_BUFFER;
  GssapiBuffer output_token;

  GssapiAuthenticatorData::lock();
  maj_stat = gss_unwrap(&min_stat, context_, challenge_token, &output_token.buffer, NULL, NULL);
  GssapiAuthenticatorData::unlock();

  if (GSS_ERROR(maj_stat)) {
    error_.assign("Failed to get unwrap challenge token (gss_unwrap()): " +
                  display_status(maj_stat, min_stat));
    return RESULT_ERROR;
  }

  if (output_token.length() != 4) {
    return RESULT_ERROR;
  }

  qop = output_token.value()[0];
  if (qop & AUTH_CONFIDENTIALITY) {
    qop = AUTH_CONFIDENTIALITY;
  } else if (qop & AUTH_INTEGRITY) {
    qop = AUTH_INTEGRITY;
  } else {
    qop = AUTH_NONE;
  }

  req_output_size = ((output_token.value())[1] << 16) | ((output_token.value())[2] << 8) |
                    ((output_token.value())[3] << 0);

  req_output_size = req_output_size & 0xFFFFFF;

  GssapiAuthenticatorData::lock();
  maj_stat = gss_wrap_size_limit(&min_stat, context_, 1, GSS_C_QOP_DEFAULT, req_output_size,
                                 &max_input_size);
  GssapiAuthenticatorData::unlock();

  if (max_input_size < req_output_size) {
    req_output_size = max_input_size;
  }

  input.push_back(qop);
  input.push_back((req_output_size >> 16) & 0xFF);
  input.push_back((req_output_size >> 8) & 0xFF);
  input.push_back((req_output_size >> 0) & 0xFF);

  // Send the authorization_id if present (proxy login), otherwise the username.
  const String& authorization_id = authorization_id_.empty() ? username_ : authorization_id_;
  input.append(authorization_id);

  input_token.length = 4 + authorization_id.size();
  input_token.value = const_cast<void*>(static_cast<const void*>(input.c_str()));

  output_token.release();

  GssapiAuthenticatorData::lock();
  maj_stat =
      gss_wrap(&min_stat, context_, 0, GSS_C_QOP_DEFAULT, &input_token, NULL, &output_token.buffer);
  GssapiAuthenticatorData::unlock();

  if (GSS_ERROR(maj_stat)) {
    error_.assign("Failed to get wrape response token (gss_wrap()): " +
                  display_status(maj_stat, min_stat));
    return RESULT_ERROR;
  }

  if (!output_token.is_empty()) {
    response_.assign(output_token.data(), output_token.length());
  }

  state_ = AUTHENTICATED;

  return RESULT_COMPLETE;
}

String GssapiAuthenticator::display_status(OM_uint32 maj, OM_uint32 min) {
  String error;
  OM_uint32 message_context;

  message_context = 0;

  do {
    GssapiBuffer message;
    OM_uint32 maj_stat, min_stat;

    GssapiAuthenticatorData::lock();
    maj_stat = gss_display_status(&min_stat, maj, GSS_C_GSS_CODE, GSS_C_NO_OID, &message_context,
                                  &message.buffer);
    GssapiAuthenticatorData::unlock();

    if (GSS_ERROR(maj_stat)) {
      error.append("GSSAPI error: (unable to get major error)");
      break;
    }

    error.append(message.data(), message.length());
  } while (message_context != 0);

  message_context = 0;

  error.append(" (");
  do {
    GssapiBuffer message;
    OM_uint32 maj_stat, min_stat;

    GssapiAuthenticatorData::lock();
    maj_stat = gss_display_status(&min_stat, min, GSS_C_MECH_CODE, GSS_C_NO_OID, &message_context,
                                  &message.buffer);
    GssapiAuthenticatorData::unlock();

    if (GSS_ERROR(maj_stat)) {
      error.append("GSSAPI error: (unable to get minor error)");
      break;
    }

    error.append(message.data(), message.length());
  } while (message_context != 0);
  error.append(" )");

  return error;
}

GssapiAuthenticator::Result GssapiAuthenticator::process(const char* token, size_t token_length) {
  Result result = RESULT_ERROR;
  gss_buffer_desc challenge_token = GSS_C_EMPTY_BUFFER;

  response_.clear();

  if (token && token_length > 0) {
    challenge_token.value = (void*)token;
    challenge_token.length = token_length;
  }

  switch (state_) {
    case NEGOTIATION:
      result = negotiate(&challenge_token);
      break;

    case AUTHENTICATION:
      result = authenticate(&challenge_token);
      break;

    default:
      break;
  }

  return result;
}

CassError
GssapiAuthenticatorData::set_lock_callbacks(DseGssapiAuthenticatorLockCallback lock_callback,
                                            DseGssapiAuthenticatorUnlockCallback unlock_callback,
                                            void* data) {
  if (lock_callback == NULL || unlock_callback == NULL || data_ == NULL) {
    lock_callback_ = dse_gssapi_authenticator_nop_lock;
    unlock_callback_ = dse_gssapi_authenticator_nop_unlock;
    data_ = NULL;
    return CASS_ERROR_LIB_BAD_PARAMS;
  } else {
    lock_callback_ = lock_callback;
    unlock_callback_ = unlock_callback;
    data_ = data;
    return CASS_OK;
  }
}

void GssapiAuthenticatorData::on_initial(CassAuthenticator* auth, void* data) {
  StringRef authenticator(DSE_AUTHENTICATOR);

  GssapiAuthenticatorData* gssapi_auth_data = static_cast<GssapiAuthenticatorData*>(data);
  GssapiAuthenticator* gssapi_auth =
      static_cast<GssapiAuthenticator*>(cass_authenticator_exchange_data(auth));

  if (gssapi_auth == NULL) {
    String service;

    size_t hostname_length = 0;
    const char* hostname = cass_authenticator_hostname(auth, &hostname_length);

    if (hostname_length == 0) {
      CassInet address;
      char inet[CASS_INET_STRING_LENGTH];
      cass_authenticator_address(auth, &address);
      cass_inet_string(address, inet);
      service.append(gssapi_auth_data->service());
      service.append("@");
      service.append(inet);
    } else {
      service.append(gssapi_auth_data->service());
      service.append("@");
      service.append(hostname);
    }

    gssapi_auth = new GssapiAuthenticator(gssapi_auth_data->authorization_id());
    cass_authenticator_set_exchange_data(auth, static_cast<void*>(gssapi_auth));

    if (gssapi_auth->init(service, gssapi_auth_data->principal()) ==
        GssapiAuthenticator::RESULT_ERROR) {
      String error("Unable to intialize GSSAPI: ");
      error.append(gssapi_auth->error());
      cass_authenticator_set_error_n(auth, error.data(), error.length());
      return;
    }
  }

  if (authenticator == cass_authenticator_class_name(auth, NULL)) {
    cass_authenticator_set_response(auth, GSSAPI_AUTH_MECHANISM, sizeof(GSSAPI_AUTH_MECHANISM) - 1);
  } else {
    on_challenge(auth, data, GSSAPI_AUTH_SERVER_INITIAL_CHALLENGE,
                 sizeof(GSSAPI_AUTH_SERVER_INITIAL_CHALLENGE) - 1);
  }
}

void GssapiAuthenticatorData::on_challenge(CassAuthenticator* auth, void* data, const char* token,
                                           size_t token_size) {
  StringRef gssapi(GSSAPI_AUTH_SERVER_INITIAL_CHALLENGE);

  GssapiAuthenticator* gssapi_auth =
      static_cast<GssapiAuthenticator*>(cass_authenticator_exchange_data(auth));

  if (gssapi == StringRef(token, token_size)) {
    if (gssapi_auth->process("", 0) == GssapiAuthenticator::RESULT_ERROR) {
      String error("GSSAPI initial handshake failed: ");
      error.append(gssapi_auth->error());
      cass_authenticator_set_error_n(auth, error.data(), error.length());
    }
  } else {
    if (gssapi_auth->process(token, token_size) == GssapiAuthenticator::RESULT_ERROR) {
      String error("GSSAPI challenge handshake failed: ");
      error.append(gssapi_auth->error());
      cass_authenticator_set_error_n(auth, error.data(), error.length());
    }
  }

  cass_authenticator_set_response(auth, gssapi_auth->response().data(),
                                  gssapi_auth->response().size());
}

void GssapiAuthenticatorData::on_cleanup(CassAuthenticator* auth, void* data) {
  GssapiAuthenticator* gssapi_auth =
      static_cast<GssapiAuthenticator*>(cass_authenticator_exchange_data(auth));
  delete gssapi_auth;
}

CassAuthenticatorCallbacks GssapiAuthenticatorData::callbacks_ = {
  GssapiAuthenticatorData::on_initial, GssapiAuthenticatorData::on_challenge, NULL,
  GssapiAuthenticatorData::on_cleanup
};

DseGssapiAuthenticatorLockCallback GssapiAuthenticatorData::lock_callback_ =
    dse_gssapi_authenticator_nop_lock;
DseGssapiAuthenticatorUnlockCallback GssapiAuthenticatorData::unlock_callback_ =
    dse_gssapi_authenticator_nop_unlock;
void* GssapiAuthenticatorData::data_ = NULL;
