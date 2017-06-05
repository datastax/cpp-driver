/*
  Copyright (c) 2017 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __DSE_AUTH_HPP_INCLUDED__
#define __DSE_AUTH_HPP_INCLUDED__

#include "dse.h"
#include "string.hpp"

#include <uv.h>

namespace dse {

class PlaintextAuthenticatorData {
public:
  PlaintextAuthenticatorData(const cass::String& username,
                             const cass::String& password,
                             const cass::String& authorization_id)
    : username_(username)
    , password_(password)
    , authorization_id_(authorization_id) { }

  static const CassAuthenticatorCallbacks* callbacks() { return &callbacks_; }

private:
  static void on_initial(CassAuthenticator* auth, void* data);

  static void on_challenge(CassAuthenticator* auth, void* data,
                           const char* token, size_t token_size);
private:
  static CassAuthenticatorCallbacks callbacks_;

private:
  cass::String username_;
  cass::String password_;
  cass::String authorization_id_;
};

class GssapiAuthenticatorData {
public:
  GssapiAuthenticatorData(const cass::String& service,
                          const cass::String& principal,
                          const cass::String& authorization_id)
    : service_(service)
    , principal_(principal)
    , authorization_id_(authorization_id) { }

  static const CassAuthenticatorCallbacks* callbacks() { return &callbacks_; }

  const cass::String& service() const { return service_; }
  const cass::String& principal() const { return principal_; }
  const cass::String& authorization_id() const { return authorization_id_; }

  static CassError set_lock_callbacks(DseGssapiAuthenticatorLockCallback lock_callback,
                                      DseGssapiAuthenticatorUnlockCallback unlock_callback,
                                      void* data);

  static void lock() { lock_callback_(data_); }
  static void unlock() { unlock_callback_(data_); }

private:
  static void on_initial(CassAuthenticator* auth, void* data);

  static void on_challenge(CassAuthenticator* auth, void* data,
                           const char* token, size_t token_size);

  static void on_cleanup(CassAuthenticator* auth, void* data);


private:
  static CassAuthenticatorCallbacks callbacks_;
  static DseGssapiAuthenticatorLockCallback lock_callback_;
  static DseGssapiAuthenticatorUnlockCallback unlock_callback_;
  static void* data_;

private:
  cass::String service_;
  cass::String principal_;
  cass::String authorization_id_;
};

} // namespace dse

#endif
