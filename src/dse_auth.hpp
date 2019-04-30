/*
  Copyright (c) DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef DATASTAX_ENTERPRISE_INTERNAL_AUTH_HPP
#define DATASTAX_ENTERPRISE_INTERNAL_AUTH_HPP

#include "allocated.hpp"
#include "dse.h"
#include "string.hpp"

#include <uv.h>

namespace datastax { namespace internal { namespace enterprise {

class PlaintextAuthenticatorData : public Allocated {
public:
  PlaintextAuthenticatorData(const datastax::String& username,
                             const datastax::String& password,
                             const datastax::String& authorization_id)
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
  datastax::String username_;
  datastax::String password_;
  datastax::String authorization_id_;
};

class GssapiAuthenticatorData : public Allocated {
public:
  GssapiAuthenticatorData(const datastax::String& service,
                          const datastax::String& principal,
                          const datastax::String& authorization_id)
    : service_(service)
    , principal_(principal)
    , authorization_id_(authorization_id) { }

  static const CassAuthenticatorCallbacks* callbacks() { return &callbacks_; }

  const datastax::String& service() const { return service_; }
  const datastax::String& principal() const { return principal_; }
  const datastax::String& authorization_id() const { return authorization_id_; }

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
  datastax::String service_;
  datastax::String principal_;
  datastax::String authorization_id_;
};

} } } // namespace datastax::internal::enterprise

#endif
