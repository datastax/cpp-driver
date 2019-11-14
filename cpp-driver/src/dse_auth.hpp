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

#ifndef DATASTAX_ENTERPRISE_INTERNAL_AUTH_HPP
#define DATASTAX_ENTERPRISE_INTERNAL_AUTH_HPP

#include "allocated.hpp"
#include "dse.h"
#include "string.hpp"

#include <uv.h>

namespace datastax { namespace internal { namespace enterprise {

class GssapiAuthenticatorData : public Allocated {
public:
  GssapiAuthenticatorData(const datastax::String& service, const datastax::String& principal,
                          const datastax::String& authorization_id)
      : service_(service)
      , principal_(principal)
      , authorization_id_(authorization_id) {}

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

  static void on_challenge(CassAuthenticator* auth, void* data, const char* token,
                           size_t token_size);

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

}}} // namespace datastax::internal::enterprise

#endif
