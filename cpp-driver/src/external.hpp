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

#ifndef DATASTAX_INTERNAL_EXTERNAL_HPP
#define DATASTAX_INTERNAL_EXTERNAL_HPP

// This abstraction allows us to separate internal types from the
// external opaque pointers that we expose.
template <typename In, typename Ex>
struct External : public In {
  In* from() { return static_cast<In*>(this); }
  const In* from() const { return static_cast<const In*>(this); }
  static Ex* to(In* in) { return static_cast<Ex*>(in); }
  static const Ex* to(const In* in) { return static_cast<const Ex*>(in); }
};

#define EXTERNAL_TYPE(InternalType, ExternalType)                        \
  extern "C" {                                                           \
  struct ExternalType##_ : public External<InternalType, ExternalType> { \
  private:                                                               \
    ~ExternalType##_() {}                                                \
  };                                                                     \
  }

#endif
