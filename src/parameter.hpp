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

#ifndef __CASS_PARMETER_HPP_INCLUDED__
#define __CASS_PARMETER_HPP_INCLUDED__

#include "buffer.hpp"
#include "buffer_collection.hpp"

#include <vector>

namespace cass {

class Parameter {
public:
  enum Type {
    NIL,
    VALUE,
    COLLECTION
  };

  Type type() const { return type_; }

  void set_nil() {
    type_ = NIL;
  }

  const Buffer& value() const { return value_; }

  void set_value(const Buffer& value) {
    type_ = VALUE;
    value_ = value;
  }

  const BufferCollection& collection() const { return collection_; }

  void set_collection(const BufferCollection& collection) {
    type_ = COLLECTION;
    collection_ = collection;
  }

private:
  Type type_;
  Buffer value_;
  BufferCollection collection_;
};

typedef std::vector<Parameter> ParameterVec;

} // namespace cass

#endif
