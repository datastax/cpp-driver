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

#ifndef __CASS_ITERATOR_HPP_INCLUDED__
#define __CASS_ITERATOR_HPP_INCLUDED__

#include "allocated.hpp"
#include "cassandra.h"
#include "external.hpp"

namespace cass {

class Iterator : public Allocated {
public:
  Iterator(CassIteratorType type)
      : type_(type) {}

  virtual ~Iterator() {}

  CassIteratorType type() const { return type_; }

  virtual bool next() = 0;

private:
  const CassIteratorType type_;
};

} // namespace cass

EXTERNAL_TYPE(cass::Iterator, CassIterator)

#endif
