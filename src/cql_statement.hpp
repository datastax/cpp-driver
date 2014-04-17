/*
  Copyright 2014 DataStax

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

#ifndef __CQL_STATEMENT_HPP_INCLUDED__
#define __CQL_STATEMENT_HPP_INCLUDED__

#include <list>
#include <utility>

struct CqlStatement {
  typedef std::pair<const char*, size_t>  Value;
  typedef std::list<Value>                ValueCollection;
  typedef ValueCollection::iterator       ValueIterator;
  typedef ValueCollection::const_iterator ConstValueIterator;

  virtual
  ~CqlStatement() {}

  virtual uint8_t
  kind() const = 0;

  virtual void
  statement(
      const std::string& statement) = 0;

  virtual void
  statement(
      const char* statement,
      size_t      size) = 0;

  virtual const char*
  statement() const = 0;

  virtual size_t
  statement_size() const = 0;

  virtual void
  consistency(
      int16_t consistency) = 0;

  virtual int16_t
  consistency() const = 0;

  virtual int16_t
  serial_consistency() const = 0;

  virtual void
  serial_consistency(
      int16_t consistency) = 0;

  virtual void
  add_value(
      const char* value,
      size_t      size) = 0;

  virtual size_t
  size() const = 0;

  virtual ValueIterator
  begin() = 0;

  virtual ValueIterator
  end() = 0;

  virtual ConstValueIterator
  begin() const = 0;

  virtual ConstValueIterator
  end() const = 0;
};

#endif
