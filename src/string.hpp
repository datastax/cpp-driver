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

#ifndef DATASTAX_INTERNAL_STRING_HPP
#define DATASTAX_INTERNAL_STRING_HPP

#include "allocator.hpp"
#include "driver_config.hpp"
#include "hash.hpp"

#include <sparsehash/dense_hash_map>
#include <sstream>
#include <string>

namespace datastax {

typedef std::basic_string<char, std::char_traits<char>, internal::Allocator<char> > String;

namespace internal {

class OStringStream : public std::basic_ostream<char, std::char_traits<char> > {
public:
  typedef char char_type;
  typedef std::char_traits<char> traits_type;
  typedef traits_type::int_type int_type;
  typedef traits_type::pos_type pos_type;
  typedef traits_type::off_type off_type;
  typedef internal::Allocator<char> allocator_type;

  typedef std::basic_string<char_type, traits_type, allocator_type> string_type;

public:
  explicit OStringStream(std::ios_base::openmode mode = ios_base::out)
      : std::basic_ostream<char_type, traits_type>(&sb_)
      , sb_(mode | ios_base::out) {}

  explicit OStringStream(const string_type& str, std::ios_base::openmode mode = ios_base::out)
      : std::basic_ostream<char_type, traits_type>(&sb_)
      , sb_(str, mode | std::ios_base::out) {}

  std::basic_stringbuf<char_type, traits_type, allocator_type>* rdbuf() const {
    return const_cast<std::basic_stringbuf<char_type, traits_type, allocator_type>*>(&sb_);
  }

  string_type str() const { return sb_.str(); }

  void str(const string_type& str) { sb_.str(str); }

private:
  std::basic_stringbuf<char, std::char_traits<char>, internal::Allocator<char> > sb_;
};

class IStringStream : public std::basic_istream<char, std::char_traits<char> > {
public:
  typedef char char_type;
  typedef std::char_traits<char> traits_type;
  typedef traits_type::int_type int_type;
  typedef traits_type::pos_type pos_type;
  typedef traits_type::off_type off_type;
  typedef internal::Allocator<char> allocator_type;

  typedef std::basic_string<char_type, traits_type, allocator_type> string_type;

public:
  explicit IStringStream(std::ios_base::openmode mode = ios_base::in)
      : std::basic_istream<char_type, traits_type>(&sb_)
      , sb_(mode | ios_base::in) {}

  explicit IStringStream(const string_type& str, std::ios_base::openmode mode = ios_base::in)
      : std::basic_istream<char_type, traits_type>(&sb_)
      , sb_(str, mode | std::ios_base::in) {}

  std::basic_stringbuf<char_type, traits_type, allocator_type>* rdbuf() const {
    return const_cast<std::basic_stringbuf<char_type, traits_type, allocator_type>*>(&sb_);
  }

  string_type str() const { return sb_.str(); }

  void str(const string_type& str) { sb_.str(str); }

private:
  std::basic_stringbuf<char, std::char_traits<char>, internal::Allocator<char> > sb_;
};

} // namespace internal
} // namespace datastax

namespace std {

#if defined(HASH_IN_TR1) && !defined(_WIN32)
namespace tr1 {
#endif

template <>
struct hash<datastax::String> {
  size_t operator()(const datastax::String& str) const {
    return datastax::hash::fnv1a(str.data(), str.size());
  }
};

#if defined(HASH_IN_TR1) && !defined(_WIN32)
} // namespace tr1
#endif

} // namespace std

#endif
