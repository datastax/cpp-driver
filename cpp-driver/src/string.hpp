/*
  Copyright (c) DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __DSE_STRING_HPP_INCLUDED__
#define __DSE_STRING_HPP_INCLUDED__

#include "allocator.hpp"
#include "cassconfig.hpp"
#include "hash.hpp"

#include <sparsehash/dense_hash_map>
#include <string>
#include <sstream>

namespace cass {

typedef std::basic_string<char, std::char_traits<char>, cass::Allocator<char> > String;

class OStringStream : public std::basic_ostream<char, std::char_traits<char> >
{
public:
    typedef char                   char_type;
    typedef std::char_traits<char> traits_type;
    typedef traits_type::int_type  int_type;
    typedef traits_type::pos_type  pos_type;
    typedef traits_type::off_type  off_type;
    typedef cass::Allocator<char>  allocator_type;

    typedef std::basic_string<char_type, traits_type, allocator_type> string_type;

public:
    explicit OStringStream(std::ios_base::openmode mode = ios_base::out)
      : std::basic_ostream<char_type, traits_type>(&sb_)
      , sb_(mode | ios_base::out) { }

    explicit OStringStream(const string_type& str,
                           std::ios_base::openmode mode = ios_base::out)
    : std::basic_ostream<char_type, traits_type>(&sb_)
    , sb_(str, mode | std::ios_base::out) { }

    std::basic_stringbuf<char_type, traits_type, allocator_type>* rdbuf() const {
      return const_cast<std::basic_stringbuf<char_type, traits_type, allocator_type>*>(&sb_);
    }

    string_type str() const { return sb_.str(); }

    void str(const string_type& str) { sb_.str(str); }

private:
  std::basic_stringbuf<char, std::char_traits<char>, cass::Allocator<char> > sb_;
};

class IStringStream : public std::basic_istream<char, std::char_traits<char> >
{
public:
    typedef char                   char_type;
    typedef std::char_traits<char> traits_type;
    typedef traits_type::int_type  int_type;
    typedef traits_type::pos_type  pos_type;
    typedef traits_type::off_type  off_type;
    typedef cass::Allocator<char>  allocator_type;

    typedef std::basic_string<char_type, traits_type, allocator_type> string_type;

public:
    explicit IStringStream(std::ios_base::openmode mode = ios_base::in)
      : std::basic_istream<char_type, traits_type>(&sb_)
      , sb_(mode | ios_base::in) { }

    explicit IStringStream(const string_type& str,
                           std::ios_base::openmode mode = ios_base::in)
    : std::basic_istream<char_type, traits_type>(&sb_)
    , sb_(str, mode | std::ios_base::in) { }

    std::basic_stringbuf<char_type, traits_type, allocator_type>* rdbuf() const {
      return const_cast<std::basic_stringbuf<char_type, traits_type, allocator_type>*>(&sb_);
    }

    string_type str() const { return sb_.str(); }

    void str(const string_type& str) { sb_.str(str); }

private:
  std::basic_stringbuf<char, std::char_traits<char>, cass::Allocator<char> > sb_;
};

} // namepsace cass

namespace std {

#if defined(HASH_IN_TR1) && !defined(_WIN32)
namespace tr1 {
#endif

template <>
struct hash<cass::String> {
  size_t operator()(const cass::String& str) const {
    return cass::hash::fnv1a(str.data(), str.size());
  }
};

#if defined(HASH_IN_TR1) && !defined(_WIN32)
} //namespace tr1
#endif

} //namespace std

#endif
