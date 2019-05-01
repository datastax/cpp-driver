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

#ifndef DATASTAX_INTERNAL_STRING_REF_HPP
#define DATASTAX_INTERNAL_STRING_REF_HPP

#include "hash.hpp"
#include "macros.hpp"
#include "string.hpp"
#include "vector.hpp"

#include <algorithm>
#include <assert.h>
#include <cctype>
#include <stddef.h>
#include <string.h>

namespace datastax {

template <class IsEqual>
int compare(const char* s1, const char* s2, size_t length, IsEqual is_equal) {
  for (size_t i = 0; i < length; ++i) {
    if (!is_equal(s1[i], s2[i])) {
      return s1[i] < s2[i] ? -1 : 1;
    }
  }
  return 0;
}

class StringRef {
public:
  typedef char value_type;
  typedef const char* pointer;
  typedef const char& reference;
  typedef const char& const_reference;
  typedef size_t size_type;
  typedef ptrdiff_t difference_type;
  typedef const char* const_iterator;
  typedef const char* iterator;

  static const size_type npos;

  struct IsEqual {
    bool operator()(char a, char b) const { return a == b; }
  };

  struct IsEqualInsensitive {
    bool operator()(char a, char b) const { return std::toupper(a) == std::toupper(b); }
  };

public:
  StringRef()
      : ptr_(NULL)
      , length_(0) {}

  StringRef(const char* ptr, size_t length)
      : ptr_(ptr)
      , length_(length) {}

  StringRef(const char* str)
      : ptr_(str)
      , length_(SAFE_STRLEN(str)) {}

  StringRef(const String& str)
      : ptr_(str.data())
      , length_(str.size()) {}

  const char* data() const { return ptr_; }
  size_t size() const { return length_; }
  size_t length() const { return length_; }
  bool empty() const { return size() == 0; }

  iterator begin() const { return ptr_; }
  iterator end() const { return ptr_ + length_; }

  char front() const { return ptr_[0]; }
  char back() const { return ptr_[length_ - 1]; }

  String to_string() const { return String(ptr_, length_); }

  StringRef substr(size_t pos = 0, size_t length = npos) {
    assert(pos < length_);
    return StringRef(ptr_ + pos, std::min(length_ - pos, length));
  }

  size_t find(const StringRef& ref) const {
    if (ref.length_ == 0) return 0;
    if (length_ == 0) return npos;
    const_iterator i = std::search(ptr_, ptr_ + length_, ref.ptr_, ref.ptr_ + ref.length_);
    if (i == end()) return npos;
    return i - begin();
  }

  int compare(const StringRef& ref) const { return compare(ref, IsEqual()); }

  int icompare(const StringRef& ref) const { return compare(ref, IsEqualInsensitive()); }

  bool equals(const StringRef& ref) const { return compare(ref) == 0; }

  bool iequals(const StringRef& ref) const { return icompare(ref) == 0; }

  bool operator==(const StringRef& ref) const { return equals(ref); }

  bool operator!=(const StringRef& ref) const { return !equals(ref); }

  bool operator<(const StringRef& ref) const { return compare(ref) < 0; }

private:
  template <class IsEqual>
  int compare(const StringRef& ref, IsEqual is_equal) const {
    if (length_ < ref.length_) {
      return -1;
    } else if (length_ > ref.length_) {
      return 1;
    }
    return datastax::compare(ptr_, ref.ptr_, length_, is_equal);
  }

  const char* ptr_;
  size_t length_;
};

typedef internal::Vector<String> StringVec;
typedef internal::Vector<StringRef> StringRefVec;

inline StringVec to_strings(const StringRefVec& refs) {
  StringVec strings;
  strings.reserve(refs.size());
  for (StringRefVec::const_iterator i = refs.begin(), end = refs.end(); i != end; ++i) {
    strings.push_back(i->to_string());
  }
  return strings;
}

inline bool starts_with(const StringRef& input, const StringRef& target) {
  return input.length() >= target.length() &&
         compare(input.data(), target.data(), target.size(), StringRef::IsEqual()) == 0;
}

inline bool ends_with(const StringRef& input, const StringRef& target) {
  return input.length() >= target.length() &&
         compare(input.data() + (input.length() - target.length()), target.data(), target.size(),
                 StringRef::IsEqual()) == 0;
}

inline bool iequals(const StringRef& lhs, const StringRef& rhs) { return lhs.iequals(rhs); }

struct StringRefIHash {
  std::size_t operator()(const StringRef& s) const {
    return hash::fnv1a(s.data(), s.size(), ::tolower);
  }
};

struct StringRefIEquals {
  bool operator()(const StringRef& lhs, const StringRef& rhs) const { return lhs.iequals(rhs); }
};

} // namespace datastax

#endif
