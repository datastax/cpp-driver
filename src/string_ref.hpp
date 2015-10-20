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

#ifndef __CASS_STRING_REF_HPP_INCLUDED__
#define __CASS_STRING_REF_HPP_INCLUDED__

#include <algorithm>
#include <assert.h>
#include <cctype>
#include <stddef.h>
#include <string>
#include <string.h>
#include <vector>

namespace cass {

template<class IsEqual>
int compare(const char* s1,  const char* s2,
            size_t length, IsEqual is_equal) {
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
    bool operator()(char a, char b) const {
      return a == b;
    }
  };

  struct IsEqualInsensitive {
    bool operator()(char a, char b) const {
      return std::toupper(a) == std::toupper(b);
    }
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
    , length_(str != NULL ? strlen(str) : 0) {}

  StringRef(const std::string& str)
    : ptr_(str.data())
    , length_(str.size()) {}

  const char* data() const { return ptr_; }
  size_t size() const { return length_; }
  size_t length() const { return length_; }

  iterator begin() const { return ptr_; }
  iterator end() const { return ptr_ + length_; }

  char front() const { return ptr_[0]; }
  char back() const { return ptr_[length_ - 1]; }

  std::string to_string() const {
    return std::string(ptr_, length_);
  }

  StringRef substr(size_t pos = 0, size_t length = npos) {
    assert(pos < length_);
    return StringRef(ptr_ + pos, std::min(length_ - pos, length));
  }

  int compare(const StringRef& ref) const {
    return compare(ref, IsEqual());
  }

  int icompare(const StringRef& ref) const {
    return compare(ref, IsEqualInsensitive());
  }

  bool equals(const StringRef& ref) const {
    return compare(ref) == 0;
  }

  bool iequals(const StringRef& ref) const {
    return icompare(ref) == 0;
  }

  bool operator==(const StringRef& ref) const {
    return equals(ref);
  }

  bool operator!=(const StringRef& ref) const {
    return !equals(ref);
  }

  bool operator<(const StringRef& ref) const {
    return compare(ref) < 0;
  }

private:
  template<class IsEqual>
  int compare(const StringRef& ref, IsEqual is_equal) const {
    if (length_ < ref.length_) {
      return -1;
    } else if(length_ > ref.length_) {
      return 1;
    }
    return cass::compare(ptr_, ref.ptr_, length_, is_equal);
  }

  const char* ptr_;
  size_t length_;
};

typedef std::vector<StringRef> StringRefVec;

inline bool starts_with(const StringRef& input, const StringRef& target) {
  return input.length() >= target.length() &&
      cass::compare(input.data(), target.data(), target.size(),
                    StringRef::IsEqual()) == 0;
}

inline bool ends_with(const StringRef& input, const StringRef& target) {
  return input.length() >= target.length() &&
      cass::compare(input.data() + (input.length() - target.length()),
                    target.data(), target.size(), StringRef::IsEqual()) == 0;
}

inline bool iequals(const StringRef& input, const StringRef& target) {
  return input.iequals(target);
}

} // namespace cass

#endif
