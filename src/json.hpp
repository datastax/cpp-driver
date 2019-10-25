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

#ifndef DATASTAX_INTERNAL_JSON_HPP
#define DATASTAX_INTERNAL_JSON_HPP

#include "memory.hpp"

namespace datastax { namespace internal { namespace json {

template <class T>
static T* new_() {
  T* ptr = reinterpret_cast<T*>(Memory::malloc(sizeof(T)));
  return new (ptr) T();
}

// Doesn't work for polymorphic types
// TODO: Add static_assert() for !is_polymorphic<T>
template <class T>
static void delete_(T* ptr) {
  if (!ptr) return;
  ptr->~T();
  Memory::free(ptr);
}

}}} // namespace datastax::internal::json

#define RAPIDJSON_NAMESPACE datastax::rapidjson
#define RAPIDJSON_NAMESPACE_BEGIN \
  namespace datastax {            \
  namespace rapidjson {
#define RAPIDJSON_NAMESPACE_END \
  }                             \
  }
#define RAPIDJSON_NEW(x) ::datastax::internal::json::new_<x>
#define RAPIDJSON_DELETE(x) ::datastax::internal::json::delete_(x)

#include "third_party/rapidjson/rapidjson/document.h"
#include "third_party/rapidjson/rapidjson/stringbuffer.h"
#ifndef JSON_DEBUG
#include "third_party/rapidjson/rapidjson/writer.h"
#define JSON_WRITE_TYPE Writer
#else
#include "third_party/rapidjson/rapidjson/prettywriter.h"
#define JSON_WRITE_TYPE PrettyWriter
#endif

namespace datastax { namespace internal { namespace json {

class Allocator {
public:
  static const bool kNeedFree = true;
  void* Malloc(size_t size) { return Memory::malloc(size); }
  void* Realloc(void* ptr, size_t original_size, size_t new_size) {
    (void)original_size;
    return Memory::realloc(ptr, new_size);
  }
  static void Free(void* ptr) { Memory::free(ptr); }
};

typedef datastax::rapidjson::UTF8<> UTF8;
typedef datastax::rapidjson::MemoryPoolAllocator<json::Allocator> MemoryPoolAllocator;
typedef datastax::rapidjson::MemoryStream MemoryStream;

typedef datastax::rapidjson::GenericDocument<UTF8, MemoryPoolAllocator, json::Allocator> Document;
typedef datastax::rapidjson::GenericValue<UTF8, MemoryPoolAllocator> Value;
typedef datastax::rapidjson::GenericStringBuffer<UTF8, json::Allocator> StringBuffer;
typedef datastax::rapidjson::AutoUTFInputStream<unsigned, MemoryStream> AutoUTFMemoryInputStream;

template <typename OutputStream, typename SourceEncoding = UTF8, typename TargetEncoding = UTF8,
          typename StackAllocator = json::Allocator,
          unsigned writeFlags = datastax::rapidjson::kWriteDefaultFlags>
class Writer
    : public datastax::rapidjson::JSON_WRITE_TYPE<OutputStream, SourceEncoding, TargetEncoding,
                                                  StackAllocator, writeFlags> {
public:
  typedef datastax::rapidjson::JSON_WRITE_TYPE<OutputStream, SourceEncoding, TargetEncoding,
                                               StackAllocator, writeFlags>
      Type;

  explicit Writer(OutputStream& os, StackAllocator* stackAllocator = 0,
                  size_t levelDepth = Type::kDefaultLevelDepth)
      : Type(os, stackAllocator, levelDepth) {}

  explicit Writer(StackAllocator* allocator = 0, size_t levelDepth = Type::kDefaultLevelDepth)
      : Type(allocator, levelDepth) {}
};

}}} // namespace datastax::internal::json

#endif // DATASTAX_INTERNAL_JSON_HPP
