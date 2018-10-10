/*
  Copyright (c) DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __DSE_JSON_HPP_INCLUDED__
#define __DSE_JSON_HPP_INCLUDED__

#include "memory.hpp"

#define RAPIDJSON_NEW(x) cass::Memory::allocate<x>
#define RAPIDJSON_DELETE(x) cass::Memory::deallocate(x)

#include "third_party/rapidjson/rapidjson/document.h"
#include "third_party/rapidjson/rapidjson/writer.h"
#include "third_party/rapidjson/rapidjson/stringbuffer.h"


namespace cass {
namespace json {

class Allocator {
public:
    static const bool kNeedFree = true;
    void* Malloc(size_t size) {
      return Memory::malloc(size);
    }
    void* Realloc(void* ptr, size_t original_size, size_t new_size) {
      (void)original_size;
      return Memory::realloc(ptr, new_size);
    }
    static void Free(void *ptr) {
      Memory::free(ptr);
    }
};

typedef rapidjson::GenericDocument<rapidjson::UTF8<>, rapidjson::MemoryPoolAllocator<json::Allocator>, json::Allocator> Document;
typedef rapidjson::GenericValue<rapidjson::UTF8<>, rapidjson::MemoryPoolAllocator<json::Allocator> > Value;
typedef rapidjson::GenericStringBuffer<rapidjson::UTF8<>, json::Allocator> StringBuffer;

template<typename OutputStream, typename SourceEncoding = rapidjson::UTF8<>, typename TargetEncoding = rapidjson::UTF8<>, typename StackAllocator = json::Allocator, unsigned writeFlags = rapidjson::kWriteDefaultFlags>
class Writer : public rapidjson::Writer<OutputStream, SourceEncoding, TargetEncoding, StackAllocator, writeFlags> {
public:
    typedef rapidjson::Writer<OutputStream, SourceEncoding, TargetEncoding, StackAllocator, writeFlags> Type;

    explicit Writer(OutputStream& os, StackAllocator* stackAllocator = 0, size_t levelDepth = Type::kDefaultLevelDepth) :
        Type(os, stackAllocator, levelDepth) { }

    explicit Writer(StackAllocator* allocator = 0, size_t levelDepth = Type::kDefaultLevelDepth) :
        Type(allocator, levelDepth) { }
};

} } // namespace cass::json

#endif
