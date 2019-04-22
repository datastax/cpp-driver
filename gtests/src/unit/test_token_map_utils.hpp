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

#ifndef __CASS_TEST_TOKEN_MAP_UTILS_HPP_INCLUDED__
#define __CASS_TEST_TOKEN_MAP_UTILS_HPP_INCLUDED__

#include "constants.hpp"
#include "decoder.hpp"
#include "token_map_impl.hpp"
#include "uint128.hpp"

#include "third_party/mt19937_64/mt19937_64.hpp"

#define CASS_PROTOCOL_VERSION 3

class BufferBuilder {
public:
  char* data() const {
    return const_cast<char*>(buffer_.data());
  }

  size_t size() const {
    return buffer_.size();
  }

  template<class T>
  void append(T value) {
    cass::String buffer(size_of(value), 0);
    encode(&buffer[0], value);
    buffer_.append(buffer);
  }

  template<class T>
  void append_value(T value) {
    append<int32_t>(size_of(value));
    append<T>(value);
  }

  void append_string(const cass::String& str) {
    append<uint16_t>(str.size());
    append(str);
  }

  template<class T>
  void encode_at(size_t index, T value) {
    assert(index < buffer_.size() && index + size_of(value) < buffer_.size());
    encode(&buffer_[index], value);
  }

private:
  static size_t size_of(uint16_t value) {
    return sizeof(int16_t);
  }

  static size_t size_of(int32_t value) {
    return sizeof(int32_t);
  }

  static size_t size_of(int64_t value) {
    return sizeof(int64_t);
  }

  static size_t size_of(const cass::String& value) {
    return value.size();
  }

  static void encode(char* buf, uint16_t value) {
    cass::encode_uint16(buf, value);
  }

  static void encode(char* buf, int32_t value) {
    cass::encode_int32(buf, value);
  }

  static void encode(char* buf, int64_t value) {
    cass::encode_int64(buf, value);
  }

  static void encode(char* buf, const cass::String& value) {
    memcpy(buf, value.data(), value.size());
  }

private:
  cass::String buffer_;
};

typedef cass::Map<cass::String, cass::String> ReplicationMap;

struct ColumnMetadata {
  ColumnMetadata(const cass::String& name, const cass::DataType::ConstPtr& data_type)
    : name(name)
    , data_type(data_type) { }
  cass::String name;
  cass::DataType::ConstPtr data_type;
};

typedef cass::Vector<ColumnMetadata> ColumnMetadataVec;
typedef cass::Vector<cass::String> TokenVec;
typedef cass::Vector<cass::Murmur3Partitioner::Token> Murmur3TokenVec;

class RowResultResponseBuilder : protected BufferBuilder {
public:
  RowResultResponseBuilder(const ColumnMetadataVec& column_metadata)
    : row_count_(0) {
    append<cass_int32_t>(CASS_RESULT_KIND_ROWS); // Kind
    append<cass_int32_t>(CASS_RESULT_FLAG_GLOBAL_TABLESPEC); // Flags
    append<cass_int32_t>(column_metadata.size()); // Column count
    append_string("keyspace");
    append_string("table");

    for (ColumnMetadataVec::const_iterator i = column_metadata.begin(),
          end = column_metadata.end(); i != end; ++i) {
      append_column_metadata(*i);
    }

    row_count_index_ = size();
    append<cass_int32_t>(0); // Row count (updated later)
  }

  void append_keyspace_row_v3(const cass::String& keyspace_name,
                              const ReplicationMap& replication) {
    append_value<cass::String>(keyspace_name);

    size_t size = sizeof(int32_t);
    for (ReplicationMap::const_iterator i = replication.begin(),
         end = replication.end(); i != end; ++i) {
      size += sizeof(int32_t) + i->first.size();
      size += sizeof(int32_t) + i->second.size();
    }

    append<cass_int32_t>(size);
    append<cass_int32_t>(replication.size()); // Element count
    for (ReplicationMap::const_iterator i = replication.begin(),
         end = replication.end(); i != end; ++i) {
      append_value<cass::String>(i->first);
      append_value<cass::String>(i->second);
    }

    ++row_count_;
  }

  void append_keyspace_row_v3(const cass::String& keyspace_name,
                              const cass::String& strategy_class,
                              const cass::String& strategy_options) {
    append_value<cass::String>(keyspace_name);
    append_value<cass::String>(strategy_class);
    append_value<cass::String>(strategy_options);

    ++row_count_;
  }

  void append_local_peers_row_v3(const TokenVec& tokens,
                                 const cass::String& partitioner,
                                 const cass::String& dc,
                                 const cass::String& rack,
                                 const cass::String& release_version) {
    append_value<cass::String>(rack);
    append_value<cass::String>(dc);
    append_value<cass::String>(release_version);
    if (!partitioner.empty()) {
      append_value<cass::String>(partitioner);
    }

    size_t size = sizeof(int32_t);
    for (TokenVec::const_iterator i = tokens.begin(),
         end = tokens.end(); i != end; ++i) {
      size += sizeof(int32_t) + i->size();
    }

    append<cass_int32_t>(size);
    append<cass_int32_t>(tokens.size()); // Element count
    for (TokenVec::const_iterator i = tokens.begin(),
         end = tokens.end(); i != end; ++i) {
      append_value<cass::String>(*i);
    }

    ++row_count_;
  }

  void append_column_metadata(const ColumnMetadata& metadata) {
    append_string(metadata.name);
    append_data_type(metadata.data_type);
  }

  void append_data_type(const cass::DataType::ConstPtr& data_type) {
    append<uint16_t>(data_type->value_type());

    switch (data_type->value_type()) {
      case CASS_VALUE_TYPE_LIST:
      case CASS_VALUE_TYPE_SET:
        append_data_type(cass::CollectionType::ConstPtr(data_type)->types()[0]);
        break;
      case CASS_VALUE_TYPE_MAP:
        append_data_type(cass::CollectionType::ConstPtr(data_type)->types()[0]);
        append_data_type(cass::CollectionType::ConstPtr(data_type)->types()[1]);
        break;
      case CASS_VALUE_TYPE_TUPLE:
      case CASS_VALUE_TYPE_UDT:
        assert(false && "Tuples and UDTs are not supported");
        break;
      default:
        break;
    }
  }

  cass::ResultResponse* finish() {
    encode_at(row_count_index_, row_count_);
    cass::Decoder decoder(data(), size(), CASS_PROTOCOL_VERSION);
    result_response_.decode(decoder);
    return &result_response_;
  }

private:
  cass::ResultResponse result_response_;
  size_t row_count_index_;
  int32_t row_count_;
};

inline cass::String to_string(const cass::Murmur3Partitioner::Token& token) {
  cass::OStringStream ss;
  ss << token;
  return ss.str();
}

inline cass::String to_string(const cass::RandomPartitioner::Token& token) {
  numeric::uint128_t r(token.lo);
  r |= (numeric::uint128_t(token.hi) << 64);
  return r.to_string();
}

inline cass::String to_string(const cass::ByteOrderedPartitioner::Token& token) {
  cass::String s;
  for (cass::ByteOrderedPartitioner::Token::const_iterator it = token.begin(),
       end = token.end(); it != end; ++it) {
    s.push_back(static_cast<char>(*it));
  }
  return s;
}

template <class TokenType>
inline TokenVec single_token(const TokenType token) {
  return TokenVec(1, to_string(token));
}

inline TokenVec random_murmur3_tokens(MT19937_64& rng, size_t num_tokens) {
  TokenVec tokens;
  for (size_t i = 0; i < num_tokens; ++i) {
    tokens.push_back(to_string(rng()));
  }
  return tokens;
}

inline TokenVec murmur3_tokens(const Murmur3TokenVec& murmur3_tokens) {
  TokenVec tokens;
  for (Murmur3TokenVec::const_iterator it = murmur3_tokens.begin(),
       end = murmur3_tokens.end(); it != end; ++it) {
    tokens.push_back(to_string(*it));
  }
  return tokens;
}

inline void add_keyspace_simple(const cass::String& keyspace_name,
                                size_t replication_factor,
                                cass::TokenMap* token_map) {

  cass::DataType::ConstPtr varchar_data_type(new cass::DataType(CASS_VALUE_TYPE_VARCHAR));

  ColumnMetadataVec column_metadata;
  column_metadata.push_back(ColumnMetadata("keyspace_name", varchar_data_type));
  column_metadata.push_back(ColumnMetadata("replication", cass::CollectionType::map(varchar_data_type, varchar_data_type, true)));
  RowResultResponseBuilder builder(column_metadata);

  ReplicationMap replication;
  replication["class"] = CASS_SIMPLE_STRATEGY;
  cass::OStringStream ss;
  ss << replication_factor;
  replication["replication_factor"] = ss.str();
  builder.append_keyspace_row_v3(keyspace_name, replication);
  builder.finish();

  token_map->add_keyspaces(cass::VersionNumber(3, 0, 0), builder.finish());
}

inline void add_keyspace_network_topology(const cass::String& keyspace_name,
                                          ReplicationMap& replication,
                                          cass::TokenMap* token_map) {

  cass::DataType::ConstPtr varchar_data_type(new cass::DataType(CASS_VALUE_TYPE_VARCHAR));

  ColumnMetadataVec column_metadata;
  column_metadata.push_back(ColumnMetadata("keyspace_name", varchar_data_type));
  column_metadata.push_back(ColumnMetadata("replication", cass::CollectionType::map(varchar_data_type, varchar_data_type, true)));
  RowResultResponseBuilder builder(column_metadata);

  replication["class"] = CASS_NETWORK_TOPOLOGY_STRATEGY;
  builder.append_keyspace_row_v3(keyspace_name, replication);
  builder.finish();

  token_map->add_keyspaces(cass::VersionNumber(3, 0, 0), builder.finish());
}

inline cass::Host::Ptr create_host(const cass::Address& address,
                                   const TokenVec& tokens,
                                   const cass::String& partitioner = "",
                                   const cass::String& dc = "dc",
                                   const cass::String& rack = "rack",
                                   const cass::String& release_version = "3.11") {
  cass::Host::Ptr host(new cass::Host(address));

  cass::DataType::ConstPtr varchar_data_type(new cass::DataType(CASS_VALUE_TYPE_VARCHAR));

  ColumnMetadataVec column_metadata;
  column_metadata.push_back(ColumnMetadata("data_center", varchar_data_type));
  column_metadata.push_back(ColumnMetadata("rack", varchar_data_type));
  column_metadata.push_back(ColumnMetadata("release_version", varchar_data_type));
  if (!partitioner.empty()) {
    column_metadata.push_back(ColumnMetadata("partitioner", varchar_data_type));
  }
  column_metadata.push_back(ColumnMetadata("tokens", cass::CollectionType::list(varchar_data_type, true)));

  RowResultResponseBuilder builder(column_metadata);
  builder.append_local_peers_row_v3(tokens, partitioner, dc, rack, release_version);

  host->set(&builder.finish()->first_row(), true);

  return host;
}

inline cass::Host::Ptr create_host(const cass::String& address,
                                   const TokenVec& tokens,
                                   const cass::String& partitioner = "",
                                   const cass::String& dc = "dc",
                                   const cass::String& rack = "rack",
                                   const cass::String& release_version = "3.11") {
  return create_host(cass::Address(address, 9042), tokens, partitioner, dc, rack, release_version);
}


inline cass::RandomPartitioner::Token create_random_token(const cass::String& s) {
  cass::RandomPartitioner::Token token;
  numeric::uint128_t i(s);
  token.lo = (i & numeric::uint128_t("0xFFFFFFFFFFFFFFFF")).to_base_type();
  token.hi = (i >> 64).to_base_type();
  return token;
}

inline cass::ByteOrderedPartitioner::Token create_byte_ordered_token(const cass::String& s) {
  cass::ByteOrderedPartitioner::Token token;
  for (cass::String::const_iterator i = s.begin(),
       end = s.end(); i != end; ++i) {
    token.push_back(static_cast<uint8_t>(*i));
  }
  return token;
}

#endif
