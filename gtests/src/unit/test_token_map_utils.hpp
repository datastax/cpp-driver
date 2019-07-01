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

#ifndef DATASTAX_TEST_TOKEN_MAP_UTILS_HPP
#define DATASTAX_TEST_TOKEN_MAP_UTILS_HPP

#include "constants.hpp"
#include "decoder.hpp"
#include "token_map_impl.hpp"
#include "uint128.hpp"

#include "third_party/mt19937_64/mt19937_64.hpp"

#define CASS_PROTOCOL_VERSION 3

using datastax::String;
using datastax::internal::OStringStream;
using datastax::internal::core::Address;
using datastax::internal::core::ByteOrderedPartitioner;
using datastax::internal::core::CollectionType;
using datastax::internal::core::DataType;
using datastax::internal::core::Decoder;
using datastax::internal::core::Host;
using datastax::internal::core::Murmur3Partitioner;
using datastax::internal::core::RandomPartitioner;
using datastax::internal::core::ResultResponse;
using datastax::internal::core::TokenMap;
using datastax::internal::core::VersionNumber;

class BufferBuilder {
public:
  char* data() const { return const_cast<char*>(buffer_.data()); }

  size_t size() const { return buffer_.size(); }

  template <class T>
  void append(T value) {
    String buffer(size_of(value), 0);
    encode(&buffer[0], value);
    buffer_.append(buffer);
  }

  template <class T>
  void append_value(T value) {
    append<int32_t>(size_of(value));
    append<T>(value);
  }

  void append_string(const String& str) {
    append<uint16_t>(str.size());
    append(str);
  }

  template <class T>
  void encode_at(size_t index, T value) {
    assert(index < buffer_.size() && index + size_of(value) < buffer_.size());
    encode(&buffer_[index], value);
  }

private:
  static size_t size_of(uint16_t value) { return sizeof(int16_t); }

  static size_t size_of(int32_t value) { return sizeof(int32_t); }

  static size_t size_of(int64_t value) { return sizeof(int64_t); }

  static size_t size_of(const String& value) { return value.size(); }

  static void encode(char* buf, uint16_t value) { datastax::internal::encode_uint16(buf, value); }

  static void encode(char* buf, int32_t value) { datastax::internal::encode_int32(buf, value); }

  static void encode(char* buf, int64_t value) { datastax::internal::encode_int64(buf, value); }

  static void encode(char* buf, const String& value) { memcpy(buf, value.data(), value.size()); }

private:
  String buffer_;
};

typedef datastax::internal::Map<String, String> ReplicationMap;

struct ColumnMetadata {
  ColumnMetadata(const String& name, const DataType::ConstPtr& data_type)
      : name(name)
      , data_type(data_type) {}
  String name;
  DataType::ConstPtr data_type;
};

typedef datastax::internal::Vector<ColumnMetadata> ColumnMetadataVec;
typedef datastax::internal::Vector<String> TokenVec;
typedef datastax::internal::Vector<Murmur3Partitioner::Token> Murmur3TokenVec;

class RowResultResponseBuilder : protected BufferBuilder {
public:
  RowResultResponseBuilder(const ColumnMetadataVec& column_metadata)
      : row_count_(0) {
    append<cass_int32_t>(CASS_RESULT_KIND_ROWS);             // Kind
    append<cass_int32_t>(CASS_RESULT_FLAG_GLOBAL_TABLESPEC); // Flags
    append<cass_int32_t>(column_metadata.size());            // Column count
    append_string("keyspace");
    append_string("table");

    for (ColumnMetadataVec::const_iterator i = column_metadata.begin(), end = column_metadata.end();
         i != end; ++i) {
      append_column_metadata(*i);
    }

    row_count_index_ = size();
    append<cass_int32_t>(0); // Row count (updated later)
  }

  void append_keyspace_row_v3(const String& keyspace_name, const ReplicationMap& replication) {
    append_value<String>(keyspace_name);

    size_t size = sizeof(int32_t);
    for (ReplicationMap::const_iterator i = replication.begin(), end = replication.end(); i != end;
         ++i) {
      size += sizeof(int32_t) + i->first.size();
      size += sizeof(int32_t) + i->second.size();
    }

    append<cass_int32_t>(size);
    append<cass_int32_t>(replication.size()); // Element count
    for (ReplicationMap::const_iterator i = replication.begin(), end = replication.end(); i != end;
         ++i) {
      append_value<String>(i->first);
      append_value<String>(i->second);
    }

    ++row_count_;
  }

  void append_keyspace_row_v3(const String& keyspace_name, const String& strategy_class,
                              const String& strategy_options) {
    append_value<String>(keyspace_name);
    append_value<String>(strategy_class);
    append_value<String>(strategy_options);

    ++row_count_;
  }

  void append_local_peers_row_v3(const TokenVec& tokens, const String& partitioner,
                                 const String& dc, const String& rack,
                                 const String& release_version) {
    append_value<String>(rack);
    append_value<String>(dc);
    append_value<String>(release_version);
    if (!partitioner.empty()) {
      append_value<String>(partitioner);
    }

    size_t size = sizeof(int32_t);
    for (TokenVec::const_iterator i = tokens.begin(), end = tokens.end(); i != end; ++i) {
      size += sizeof(int32_t) + i->size();
    }

    append<cass_int32_t>(size);
    append<cass_int32_t>(tokens.size()); // Element count
    for (TokenVec::const_iterator i = tokens.begin(), end = tokens.end(); i != end; ++i) {
      append_value<String>(*i);
    }

    ++row_count_;
  }

  void append_column_metadata(const ColumnMetadata& metadata) {
    append_string(metadata.name);
    append_data_type(metadata.data_type);
  }

  void append_data_type(const DataType::ConstPtr& data_type) {
    append<uint16_t>(data_type->value_type());

    switch (data_type->value_type()) {
      case CASS_VALUE_TYPE_LIST:
      case CASS_VALUE_TYPE_SET:
        append_data_type(CollectionType::ConstPtr(data_type)->types()[0]);
        break;
      case CASS_VALUE_TYPE_MAP:
        append_data_type(CollectionType::ConstPtr(data_type)->types()[0]);
        append_data_type(CollectionType::ConstPtr(data_type)->types()[1]);
        break;
      case CASS_VALUE_TYPE_TUPLE:
      case CASS_VALUE_TYPE_UDT:
        assert(false && "Tuples and UDTs are not supported");
        break;
      default:
        break;
    }
  }

  ResultResponse* finish() {
    encode_at(row_count_index_, row_count_);
    Decoder decoder(data(), size(), CASS_PROTOCOL_VERSION);
    result_response_.decode(decoder);
    return &result_response_;
  }

private:
  ResultResponse result_response_;
  size_t row_count_index_;
  int32_t row_count_;
};

inline String to_string(const Murmur3Partitioner::Token& token) {
  OStringStream ss;
  ss << token;
  return ss.str();
}

inline String to_string(const RandomPartitioner::Token& token) {
  numeric::uint128_t r(token.lo);
  r |= (numeric::uint128_t(token.hi) << 64);
  return r.to_string();
}

inline String to_string(const ByteOrderedPartitioner::Token& token) {
  String s;
  for (ByteOrderedPartitioner::Token::const_iterator it = token.begin(), end = token.end();
       it != end; ++it) {
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
  for (Murmur3TokenVec::const_iterator it = murmur3_tokens.begin(), end = murmur3_tokens.end();
       it != end; ++it) {
    tokens.push_back(to_string(*it));
  }
  return tokens;
}

inline void add_keyspace_simple(const String& keyspace_name, size_t replication_factor,
                                TokenMap* token_map) {

  DataType::ConstPtr varchar_data_type(new DataType(CASS_VALUE_TYPE_VARCHAR));

  ColumnMetadataVec column_metadata;
  column_metadata.push_back(ColumnMetadata("keyspace_name", varchar_data_type));
  column_metadata.push_back(ColumnMetadata(
      "replication", CollectionType::map(varchar_data_type, varchar_data_type, true)));
  RowResultResponseBuilder builder(column_metadata);

  ReplicationMap replication;
  replication["class"] = CASS_SIMPLE_STRATEGY;
  OStringStream ss;
  ss << replication_factor;
  replication["replication_factor"] = ss.str();
  builder.append_keyspace_row_v3(keyspace_name, replication);
  builder.finish();

  token_map->add_keyspaces(VersionNumber(3, 0, 0), builder.finish());
}

inline void add_keyspace_network_topology(const String& keyspace_name, ReplicationMap& replication,
                                          TokenMap* token_map) {

  DataType::ConstPtr varchar_data_type(new DataType(CASS_VALUE_TYPE_VARCHAR));

  ColumnMetadataVec column_metadata;
  column_metadata.push_back(ColumnMetadata("keyspace_name", varchar_data_type));
  column_metadata.push_back(ColumnMetadata(
      "replication", CollectionType::map(varchar_data_type, varchar_data_type, true)));
  RowResultResponseBuilder builder(column_metadata);

  replication["class"] = CASS_NETWORK_TOPOLOGY_STRATEGY;
  builder.append_keyspace_row_v3(keyspace_name, replication);
  builder.finish();

  token_map->add_keyspaces(VersionNumber(3, 0, 0), builder.finish());
}

inline Host::Ptr create_host(const Address& address, const TokenVec& tokens,
                             const String& partitioner = "", const String& dc = "dc",
                             const String& rack = "rack", const String& release_version = "3.11") {
  Host::Ptr host(new Host(address));

  DataType::ConstPtr varchar_data_type(new DataType(CASS_VALUE_TYPE_VARCHAR));

  ColumnMetadataVec column_metadata;
  column_metadata.push_back(ColumnMetadata("data_center", varchar_data_type));
  column_metadata.push_back(ColumnMetadata("rack", varchar_data_type));
  column_metadata.push_back(ColumnMetadata("release_version", varchar_data_type));
  if (!partitioner.empty()) {
    column_metadata.push_back(ColumnMetadata("partitioner", varchar_data_type));
  }
  column_metadata.push_back(
      ColumnMetadata("tokens", CollectionType::list(varchar_data_type, true)));

  RowResultResponseBuilder builder(column_metadata);
  builder.append_local_peers_row_v3(tokens, partitioner, dc, rack, release_version);

  host->set(&builder.finish()->first_row(), true);

  return host;
}

inline Host::Ptr create_host(const String& address, const TokenVec& tokens,
                             const String& partitioner = "", const String& dc = "dc",
                             const String& rack = "rack", const String& release_version = "3.11") {
  return create_host(Address(address, 9042), tokens, partitioner, dc, rack, release_version);
}

inline RandomPartitioner::Token create_random_token(const String& s) {
  RandomPartitioner::Token token;
  numeric::uint128_t i(s);
  token.lo = (i & numeric::uint128_t("0xFFFFFFFFFFFFFFFF")).to_base_type();
  token.hi = (i >> 64).to_base_type();
  return token;
}

inline ByteOrderedPartitioner::Token create_byte_ordered_token(const String& s) {
  ByteOrderedPartitioner::Token token;
  for (String::const_iterator i = s.begin(), end = s.end(); i != end; ++i) {
    token.push_back(static_cast<uint8_t>(*i));
  }
  return token;
}

#endif
