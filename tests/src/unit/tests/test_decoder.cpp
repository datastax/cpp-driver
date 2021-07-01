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

#include <gtest/gtest.h>

#include "decoder.hpp"
#include "logger.hpp"

using namespace datastax;
using namespace datastax::internal;
using namespace datastax::internal::core;

// Testing decoder class to expose protected testing methods
class TestDecoder : public Decoder {
public:
  TestDecoder(const char* input, size_t length,
              ProtocolVersion protocol_version = ProtocolVersion::highest_supported())
      : Decoder(input, length, protocol_version) {}

  inline const char* buffer() const { return Decoder::buffer(); }
  inline size_t remaining() const { return Decoder::remaining(); }
};

class DecoderUnitTest : public testing::Test {
public:
  void SetUp() {
    failure_logged_ = false;
    warning_logged_ = false;
    cass_log_set_level(CASS_LOG_WARN);
    Logger::set_callback(DecoderUnitTest::log, NULL);
  }

  static void log(const CassLogMessage* message, void* data) {
    std::string function = message->function;
    if (message->severity == CASS_LOG_ERROR && function.find("Decoder::") != std::string::npos) {
      failure_logged_ = true;
    } else if (message->severity == CASS_LOG_WARN &&
               function.find("Decoder::") != std::string::npos) {
      warning_logged_ = true;
    }
  }

  static bool warning_logged_;
  static bool failure_logged_;
};

bool DecoderUnitTest::failure_logged_ = false;
bool DecoderUnitTest::warning_logged_ = false;

TEST_F(DecoderUnitTest, DecodeByte) {
  const signed char input[2] = { -1, 0 };
  TestDecoder decoder((const char*)input, 2);
  uint8_t value = 0;

  // SUCCESS
  ASSERT_TRUE(decoder.decode_byte(value));
  ASSERT_EQ((char*)&input[1], decoder.buffer());
  ASSERT_EQ(1ul, decoder.remaining());
  ASSERT_EQ(std::numeric_limits<uint8_t>::max(), value);
  ASSERT_TRUE(decoder.decode_byte(value));
  ASSERT_EQ(0ul, decoder.remaining());
  ASSERT_EQ(std::numeric_limits<uint8_t>::min(), value);

  // FAIL
  ASSERT_FALSE(decoder.decode_byte(value));
  ASSERT_TRUE(failure_logged_);
}

TEST_F(DecoderUnitTest, AsByte) {
  const signed char input[1] = { -1 };
  TestDecoder decoder((const char*)input, 1);
  uint8_t value = 0;

  // SUCCESS
  for (int i = 0; i < 10; ++i) {
    ASSERT_TRUE(decoder.as_byte(&value));
    ASSERT_EQ((const char*)&input[0], decoder.buffer());
    ASSERT_EQ(1ul, decoder.remaining());
    ASSERT_EQ(std::numeric_limits<uint8_t>::max(), value);
  }

  // Decode byte to finish decoding buffer
  ASSERT_TRUE(decoder.decode_byte(value));

  // FAIL
  ASSERT_FALSE(decoder.as_byte(&value));
  ASSERT_TRUE(failure_logged_);
}

TEST_F(DecoderUnitTest, AsBool) {
  const signed char input[2] = { 0, 1 };
  TestDecoder decoder((const char*)input, 2);
  bool value = false;

  // SUCCESS (false)
  for (int i = 0; i < 10; ++i) {
    ASSERT_TRUE(decoder.as_bool(&value));
    ASSERT_EQ((const char*)&input[0], decoder.buffer());
    ASSERT_EQ(2ul, decoder.remaining());
    ASSERT_FALSE(value);
  }

  // Decode byte to move to next bool in buffer
  uint8_t byte_value = 0;
  ASSERT_TRUE(decoder.decode_byte(byte_value));

  // SUCCESS (true)
  for (int i = 0; i < 10; ++i) {
    ASSERT_TRUE(decoder.as_bool(&value));
    ASSERT_EQ((const char*)&input[1], decoder.buffer());
    ASSERT_EQ(1ul, decoder.remaining());
    ASSERT_TRUE(value);
  }

  // Decode byte to finish decoding buffer
  ASSERT_TRUE(decoder.decode_byte(byte_value));

  // FAIL
  ASSERT_FALSE(decoder.as_bool(&value));
  ASSERT_TRUE(failure_logged_);
}

TEST_F(DecoderUnitTest, DecodeInt8) {
  const signed char input[2] = { -128, 127 };
  TestDecoder decoder((const char*)input, 2);
  int8_t value = 0;

  // SUCCESS
  ASSERT_TRUE(decoder.decode_int8(value));
  ASSERT_EQ((const char*)&input[1], decoder.buffer());
  ASSERT_EQ(1ul, decoder.remaining());
  ASSERT_EQ(std::numeric_limits<int8_t>::min(), value);
  ASSERT_TRUE(decoder.decode_int8(value));
  ASSERT_EQ(0ul, decoder.remaining());
  ASSERT_EQ(std::numeric_limits<int8_t>::max(), value);

  // FAIL
  ASSERT_FALSE(decoder.decode_int8(value));
  ASSERT_TRUE(failure_logged_);
}

TEST_F(DecoderUnitTest, AsInt8) {
  const signed char input[1] = { -128 };
  TestDecoder decoder((const char*)input, 1);
  int8_t value = 0;

  // SUCCESS
  for (int i = 0; i < 10; ++i) {
    ASSERT_TRUE(decoder.as_int8(&value));
    ASSERT_EQ((const char*)&input[0], decoder.buffer());
    ASSERT_EQ(1ul, decoder.remaining());
    ASSERT_EQ(std::numeric_limits<int8_t>::min(), value);
  }

  // Decode int8 to finish decoding buffer
  int8_t int8_value = 0;
  ASSERT_TRUE(decoder.decode_int8(int8_value));

  // FAIL
  ASSERT_FALSE(decoder.as_int8(&value));
  ASSERT_TRUE(failure_logged_);
}

TEST_F(DecoderUnitTest, DecodeUInt16) {
  const signed char input[4] = { -1, -1, 0, 0 };
  TestDecoder decoder((const char*)input, 4);
  uint16_t value = 0;

  // SUCCESS
  ASSERT_TRUE(decoder.decode_uint16(value));
  ASSERT_EQ((const char*)&input[2], decoder.buffer());
  ASSERT_EQ(2ul, decoder.remaining());
  ASSERT_EQ(std::numeric_limits<uint16_t>::max(), value);
  ASSERT_TRUE(decoder.decode_uint16(value));
  ASSERT_EQ(0ul, decoder.remaining());
  ASSERT_EQ(std::numeric_limits<uint16_t>::min(), value);

  // FAIL
  ASSERT_FALSE(decoder.decode_uint16(value));
  ASSERT_TRUE(failure_logged_);
}

TEST_F(DecoderUnitTest, DecodeInt16) {
  const signed char input[4] = { -128, 0, 127, -1 };
  TestDecoder decoder((const char*)input, 4);
  int16_t value = 0;

  // SUCCESS
  ASSERT_TRUE(decoder.decode_int16(value));
  ASSERT_EQ((const char*)&input[2], decoder.buffer());
  ASSERT_EQ(2ul, decoder.remaining());
  ASSERT_EQ(std::numeric_limits<int16_t>::min(), value);
  ASSERT_TRUE(decoder.decode_int16(value));
  ASSERT_EQ(0ul, decoder.remaining());
  ASSERT_EQ(std::numeric_limits<int16_t>::max(), value);

  // FAIL
  ASSERT_FALSE(decoder.decode_int16(value));
  ASSERT_TRUE(failure_logged_);
}

TEST_F(DecoderUnitTest, AsInt16) {
  const signed char input[2] = { -128, 0 };
  TestDecoder decoder((const char*)input, 2);
  int16_t value = 0;

  // SUCCESS
  for (int i = 0; i < 10; ++i) {
    ASSERT_TRUE(decoder.as_int16(&value));
    ASSERT_EQ((const char*)&input[0], decoder.buffer());
    ASSERT_EQ(2ul, decoder.remaining());
    ASSERT_EQ(std::numeric_limits<int16_t>::min(), value);
  }

  // Decode int16 to finish decoding buffer
  int16_t int16_value = 0;
  ASSERT_TRUE(decoder.decode_int16(int16_value));

  // FAIL
  ASSERT_FALSE(decoder.as_int16(&value));
  ASSERT_TRUE(failure_logged_);
}

TEST_F(DecoderUnitTest, DecodeUInt32) {
  const signed char input[8] = { -1, -1, -1, -1, 0, 0, 0, 0 };
  TestDecoder decoder((const char*)input, 8);
  uint32_t value = 0;

  // SUCCESS
  ASSERT_TRUE(decoder.decode_uint32(value));
  ASSERT_EQ((const char*)&input[4], decoder.buffer());
  ASSERT_EQ(4ul, decoder.remaining());
  ASSERT_EQ(std::numeric_limits<uint32_t>::max(), value);
  ASSERT_TRUE(decoder.decode_uint32(value));
  ASSERT_EQ(std::numeric_limits<uint32_t>::min(), value);
  ASSERT_EQ(0ul, decoder.remaining());

  // FAIL
  ASSERT_FALSE(decoder.decode_uint32(value));
  ASSERT_TRUE(failure_logged_);
}

TEST_F(DecoderUnitTest, AsUInt32) {
  const signed char input[4] = { -1, -1, -1, -1 };
  TestDecoder decoder((const char*)input, 4);
  uint32_t value = 0;

  // SUCCESS
  for (int i = 0; i < 10; ++i) {
    ASSERT_TRUE(decoder.as_uint32(&value));
    ASSERT_EQ((const char*)&input[0], decoder.buffer());
    ASSERT_EQ(std::numeric_limits<uint32_t>::max(), value);
  }

  // Decode uint32 to finish decoding buffer
  uint32_t uint32_value = 0;
  ASSERT_TRUE(decoder.decode_uint32(uint32_value));

  // FAIL
  ASSERT_FALSE(decoder.as_uint32(&value));
  ASSERT_TRUE(failure_logged_);
}

TEST_F(DecoderUnitTest, DecodeInt32) {
  const signed char input[8] = { -128, 0, 0, 0, 127, -1, -1, -1 };
  TestDecoder decoder((const char*)input, 8);
  int32_t value = 0;

  // SUCCESS
  ASSERT_TRUE(decoder.decode_int32(value));
  ASSERT_EQ((const char*)&input[4], decoder.buffer());
  ASSERT_EQ(4ul, decoder.remaining());
  ASSERT_EQ(std::numeric_limits<int32_t>::min(), value);
  ASSERT_TRUE(decoder.decode_int32(value));
  ASSERT_EQ(0ul, decoder.remaining());
  ASSERT_EQ(std::numeric_limits<int32_t>::max(), value);

  // FAIL
  ASSERT_FALSE(decoder.decode_int32(value));
  ASSERT_TRUE(failure_logged_);
}

TEST_F(DecoderUnitTest, AsInt32) {
  const signed char input[4] = { -128, 0, 0, 0 };
  TestDecoder decoder((const char*)input, 4);
  int32_t value = 0;

  // SUCCESS
  for (int i = 0; i < 10; ++i) {
    ASSERT_TRUE(decoder.as_int32(&value));
    ASSERT_EQ((const char*)&input[0], decoder.buffer());
    ASSERT_EQ(4ul, decoder.remaining());
    ASSERT_EQ(std::numeric_limits<int32_t>::min(), value);
  }

  // Decode int32 to finish decoding buffer
  int32_t int32_value = 0;
  ASSERT_TRUE(decoder.decode_int32(int32_value));

  // FAIL
  ASSERT_FALSE(decoder.as_int32(&value));
  ASSERT_TRUE(failure_logged_);
}

TEST_F(DecoderUnitTest, DecodeInt64) {
  const signed char input[16] = { -128, 0, 0, 0, 0, 0, 0, 0, 127, -1, -1, -1, -1, -1, -1, -1 };
  TestDecoder decoder((const char*)input, 16);
  int64_t value = 0;

  // SUCCESS
  ASSERT_TRUE(decoder.decode_int64(value));
  ASSERT_EQ((const char*)&input[8], decoder.buffer());
  ASSERT_EQ(8ul, decoder.remaining());
  ASSERT_EQ(std::numeric_limits<int64_t>::min(), value);
  ASSERT_TRUE(decoder.decode_int64(value));
  ASSERT_EQ(0ul, decoder.remaining());
  ASSERT_EQ(std::numeric_limits<int64_t>::max(), value);

  // FAIL
  ASSERT_FALSE(decoder.decode_int64(value));
  ASSERT_TRUE(failure_logged_);
}

TEST_F(DecoderUnitTest, AsInt64) {
  const signed char input[8] = { -128, 0, 0, 0, 0, 0, 0, 0 };
  TestDecoder decoder((const char*)input, 8);
  cass_int64_t value = 0;

  // SUCCESS
  for (int i = 0; i < 10; ++i) {
    ASSERT_TRUE(decoder.as_int64(&value));
    ASSERT_EQ((const char*)&input[0], decoder.buffer());
    ASSERT_EQ(8ul, decoder.remaining());
    ASSERT_EQ(std::numeric_limits<cass_int64_t>::min(), value);
  }

  // Decode in64 to finish decoding buffer
  int64_t int64_value = 0;
  ASSERT_TRUE(decoder.decode_int64(int64_value));

  // FAIL
  ASSERT_FALSE(decoder.as_int64(&value));
  ASSERT_TRUE(failure_logged_);
}

TEST_F(DecoderUnitTest, DecodeFloat) {
  const signed char input[8] = { 0, -128, 0, 0, 127, 127, -1, -1 };
  TestDecoder decoder((const char*)input, 8);
  float value = 0;

  // SUCCESS
  ASSERT_TRUE(decoder.decode_float(value));
  ASSERT_EQ((const char*)&input[4], decoder.buffer());
  ASSERT_EQ(4ul, decoder.remaining());
  ASSERT_EQ(std::numeric_limits<float>::min(), value);
  ASSERT_TRUE(decoder.decode_float(value));
  ASSERT_EQ(0ul, decoder.remaining());
  ASSERT_EQ(std::numeric_limits<float>::max(), value);

  // FAIL
  ASSERT_FALSE(decoder.decode_float(value));
  ASSERT_TRUE(failure_logged_);
}

TEST_F(DecoderUnitTest, AsFloat) {
  const signed char input[4] = { 0, -128, 0, 0 };
  TestDecoder decoder((const char*)input, 4);
  float value = 0.0f;

  // SUCCESS
  for (int i = 0; i < 10; ++i) {
    ASSERT_TRUE(decoder.as_float(&value));
    ASSERT_EQ((const char*)&input[0], decoder.buffer());
    ASSERT_EQ(4ul, decoder.remaining());
    ASSERT_EQ(std::numeric_limits<float>::min(), value);
  }

  // Decode int32 to finish decoding buffer
  float float_value = 0;
  ASSERT_TRUE(decoder.decode_float(float_value));

  // FAIL
  ASSERT_FALSE(decoder.as_float(&value));
  ASSERT_TRUE(failure_logged_);
}

TEST_F(DecoderUnitTest, DecodeDouble) {
  const signed char input[16] = { 0, 16, 0, 0, 0, 0, 0, 0, 127, -17, -1, -1, -1, -1, -1, -1 };
  TestDecoder decoder((const char*)input, 16);
  double value = 0;

  // SUCCESS
  ASSERT_TRUE(decoder.decode_double(value));
  ASSERT_EQ((const char*)&input[8], decoder.buffer());
  ASSERT_EQ(8ul, decoder.remaining());
  ASSERT_EQ(std::numeric_limits<double>::min(), value);
  ASSERT_TRUE(decoder.decode_double(value));
  ASSERT_EQ(0ul, decoder.remaining());
  ASSERT_EQ(std::numeric_limits<double>::max(), value);

  // FAIL
  ASSERT_FALSE(decoder.decode_double(value));
  ASSERT_TRUE(failure_logged_);
}

TEST_F(DecoderUnitTest, AsDouble) {
  const signed char input[8] = { 0, 16, 0, 0, 0, 0, 0, 0 };
  TestDecoder decoder((const char*)input, 8);
  double value = 0.0;

  // SUCCESS
  for (int i = 0; i < 10; ++i) {
    ASSERT_TRUE(decoder.as_double(&value));
    ASSERT_EQ((const char*)&input[0], decoder.buffer());
    ASSERT_EQ(8ul, decoder.remaining());
    ASSERT_EQ(std::numeric_limits<double>::min(), value);
  }

  // Decode int64 to finish decoding buffer
  double double_value = 0;
  ASSERT_TRUE(decoder.decode_double(double_value));

  // FAIL
  ASSERT_FALSE(decoder.as_double(&value));
  ASSERT_TRUE(failure_logged_);
}

TEST_F(DecoderUnitTest, DecodeString) {
  const signed char input[17] = { 0, 8, 68, 97, 116, 97, 83, 116, 97, 120, // DataStax
                                  0, 5, 67, 47, 67,  43, 43 };             // C/C++
  TestDecoder decoder((const char*)input, 17);
  const char* value = NULL;
  size_t value_size = 0;

  // SUCCESS
  ASSERT_TRUE(decoder.decode_string(&value, value_size));
  ASSERT_EQ((const char*)&input[10], decoder.buffer());
  ASSERT_EQ(7ul, decoder.remaining());
  ASSERT_EQ(8ul, value_size);
  ASSERT_STREQ("DataStax", std::string(value, value_size).c_str());
  ASSERT_TRUE(decoder.decode_string(&value, value_size));
  ASSERT_EQ(0ul, decoder.remaining());
  ASSERT_EQ(5ul, value_size);
  ASSERT_STREQ("C/C++", std::string(value, value_size).c_str());

  // FAIL
  ASSERT_FALSE(decoder.decode_string(&value, value_size));
  ASSERT_TRUE(failure_logged_);
}

TEST_F(DecoderUnitTest, DecodeStringRef) {
  const signed char input[17] = { 0, 8, 68, 97, 116, 97, 83, 116, 97, 120, // DataStax
                                  0, 5, 67, 47, 67,  43, 43 };             // C/C++
  TestDecoder decoder((const char*)input, 17);
  StringRef value;

  // SUCCESS
  ASSERT_TRUE(decoder.decode_string(&value));
  ASSERT_EQ((const char*)&input[10], decoder.buffer());
  ASSERT_EQ(7ul, decoder.remaining());
  ASSERT_EQ(8ul, value.size());
  ASSERT_STREQ("DataStax", std::string(value.data(), value.size()).c_str());
  ASSERT_TRUE(decoder.decode_string(&value));
  ASSERT_EQ(0ul, decoder.remaining());
  ASSERT_EQ(5ul, value.size());
  ASSERT_STREQ("C/C++", std::string(value.data(), value.size()).c_str());

  // FAIL
  ASSERT_FALSE(decoder.decode_string(&value));
  ASSERT_TRUE(failure_logged_);
}

TEST_F(DecoderUnitTest, DecodeLongString) {
  const signed char input[21] = { 0, 0, 0, 8, 68, 97, 116, 97, 83, 116, 97, 120, // DataStax
                                  0, 0, 0, 5, 67, 47, 67,  43, 43 };             // C/C++
  TestDecoder decoder((const char*)input, 21);
  const char* value = NULL;
  size_t value_size = 0;

  // SUCCESS
  ASSERT_TRUE(decoder.decode_long_string(&value, value_size));
  ASSERT_EQ((const char*)&input[12], decoder.buffer());
  ASSERT_EQ(9ul, decoder.remaining());
  ASSERT_EQ(8ul, value_size);
  ASSERT_STREQ("DataStax", std::string(value, value_size).c_str());
  ASSERT_TRUE(decoder.decode_long_string(&value, value_size));
  ASSERT_EQ(0ul, decoder.remaining());
  ASSERT_EQ(5ul, value_size);
  ASSERT_STREQ("C/C++", std::string(value, value_size).c_str());

  // FAIL
  ASSERT_FALSE(decoder.decode_long_string(&value, value_size));
  ASSERT_TRUE(failure_logged_);
}

TEST_F(DecoderUnitTest, DecodeBytes) {
  const signed char input[21] = { 0, 0, 0, 8, 68, 97, 116, 97, 83, 116, 97, 120, // DataStax
                                  0, 0, 0, 5, 67, 47, 67,  43, 43 };             // C/C++
  TestDecoder decoder((const char*)input, 21);
  const char* value = NULL;
  size_t value_size = 0;

  // SUCCESS
  ASSERT_TRUE(decoder.decode_bytes(&value, value_size));
  ASSERT_EQ((const char*)&input[12], decoder.buffer());
  ASSERT_EQ(9ul, decoder.remaining());
  ASSERT_EQ(8ul, value_size);
  for (int i = 0; i < 8; ++i) {
    ASSERT_EQ(input[i + 4], value[i]);
  }
  ASSERT_TRUE(decoder.decode_bytes(&value, value_size));
  ASSERT_EQ(0ul, decoder.remaining());
  ASSERT_EQ(5ul, value_size);
  for (int i = 0; i < 5; ++i) {
    ASSERT_EQ((char)input[i + 16], value[i]);
  }

  // FAIL
  ASSERT_FALSE(decoder.decode_bytes(&value, value_size));
  ASSERT_TRUE(failure_logged_);
}

TEST_F(DecoderUnitTest, DecodeBytesRef) {
  const signed char input[21] = { 0, 0, 0, 8, 68, 97, 116, 97, 83, 116, 97, 120, // DataStax
                                  0, 0, 0, 5, 67, 47, 67,  43, 43 };             // C/C++
  TestDecoder decoder((const char*)input, 21);
  StringRef value;

  // SUCCESS
  ASSERT_TRUE(decoder.decode_bytes(&value));
  ASSERT_EQ((const char*)&input[12], decoder.buffer());
  ASSERT_EQ(9ul, decoder.remaining());
  ASSERT_EQ(8ul, value.size());
  for (int i = 0; i < 8; ++i) {
    ASSERT_EQ((char)input[i + 4], value.data()[i]);
  }
  ASSERT_TRUE(decoder.decode_bytes(&value));
  ASSERT_EQ(0ul, decoder.remaining());
  ASSERT_EQ(5ul, value.size());
  for (int i = 0; i < 5; ++i) {
    ASSERT_EQ((char)input[i + 16], value.data()[i]);
  }

  // FAIL
  ASSERT_FALSE(decoder.decode_bytes(&value));
  ASSERT_TRUE(failure_logged_);
}

TEST_F(DecoderUnitTest, DecodeInetAddress) {
  const signed char input[30] = { 4,  127, 0, 0, 1, 0, 0, 35, 82, // 127.0.0.1:9042
                                  16, 0,   0, 0, 0, 0, 0, 0,  0,  0, 0,
                                  0,  0,   0, 0, 0, 1, 0, 0,  35, 82 }; // [::1]:9042
  TestDecoder decoder((const char*)input, 30);
  Address value;

  // SUCCESS
  ASSERT_TRUE(decoder.decode_inet(&value));
  ASSERT_EQ((const char*)&input[9], decoder.buffer());
  ASSERT_EQ(21ul, decoder.remaining());
  ASSERT_STREQ("127.0.0.1:9042", value.to_string(true).c_str());
  ASSERT_TRUE(decoder.decode_inet(&value));
  ASSERT_EQ(0ul, decoder.remaining());
  ASSERT_STREQ("[::1]:9042", value.to_string(true).c_str());

  // FAIL
  ASSERT_FALSE(decoder.decode_inet(&value));
  ASSERT_TRUE(failure_logged_);
}

TEST_F(DecoderUnitTest, DecodeInetStruct) {
  const signed char input[22] = { 4,  127, 0, 0, 1, // 127.0.0.1
                                  16, 0,   0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 }; // [::1]
  TestDecoder decoder((const char*)input, 22);
  CassInet value;

  // SUCCESS
  ASSERT_TRUE(decoder.decode_inet(&value));
  ASSERT_EQ((const char*)&input[5], decoder.buffer());
  ASSERT_EQ(17ul, decoder.remaining());
  for (int i = 0; i < value.address_length; ++i) {
    uint8_t byte = 0;
    decode_byte((const char*)&input[i + 1], byte);
    ASSERT_EQ(byte, value.address[i]);
  }
  ASSERT_TRUE(decoder.decode_inet(&value));
  ASSERT_EQ(0ul, decoder.remaining());
  for (int i = 0; i < value.address_length; ++i) {
    uint8_t byte = 0;
    decode_byte((const char*)&input[i + 6], byte);
    ASSERT_EQ(byte, value.address[i]);
  }

  // FAIL
  ASSERT_FALSE(decoder.decode_inet(&value));
  ASSERT_TRUE(failure_logged_);
}

TEST_F(DecoderUnitTest, AsInetIPv4) {
  const signed char input[4] = { 127, 0, 0, 1 }; // 127.0.0.1
  TestDecoder decoder((const char*)input, 4);
  CassInet value;

  // SUCCESS (IPv4)
  for (int i = 0; i < 10; ++i) {
    ASSERT_TRUE(decoder.as_inet(4, &value));
    ASSERT_EQ((const char*)&input[0], decoder.buffer());
    ASSERT_EQ(4ul, decoder.remaining());
    for (int j = 0; j < value.address_length; ++j) {
      uint8_t byte = 0;
      decode_byte((const char*)&input[j], byte);
      ASSERT_EQ(byte, value.address[j]);
    }
  }
  ASSERT_FALSE(failure_logged_);
}

TEST_F(DecoderUnitTest, AsInetIPv6) {
  const signed char input[16] = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 }; // [::1]
  TestDecoder decoder((const char*)input, 16);
  CassInet value;

  // SUCCESS (IPv6)
  for (int i = 0; i < 10; ++i) {
    ASSERT_TRUE(decoder.as_inet(16, &value));
    ASSERT_EQ((const char*)&input[0], decoder.buffer());
    ASSERT_EQ(16ul, decoder.remaining());
    for (int j = 0; j < value.address_length; ++j) {
      uint8_t byte = 0;
      decode_byte((const char*)&input[j], byte);
      ASSERT_EQ(byte, value.address[j]);
    }
  }
  ASSERT_FALSE(failure_logged_);
}

TEST_F(DecoderUnitTest, DecodeStringMap) {
  const signed char input[38] = { 0, 2, 0,   7,  99,  111, 109, 112, 97,  110, 121, // key = company
                                  0, 8, 68,  97, 116, 97,  83,  116, 97,  120, // value = DataStax
                                  0, 8, 108, 97, 110, 103, 117, 97,  103, 101, // key = language
                                  0, 5, 67,  47, 67,  43,  43 };               // value = C/C++
  TestDecoder decoder((const char*)input, 38);
  Map<String, String> value;

  // SUCCESS
  ASSERT_TRUE(decoder.decode_string_map(value));
  ASSERT_EQ((const char*)&input[38], decoder.buffer());
  ASSERT_EQ(0ul, decoder.remaining());
  ASSERT_EQ(2ul, value.size());
  ASSERT_STREQ("DataStax", value["company"].c_str());
  ASSERT_STREQ("C/C++", value["language"].c_str());

  // FAIL
  ASSERT_FALSE(decoder.decode_string_map(value));
  ASSERT_TRUE(failure_logged_);
}

TEST_F(DecoderUnitTest, DecodeStringlistVector) {
  const signed char input[19] = { 0, 2, 0,  8,  68, 97, 116, 97, 83, 116, 97, 120, // DataStax
                                  0, 5, 67, 47, 67, 43, 43 };                      // C/C++
  TestDecoder decoder((const char*)input, 19);
  Vector<String> value;

  // SUCCESS
  ASSERT_TRUE(decoder.decode_stringlist(value));
  ASSERT_EQ((const char*)&input[19], decoder.buffer());
  ASSERT_EQ(0ul, decoder.remaining());
  ASSERT_EQ(2ul, value.size());
  ASSERT_STREQ("DataStax", value[0].c_str());
  ASSERT_STREQ("C/C++", value[1].c_str());

  // FAIL
  ASSERT_FALSE(decoder.decode_stringlist(value));
  ASSERT_TRUE(failure_logged_);
}

TEST_F(DecoderUnitTest, DecodeStringlistStringRefVec) {
  const signed char input[19] = { 0, 2, 0,  8,  68, 97, 116, 97, 83, 116, 97, 120, // DataStax
                                  0, 5, 67, 47, 67, 43, 43 };                      // C/C++
  TestDecoder decoder((const char*)input, 19);
  StringRefVec value;

  // SUCCESS
  ASSERT_TRUE(decoder.decode_stringlist(value));
  ASSERT_EQ((const char*)&input[19], decoder.buffer());
  ASSERT_EQ(0ul, decoder.remaining());
  ASSERT_EQ(2ul, value.size());
  ASSERT_STREQ("DataStax", std::string(value[0].data(), value[0].size()).c_str());
  ASSERT_STREQ("C/C++", std::string(value[1].data(), value[1].size()).c_str());

  // FAIL
  ASSERT_FALSE(decoder.decode_stringlist(value));
  ASSERT_TRUE(failure_logged_);
}

TEST_F(DecoderUnitTest, AsStringlist) {
  const signed char input[19] = { 0, 2, 0,  8,  68, 97, 116, 97, 83, 116, 97, 120, // DataStax
                                  0, 5, 67, 47, 67, 43, 43 };                      // C/C++
  TestDecoder decoder((const char*)input, 19);
  StringRefVec value;

  // SUCCESS
  for (int i = 0; i < 10; ++i) {
    ASSERT_TRUE(decoder.as_stringlist(value));
    ASSERT_EQ((const char*)&input[0], decoder.buffer());
    ASSERT_EQ(19ul, decoder.remaining());
    ASSERT_EQ(2ul, value.size());
    ASSERT_STREQ("DataStax", std::string(value[0].data(), value[0].size()).c_str());
    ASSERT_STREQ("C/C++", std::string(value[1].data(), value[1].size()).c_str());
  }

  // Decode stringlist to finish decoding buffer
  ASSERT_TRUE(decoder.decode_stringlist(value));

  // FAIL
  ASSERT_FALSE(decoder.as_stringlist(value));
  ASSERT_TRUE(failure_logged_);
}

TEST_F(DecoderUnitTest, DecodeStringMultiMap) {
  const signed char input[58] = { 0, 1, 0,  7,   100, 114, 105, 118, 101, 114, 115, // key = drivers
                                  0, 7, 0,  5,   67,  47,  67,  43,  43,            // C/C++
                                  0, 2, 67, 35,                                     // C#
                                  0, 4, 74, 97,  118, 97,                           // Java
                                  0, 7, 78, 111, 100, 101, 46,  106, 115,           // Node.js
                                  0, 3, 80, 72,  80,                                // PHP
                                  0, 6, 80, 121, 116, 104, 111, 110,                // Python
                                  0, 4, 82, 117, 98,  121 };                        // Ruby
  TestDecoder decoder((const char*)input, 58);
  StringMultimap value;

  // SUCCESS
  ASSERT_TRUE(decoder.decode_string_multimap(value));
  ASSERT_EQ((const char*)&input[58], decoder.buffer());
  ASSERT_EQ(0ul, decoder.remaining());
  ASSERT_EQ(1ul, value.size());
  Vector<String> values = value["drivers"];
  ASSERT_EQ(7ul, values.size());
  ASSERT_STREQ("C/C++", std::string(values[0].data(), values[0].size()).c_str());
  ASSERT_STREQ("C#", std::string(values[1].data(), values[1].size()).c_str());
  ASSERT_STREQ("Java", std::string(values[2].data(), values[2].size()).c_str());
  ASSERT_STREQ("Node.js", std::string(values[3].data(), values[3].size()).c_str());
  ASSERT_STREQ("PHP", std::string(values[4].data(), values[4].size()).c_str());
  ASSERT_STREQ("Python", std::string(values[5].data(), values[5].size()).c_str());
  ASSERT_STREQ("Ruby", std::string(values[6].data(), values[6].size()).c_str());

  // FAIL
  ASSERT_FALSE(decoder.decode_string_multimap(value));
  ASSERT_TRUE(failure_logged_);
}

TEST_F(DecoderUnitTest, DecodeOption) {
  const signed char input[14] = {
    0, 1, // ASCII
    0, 0, 0, 8, 68, 97, 116, 97, 83, 116, 97, 120
  }; // Custom = DataStax
  TestDecoder decoder((const char*)input, 14);
  uint16_t type;
  const char* class_name = NULL;
  size_t class_name_size = 0;

  // SUCCESS
  ASSERT_TRUE(decoder.decode_option(type, &class_name, class_name_size));
  ASSERT_EQ((const char*)&input[2], decoder.buffer());
  ASSERT_EQ(12ul, decoder.remaining());
  ASSERT_EQ(CASS_VALUE_TYPE_ASCII, type);
  ASSERT_TRUE(decoder.decode_option(type, &class_name, class_name_size));
  ASSERT_EQ(0ul, decoder.remaining());
  ASSERT_EQ(CASS_VALUE_TYPE_CUSTOM, type);
  ASSERT_STREQ("DataStax", std::string(class_name, class_name_size).c_str());
  ASSERT_EQ(8ul, class_name_size);

  // FAIL
  ASSERT_FALSE(decoder.decode_option(type, &class_name, class_name_size));
  ASSERT_TRUE(failure_logged_);
}

TEST_F(DecoderUnitTest, DecodeUuid) {
  const signed char input[32] = { -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                                  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0 };
  TestDecoder decoder((const char*)input, 32);
  CassUuid value;

  // SUCCESS
  ASSERT_TRUE(decoder.decode_uuid(&value));
  ASSERT_EQ((const char*)&input[16], decoder.buffer());
  ASSERT_EQ(16ul, decoder.remaining());
  ASSERT_EQ(std::numeric_limits<uint64_t>::max(), value.clock_seq_and_node);
  ASSERT_EQ(std::numeric_limits<uint64_t>::max(), value.time_and_version);
  ASSERT_TRUE(decoder.decode_uuid(&value));
  ASSERT_EQ(0ul, decoder.remaining());
  ASSERT_EQ(std::numeric_limits<uint64_t>::min(), value.clock_seq_and_node);
  ASSERT_EQ(std::numeric_limits<uint64_t>::min(), value.time_and_version);

  // FAIL
  ASSERT_FALSE(decoder.decode_uuid(&value));
  ASSERT_TRUE(failure_logged_);
}

TEST_F(DecoderUnitTest, AsDecimal) {
  const signed char input[8] = { 0, 0, 0, 4, 0, 1, 2, 3 };
  TestDecoder decoder((const char*)input, 8);
  const uint8_t* value = NULL;
  size_t value_size = 0;
  int32_t value_scale = 0;

  // SUCCESS
  for (int i = 0; i < 10; ++i) {
    ASSERT_TRUE(decoder.as_decimal(&value, &value_size, &value_scale));
    ASSERT_EQ((const char*)&input[0], decoder.buffer());
    ASSERT_EQ(8ul, decoder.remaining());
    for (size_t j = 0; j < value_size; ++j) {
      uint8_t byte = 0;
      decode_byte((const char*)&input[j + sizeof(int32_t)], byte);
      ASSERT_EQ(byte, value[j]);
    }
  }

  // Decode some bytes in the decimal to increment the buffer
  for (int i = 0; i < 4; ++i) {
    uint8_t byte = 0;
    ASSERT_TRUE(decoder.decode_byte(byte));
  }

  // FAIL
  ASSERT_FALSE(decoder.as_decimal(&value, &value_size, &value_scale));
  ASSERT_TRUE(failure_logged_);
}

TEST_F(DecoderUnitTest, AsDuration) {
  const signed char input[4] = { 2, 4, 6, -127 }; // 1, 2, 3 (zig zag encoding)
  TestDecoder decoder((const char*)input, 4);
  int32_t months = 0;
  int32_t days = 0;
  int64_t nanos = 0;

  // SUCCESS
  for (int i = 0; i < 10; ++i) {
    ASSERT_TRUE(decoder.as_duration(&months, &days, &nanos));
    ASSERT_EQ((const char*)&input[0], decoder.buffer());
    ASSERT_EQ(4ul, decoder.remaining());
    ASSERT_EQ(1, months);
    ASSERT_EQ(2, days);
    ASSERT_EQ(3, nanos);
  }

  // Decode three bytes in the duration to increment to the next duration
  for (int i = 0; i < 3; ++i) {
    uint8_t byte = 0;
    ASSERT_TRUE(decoder.decode_byte(byte));
  }

  // FAIL
  ASSERT_FALSE(decoder.as_duration(&months, &days, &nanos));
  ASSERT_TRUE(failure_logged_);
}

TEST_F(DecoderUnitTest, DecodeCustomPayload) {
  const signed char input[21] = { 0, 1, 0, 8, 68, 97, 116, 97, 83, 116, 97, 120, // DataStax
                                  0, 0, 0, 5, 67, 47, 67,  43, 43 };             // C/C++
  TestDecoder decoder((const char*)input, 21);
  CustomPayloadVec value;

  // SUCCESS
  ASSERT_TRUE(decoder.decode_custom_payload(value));
  ASSERT_EQ(0ul, decoder.remaining());
  ASSERT_EQ(1ul, value.size());
  ASSERT_EQ(8ul, value[0].name.size());
  ASSERT_STREQ("DataStax", std::string(value[0].name.data(), value[0].name.size()).c_str());
  ASSERT_EQ(5ul, value[0].value.size());
  for (int i = 0; i < 5; ++i) {
    ASSERT_EQ((char)input[i + 16], value[0].value.data()[i]);
  }

  // FAIL
  ASSERT_FALSE(decoder.decode_custom_payload(value));
  ASSERT_TRUE(failure_logged_);
}

TEST_F(DecoderUnitTest, DecodeFailures) {
  const signed char input[4] = { 0, 0, 0, 42 };
  TestDecoder decoder((const char*)input, 4, 1);
  FailureVec value;
  int32_t value_size = 0;

  // SUCCESS
  ASSERT_TRUE(decoder.decode_failures(value, value_size));
  ASSERT_EQ(0ul, decoder.remaining());
  ASSERT_EQ(0ul, value.size());
  ASSERT_EQ(42, value_size);

  // FAIL
  ASSERT_FALSE(decoder.decode_failures(value, value_size));
  ASSERT_TRUE(failure_logged_);
}

TEST_F(DecoderUnitTest, DecodeFailuresWithVector) {
  const signed char input[30] = { 0, 0, 0,  2, 4, 127, 0, 0, 1, // 127.0.0.1
                                  0, 1, 16, 0, 0, 0,   0, 0, 0, 0,
                                  0, 0, 0,  0, 0, 0,   0, 0, 1, // [::1] [02]
                                  0, 2 };
  TestDecoder decoder((const char*)input, 30, 5);
  FailureVec value;
  int32_t value_size = 0;

  // SUCCESS
  ASSERT_TRUE(decoder.decode_failures(value, value_size));
  ASSERT_EQ(0ul, decoder.remaining());
  ASSERT_EQ(2ul, value.size());
  ASSERT_EQ(2, value_size);
  for (int i = 0; i < value[0].endpoint.address_length; ++i) {
    uint8_t byte;
    decode_byte((const char*)&input[i + 5], byte);
    ASSERT_EQ(byte, value[0].endpoint.address[i]);
  }
  ASSERT_EQ(1ul, value[0].failurecode);
  for (int i = 0; i < value[1].endpoint.address_length; ++i) {
    uint8_t byte;
    decode_byte((const char*)&input[i + 12], byte);
    ASSERT_EQ(byte, value[1].endpoint.address[i]);
  }
  ASSERT_EQ(2ul, value[1].failurecode);

  // FAIL
  ASSERT_FALSE(decoder.decode_failures(value, value_size));
  ASSERT_TRUE(failure_logged_);
}

TEST_F(DecoderUnitTest, DecodeWriteType) {
  const signed char input[67] = { 0,  6,  83, 73, 77, 80, 76, 69, // SIMPLE
                                  0,  5,  66, 65, 84, 67, 72,     // BATCH
                                  0,  14, 85, 78, 76, 79, 71, 71, 69, 68, 95,
                                  66, 65, 84, 67, 72,                         // UNLOGGED_BATCH
                                  0,  7,  67, 79, 85, 78, 84, 69, 82,         // COUNTER
                                  0,  9,  66, 65, 84, 67, 72, 95, 76, 79, 71, // BATCH_LOG
                                  0,  3,  67, 65, 83,                         // CAS
                                  0,  4,  86, 73, 69, 87,                     // VIEW
                                  0,  3,  67, 68, 67 };                       // CDC
  TestDecoder decoder((const char*)input, 67);
  CassWriteType value;

  // SUCCESS
  ASSERT_TRUE(decoder.decode_write_type(value));
  ASSERT_EQ(59ul, decoder.remaining());
  ASSERT_EQ(CASS_WRITE_TYPE_SIMPLE, value);
  ASSERT_TRUE(decoder.decode_write_type(value));
  ASSERT_EQ(52ul, decoder.remaining());
  ASSERT_EQ(CASS_WRITE_TYPE_BATCH, value);
  ASSERT_TRUE(decoder.decode_write_type(value));
  ASSERT_EQ(36ul, decoder.remaining());
  ASSERT_EQ(CASS_WRITE_TYPE_UNLOGGED_BATCH, value);
  ASSERT_TRUE(decoder.decode_write_type(value));
  ASSERT_EQ(27ul, decoder.remaining());
  ASSERT_EQ(CASS_WRITE_TYPE_COUNTER, value);
  ASSERT_TRUE(decoder.decode_write_type(value));
  ASSERT_EQ(16ul, decoder.remaining());
  ASSERT_EQ(CASS_WRITE_TYPE_BATCH_LOG, value);
  ASSERT_TRUE(decoder.decode_write_type(value));
  ASSERT_EQ(11ul, decoder.remaining());
  ASSERT_EQ(CASS_WRITE_TYPE_CAS, value);
  ASSERT_TRUE(decoder.decode_write_type(value));
  ASSERT_EQ(5ul, decoder.remaining());
  ASSERT_EQ(CASS_WRITE_TYPE_VIEW, value);
  ASSERT_TRUE(decoder.decode_write_type(value));
  ASSERT_EQ(0ul, decoder.remaining());
  ASSERT_EQ(CASS_WRITE_TYPE_CDC, value);

  // FAIL
  ASSERT_FALSE(decoder.decode_write_type(value));
  ASSERT_EQ(CASS_WRITE_TYPE_UNKNOWN, value);
  ASSERT_TRUE(failure_logged_);
}

TEST_F(DecoderUnitTest, DecodeWarnings) {
  const signed char input[38] = { 0,   2,   0,   16,  87,  97,  114, 110, 105, 110, 103,
                                  32,  78,  117, 109, 98,  101, 114, 32,  49, // Warning Number 1
                                  0,   16,  87,  97,  114, 110, 105, 110, 103, 32,  78,
                                  117, 109, 98,  101, 114, 32,  50 }; // Warning Number 2
  TestDecoder decoder((const char*)input, 38);
  WarningVec value;

  // SUCCESS
  ASSERT_TRUE(decoder.decode_warnings(value));
  ASSERT_EQ(0ul, decoder.remaining());
  ASSERT_EQ(2ul, value.size());
  ASSERT_STREQ("Warning Number 1", std::string(value[0].data(), value[0].size()).c_str());
  ASSERT_STREQ("Warning Number 2", std::string(value[1].data(), value[1].size()).c_str());

  // FAIL
  ASSERT_FALSE(decoder.decode_warnings(value));
  ASSERT_TRUE(failure_logged_);
}
