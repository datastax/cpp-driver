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

#ifndef __TEST_BIG_NUMBER_HPP__
#define __TEST_BIG_NUMBER_HPP__
#include "objects/object_base.hpp"
#include "test_utils.hpp"

#include <algorithm>
#include <vector>

#include <openssl/bn.h>

namespace test { namespace driver {

/**
 * Helper class for working with Java byte arrays (e.g. BigInteger and
 * BigDecimal); converting binary values.
 */
class BigNumber {
public:
  BigNumber()
      : scale_(0) {}

  BigNumber(const std::string& big_number)
      : big_number_(BN_new())
      , scale_(0) {
    BIGNUM* bignum = big_number_.get();
    std::string copy = test::Utils::trim(big_number);

    // Ensure the number is valid
    if (is_valid(big_number)) {
      // Check if number is a decimal
      size_t decimal_location = copy.find(".");
      if (decimal_location != std::string::npos) {
        // Remove decimal and calculate scale
        copy.erase(decimal_location, 1);
        scale_ = copy.size() - decimal_location;
      }

      BN_dec2bn(&bignum, copy.c_str());
    } else {
      EXPECT_TRUE(false) << "Invalid BigNumber " << copy << ": Value will be 0";
      BIGNUM* bignum = big_number_.get();
      BN_zero(bignum);
    }
  }

  BigNumber(const unsigned char* bytes, size_t bytes_length, int32_t scale)
      : big_number_(BN_new())
      , scale_(scale) {
    if (bytes && bytes_length > 0) {
      // Determine if value is negative and handle two's complement
      bool is_negative = ((bytes[0] & 0x80) != 0);
      if (is_negative) {
        // Create a copy and convert to two's complement
        std::vector<unsigned char> twos_complement(bytes_length);
        memcpy(&twos_complement[0], bytes, bytes_length);
        bool is_carry = true;
        for (ssize_t i = bytes_length - 1; i >= 0; --i) {
          twos_complement[i] ^= 0xFF;
          if (is_carry) {
            is_carry = ((++twos_complement[i]) == 0);
          }
        }
        big_number_ = BN_bin2bn(&twos_complement[0], bytes_length, NULL);
        BN_set_negative(big_number_.get(), 1);
      } else {
        big_number_ = BN_bin2bn(bytes, bytes_length, NULL);
        BN_set_negative(big_number_.get(), 0);
      }
    } else {
      BIGNUM* bignum = big_number_.get();
      BN_zero(bignum);
      scale_ = 0;
    }
  }

  /**
   * Comparison operation for BigNumber
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const BigNumber& rhs) const {
    if (scale_ < rhs.scale_) return -1;
    if (scale_ > rhs.scale_) return 1;
    return BN_cmp(big_number_.get(), rhs.big_number_.get());
  }

  /**
   * Encode the varint using two's complement
   *
   * @return Vector of bytes in two's complement
   */
  std::vector<unsigned char> encode_varint() const {
    // Handle NULL and zero varint
    if (!big_number_ || BN_num_bytes(big_number_.get()) == 0) {
      std::vector<unsigned char> bytes(1);
      bytes[0] = 0x00;
      return bytes;
    }

    size_t number_of_bytes = BN_num_bytes(big_number_.get()) + 1;
    std::vector<unsigned char> bytes(number_of_bytes);
    if (BN_bn2bin(big_number_.get(), &bytes[1]) > 0) {
      // Set the sign and convert to two's complement (if necessary)
      if (BN_is_negative(big_number_.get())) {
        bool is_carry = true;
        for (ssize_t i = number_of_bytes - 1; i >= 0; --i) {
          bytes[i] ^= 0xFF;
          if (is_carry) {
            is_carry = ((++bytes[i]) == 0);
          }
        }
        bytes[0] |= 0x80;
      } else {
        bytes[0] = 0x00;
      }
    }

    return bytes;
  }

  /**
   * Get the scale for the big number
   *
   * @return Scale for number
   */
  int32_t scale() const { return scale_; }

  /**
   * Get the string representation of the big number
   *
   * @return String representation of numerical value
   */
  std::string str() const {
    std::string result;
    if (!big_number_) return result;

    char* decimal = BN_bn2dec(big_number_.get());
    result.assign(decimal);
    OPENSSL_free(decimal);

    // Normalize - strip leading zeros
    result.erase(0, result.find_first_not_of('0'));
    if (result.size() == 0) {
      result = "0";
    }

    // Return the value as integer or decimal (depending)
    if (scale_ > 0) {
      size_t decimal_location = result.size() - scale_;
      return result.substr(0, decimal_location) + "." + result.substr(decimal_location);
    }
    return result;
  }

private:
  /**
   * OpenSSL big number
   */
  Object<BIGNUM, BN_free> big_number_;
  /**
   * Scale for decimal values
   */
  int32_t scale_;

  /**
   * Ensure the big number is valid (digits and period)
   *
   * @param big_number String value to check
   * @return True if string is valid; false otherwise
   */
  bool is_valid(const std::string& big_number) {
    // Ensure the big number is a number
    if (big_number.empty()) return false;
    if (big_number.find_first_not_of("0123456789-.") != std::string::npos) {
      return false;
    }

    // Ensure the big number has at most 1 decimal place
    if (std::count(big_number.begin(), big_number.end(), '.') > 1) {
      return false;
    }

    // Ensure the big number has at most 1 minus sign (and is at the beginning)
    size_t count = std::count(big_number.begin(), big_number.end(), '-');
    if (count > 1) {
      return false;
    } else if (count == 1) {
      if (big_number[0] != '-') {
        return false;
      }
    }

    // Big number is valid
    return true;
  }
};

}} // namespace test::driver

#endif // __TEST_BIG_NUMBER_HPP__
