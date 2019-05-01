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

#include "token_map_impl.hpp"

#include "uint128.hpp"

#include <ctype.h>
#include <stdio.h>

using namespace datastax;
using namespace datastax::internal;
using namespace datastax::internal::core;

namespace {

String to_string(RandomPartitioner::Token token) {
  return numeric::uint128_t(token.lo, token.hi).to_string();
}

} // namespace

TEST(TokenUnitTest, RandomAbs) {
  // Two's complement: -170141183460469231731687303715884105728
  {
    uint8_t digest[16] = {};
    digest[0] = 0x80;

    RandomPartitioner::Token token;
    token.hi = RandomPartitioner::encode(digest);
    token.lo = RandomPartitioner::encode(digest + 8);
    token = RandomPartitioner::abs(token);

    EXPECT_EQ(to_string(token), "170141183460469231731687303715884105728");
  }

  // Two's complement: -170141183460469231731687303715884105727
  {
    uint8_t digest[16] = {};
    digest[0] = 0x80;
    digest[15] = 0x01;

    RandomPartitioner::Token token;
    token.hi = RandomPartitioner::encode(digest);
    token.lo = RandomPartitioner::encode(digest + 8);
    token = RandomPartitioner::abs(token);

    EXPECT_EQ(to_string(token), "170141183460469231731687303715884105727");
  }

  // Two's complement: -18446744073709551616
  {
    uint8_t digest[16] = {};
    digest[0] = 0xFF;
    digest[1] = 0xFF;
    digest[2] = 0xFF;
    digest[3] = 0xFF;
    digest[4] = 0xFF;
    digest[5] = 0xFF;
    digest[6] = 0xFF;
    digest[7] = 0xFF;

    RandomPartitioner::Token token;
    token.hi = RandomPartitioner::encode(digest);
    token.lo = RandomPartitioner::encode(digest + 8);
    token = RandomPartitioner::abs(token);

    EXPECT_EQ(to_string(token), "18446744073709551616");
  }

  // Two's complement: 0
  {
    uint8_t digest[16] = {};

    RandomPartitioner::Token token;
    token.hi = RandomPartitioner::encode(digest);
    token.lo = RandomPartitioner::encode(digest + 8);
    token = RandomPartitioner::abs(token);

    EXPECT_EQ(numeric::uint128_t(token.lo, token.hi).to_string(), "0");
  }

  // Two's complement: 170141183460469231731687303715884105727
  {
    uint8_t digest[16] = {};
    digest[0] = 0x7F;
    digest[1] = 0xFF;
    digest[2] = 0xFF;
    digest[3] = 0xFF;
    digest[4] = 0xFF;
    digest[5] = 0xFF;
    digest[6] = 0xFF;
    digest[7] = 0xFF;
    digest[8] = 0xFF;
    digest[9] = 0xFF;
    digest[10] = 0xFF;
    digest[11] = 0xFF;
    digest[12] = 0xFF;
    digest[13] = 0xFF;
    digest[14] = 0xFF;
    digest[15] = 0xFF;

    RandomPartitioner::Token token;
    token.hi = RandomPartitioner::encode(digest);
    token.lo = RandomPartitioner::encode(digest + 8);
    token = RandomPartitioner::abs(token);

    EXPECT_EQ(to_string(token), "170141183460469231731687303715884105727");
  }
}

TEST(TokenUnitTest, RandomLessThan) {
  // 'hi' is the same and 'lo' is less than
  {
    RandomPartitioner::Token t1, t2;

    // Two's complement: 0
    {
      uint8_t digest[16] = {};
      t1.hi = RandomPartitioner::encode(digest);
      t1.lo = RandomPartitioner::encode(digest + 8);
      t1 = RandomPartitioner::abs(t1);
    }

    // Two's complement: 1
    {
      uint8_t digest[16] = {};
      digest[15] = 0x01;
      t2.hi = RandomPartitioner::encode(digest);
      t2.lo = RandomPartitioner::encode(digest + 8);
      t2 = RandomPartitioner::abs(t2);
    }

    EXPECT_TRUE(t1 < t2);
  }

  // 'lo' is the same and 'hi' is less than
  {
    RandomPartitioner::Token t1, t2;

    // Two's complement: 18446744073709551616
    {
      uint8_t digest[16] = {};
      digest[7] = 0x01;

      t1.hi = RandomPartitioner::encode(digest);
      t1.lo = RandomPartitioner::encode(digest + 8);
      t1 = RandomPartitioner::abs(t1);
    }

    // Two's complement: 36893488147419103232
    {
      uint8_t digest[16] = {};
      digest[7] = 0x02;
      t2.hi = RandomPartitioner::encode(digest);
      t2.lo = RandomPartitioner::encode(digest + 8);
      t2 = RandomPartitioner::abs(t2);
    }

    EXPECT_TRUE(t1 < t2);
  }

  // Absolute value of negative values
  {
    RandomPartitioner::Token t1, t2;

    // Two's complement: -170141183460469231731687303715884105727
    {
      uint8_t digest[16] = {};
      digest[0] = 0x80;
      digest[15] = 0x01;
      t1.hi = RandomPartitioner::encode(digest);
      t1.lo = RandomPartitioner::encode(digest + 8);
      t1 = RandomPartitioner::abs(t1);
    }

    // Two's complement: -170141183460469231731687303715884105728
    {
      uint8_t digest[16] = {};
      digest[0] = 0x80;
      t2.hi = RandomPartitioner::encode(digest);
      t2.lo = RandomPartitioner::encode(digest + 8);
      t2 = RandomPartitioner::abs(t2);
    }

    EXPECT_TRUE(t1 < t2);
  }

  // Same value
  {
    RandomPartitioner::Token t1, t2;

    // Two's complement: 18446744073709551616
    {
      uint8_t digest[16] = {};
      digest[7] = 0x01;
      t1.hi = RandomPartitioner::encode(digest);
      t1.lo = RandomPartitioner::encode(digest + 8);
      t1 = RandomPartitioner::abs(t1);
    }

    // Two's complement: 18446744073709551616
    {
      uint8_t digest[16] = {};
      digest[7] = 0x01;
      t2.hi = RandomPartitioner::encode(digest);
      t2.lo = RandomPartitioner::encode(digest + 8);
      t2 = RandomPartitioner::abs(t2);
    }

    EXPECT_FALSE(t1 < t2);
  }

  // Zero
  {
    RandomPartitioner::Token t1, t2;

    {
      uint8_t digest[16] = {};
      t1.hi = RandomPartitioner::encode(digest);
      t1.lo = RandomPartitioner::encode(digest + 8);
      t1 = RandomPartitioner::abs(t1);
    }

    {
      uint8_t digest[16] = {};
      t2.hi = RandomPartitioner::encode(digest);
      t2.lo = RandomPartitioner::encode(digest + 8);
      t2 = RandomPartitioner::abs(t2);
    }

    EXPECT_FALSE(t1 < t2);
  }
}

TEST(TokenUnitTest, RandomEqual) {
  // Same value
  {
    RandomPartitioner::Token t1, t2;

    // Two's complement: 18446744073709551616
    {
      uint8_t digest[16] = {};
      digest[7] = 0x01;
      t1.hi = RandomPartitioner::encode(digest);
      t1.lo = RandomPartitioner::encode(digest + 8);
      t1 = RandomPartitioner::abs(t1);
    }

    // Two's complement: 18446744073709551616
    {
      uint8_t digest[16] = {};
      digest[7] = 0x01;
      t2.hi = RandomPartitioner::encode(digest);
      t2.lo = RandomPartitioner::encode(digest + 8);
      t2 = RandomPartitioner::abs(t2);
    }

    EXPECT_TRUE(t1 == t2);
  }

  // Zero
  {
    RandomPartitioner::Token t1, t2;

    {
      uint8_t digest[16] = {};
      t1.hi = RandomPartitioner::encode(digest);
      t1.lo = RandomPartitioner::encode(digest + 8);
      t1 = RandomPartitioner::abs(t1);
    }

    {
      uint8_t digest[16] = {};
      t2.hi = RandomPartitioner::encode(digest);
      t2.lo = RandomPartitioner::encode(digest + 8);
      t2 = RandomPartitioner::abs(t2);
    }

    EXPECT_TRUE(t1 == t2);
  }

  // 'hi' is the same and 'lo' is less than
  {
    RandomPartitioner::Token t1, t2;

    // Two's complement: 0
    {
      uint8_t digest[16] = {};
      t1.hi = RandomPartitioner::encode(digest);
      t1.lo = RandomPartitioner::encode(digest + 8);
      t1 = RandomPartitioner::abs(t1);
    }

    // Two's complement: 1
    {
      uint8_t digest[16] = {};
      digest[15] = 0x01;
      t2.hi = RandomPartitioner::encode(digest);
      t2.lo = RandomPartitioner::encode(digest + 8);
      t2 = RandomPartitioner::abs(t2);
    }

    EXPECT_FALSE(t1 == t2);
  }

  // 'lo' is the same and 'hi' is less than
  {
    RandomPartitioner::Token t1, t2;

    // Two's complement: 18446744073709551616
    {
      uint8_t digest[16] = {};
      digest[7] = 0x01;

      t1.hi = RandomPartitioner::encode(digest);
      t1.lo = RandomPartitioner::encode(digest + 8);
      t1 = RandomPartitioner::abs(t1);
    }

    // Two's complement: 36893488147419103232
    {
      uint8_t digest[16] = {};
      digest[7] = 0x02;
      t2.hi = RandomPartitioner::encode(digest);
      t2.lo = RandomPartitioner::encode(digest + 8);
      t2 = RandomPartitioner::abs(t2);
    }

    EXPECT_FALSE(t1 == t2);
  }
}

TEST(TokenUnitTest, RandomHash) {
  // Sampled using: SELECT token(key) FROM sometable;
  EXPECT_EQ(to_string(RandomPartitioner::hash("a")), "16955237001963240173058271559858726497");
  EXPECT_EQ(to_string(RandomPartitioner::hash("b")), "144992942750327304334463589818972416113");
  EXPECT_EQ(to_string(RandomPartitioner::hash("c")), "99079589977253916124855502156832923443");
  EXPECT_EQ(to_string(RandomPartitioner::hash("d")), "166860289390734216023086131251507064403");
  EXPECT_EQ(to_string(RandomPartitioner::hash("abc")), "148866708576779697295343134153845407886");
  EXPECT_EQ(to_string(RandomPartitioner::hash("xyz")), "61893731502141497228477852773302439842");
}

TEST(TokenUnitTest, RandomFromString) {
  EXPECT_EQ(to_string(RandomPartitioner::from_string("0")), "0");
  EXPECT_EQ(to_string(RandomPartitioner::from_string("1")), "1");
  EXPECT_EQ(to_string(RandomPartitioner::from_string("170141183460469231731687303715884105727")),
            "170141183460469231731687303715884105727");
  EXPECT_EQ(to_string(RandomPartitioner::from_string("170141183460469231731687303715884105728")),
            "170141183460469231731687303715884105728");
}
