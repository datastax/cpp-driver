/*
 * Copyright (C) 2016 Christopher Batey and Dogan Narinc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.scassandra.cql;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.codec.binary.Hex;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;

import static org.junit.Assert.*;
import static org.scassandra.cql.CqlTypesEqualsTest.Result.*;
import static org.scassandra.cql.ListType.list;
import static org.scassandra.cql.MapType.map;
import static org.scassandra.cql.PrimitiveType.*;
import static org.scassandra.cql.SetType.set;
import static org.scassandra.cql.TupleType.tuple;

@RunWith(Parameterized.class)
public class CqlTypesEqualsTest {

    enum Result {
        MATCH,
        NO_MATCH,
        ILLEGAL_ARGUMENT
    }

    @Parameterized.Parameters(name = "Type: {0} expected: {1}, actual {2}, should be equal: {3}")
    public static Collection<Object[]> data() throws UnknownHostException {
        return Arrays.asList(new Object[][]{
                {ASCII, "one", "one", MATCH},
                {ASCII, "one", "two", NO_MATCH},
                {ASCII, "one", null, NO_MATCH},
                {ASCII, null, "two", NO_MATCH},
                {ASCII, "one", 5, NO_MATCH},
                {ASCII, "one", java.util.UUID.randomUUID(), NO_MATCH},

                {TEXT, "one", "one", MATCH},
                {TEXT, "one", "two", NO_MATCH},
                {TEXT, "one", null, NO_MATCH},
                {TEXT, null, "two", NO_MATCH},

                {VARCHAR, "one", "one", MATCH},
                {VARCHAR, "one", "two", NO_MATCH},
                {VARCHAR, "one", null, NO_MATCH},
                {VARCHAR, null, "two", NO_MATCH},

                {BIG_INT, 1, "1", MATCH},
                {BIG_INT, 1, 1d, MATCH},
                {BIG_INT, 1l, 1d, MATCH},
                {BIG_INT, new Long("1"), new Long("1"), MATCH},
                {BIG_INT, new BigInteger("1"), 1d, MATCH},

                {BIG_INT, null, 1d, NO_MATCH},
                {BIG_INT, "hello", 1d, ILLEGAL_ARGUMENT},

                {COUNTER, 1, "1", MATCH},
                {COUNTER, 1, 1d, MATCH},
                {COUNTER, 1l, 1d, MATCH},
                {COUNTER, new BigInteger("1"), 1d, MATCH},

                {COUNTER, new BigInteger("1"), null, NO_MATCH},
                {COUNTER, null, new BigInteger("1"), NO_MATCH},
                {COUNTER, "hello", 1d, ILLEGAL_ARGUMENT},

                {INT, 1, "1", MATCH},
                {INT, 1, 1d, MATCH},
                {INT, "1", 1d, MATCH},
                {INT, new BigInteger("1"), 1d, MATCH},

                {INT, null, 1, NO_MATCH},
                {INT, "hello", 1d, ILLEGAL_ARGUMENT},

                {VAR_INT, "1", "1", MATCH},
                {VAR_INT, "1", 1d, MATCH},
                {VAR_INT, new BigInteger("1"), 1d, MATCH},

                {VAR_INT, "1", null, NO_MATCH},
                {VAR_INT, null, 1d, NO_MATCH},
                {VAR_INT, 1, 1d, ILLEGAL_ARGUMENT},
                {VAR_INT, 1, 1d, ILLEGAL_ARGUMENT},
                {VAR_INT, 1l, 1d, ILLEGAL_ARGUMENT},
                {VAR_INT, "hello", 1d, ILLEGAL_ARGUMENT},

                {BOOLEAN, true, "true", MATCH},
                {BOOLEAN, true, true, MATCH},
                {BOOLEAN, false, false, MATCH},

                {BOOLEAN, true, false, NO_MATCH},
                {BOOLEAN, null, false, ILLEGAL_ARGUMENT},
                {BOOLEAN, true, false, NO_MATCH},
                {BOOLEAN, 1, false, ILLEGAL_ARGUMENT},
                {BOOLEAN, "true", false, ILLEGAL_ARGUMENT},
                {BOOLEAN, new BigDecimal("1.2"), false, ILLEGAL_ARGUMENT},

                {BLOB, "0x0012345435345345435435", "0x0012345435345345435435", MATCH},
                {BLOB, ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}), "0x" + Hex.encodeHexString(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}), MATCH},

                {BLOB, ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}), "0x" + Hex.encodeHexString(new byte[]{1, 2, 3, 4, 5, 6, 7, 8}), NO_MATCH},
                {BLOB, "0x0012345435345345435435", "0x0012345435345345435433", NO_MATCH},
                {BLOB, 1, "0x0012345435345345435435", ILLEGAL_ARGUMENT},
                {BLOB, new BigInteger("1"), "0x0012345435345345435435", ILLEGAL_ARGUMENT},
                {BLOB, new BigDecimal("1"), "0x0012345435345345435435", ILLEGAL_ARGUMENT},
                {BLOB, null, "0x0012345435345345435433", ILLEGAL_ARGUMENT},

                {DECIMAL, "1", "1", MATCH},
                {DECIMAL, "1.0000", "1", MATCH},
                {DECIMAL, new BigDecimal("1.0000"), "1", MATCH},

                {DECIMAL, "1", null, NO_MATCH},
                {DECIMAL, "1", "2", NO_MATCH},
                {DECIMAL, new BigInteger("1"), "1.234", ILLEGAL_ARGUMENT},
                {DECIMAL, null, "1", ILLEGAL_ARGUMENT},
                {DECIMAL, 1, "1", ILLEGAL_ARGUMENT},
                {DECIMAL, 1, 1, ILLEGAL_ARGUMENT},
                {DECIMAL, 1l, 1, ILLEGAL_ARGUMENT},
                {DECIMAL, "hello", new BigInteger("1"), ILLEGAL_ARGUMENT},
                {DECIMAL, "hello", 1, ILLEGAL_ARGUMENT},

                {FLOAT, "1", "1", MATCH},
                {FLOAT, "1.0000", "1", MATCH},

                {FLOAT, "1", null, NO_MATCH},
                {FLOAT, "1", "2", NO_MATCH},
                {FLOAT, new BigInteger("1"), "1.234", ILLEGAL_ARGUMENT},
                {FLOAT, null, "1", ILLEGAL_ARGUMENT},
                {FLOAT, 1, "1", ILLEGAL_ARGUMENT},
                {FLOAT, 1, 1, ILLEGAL_ARGUMENT},
                {FLOAT, 1l, 1, ILLEGAL_ARGUMENT},
                {FLOAT, "hello", new BigInteger("1"), ILLEGAL_ARGUMENT},
                {FLOAT, "hello", 1, ILLEGAL_ARGUMENT},

                {DOUBLE, "1", "1", MATCH},
                {DOUBLE, "1.0000", "1", MATCH},

                {DOUBLE, "1", null, NO_MATCH},
                {DOUBLE, "1", "2", NO_MATCH},
                {DOUBLE, new BigInteger("1"), "1.234", ILLEGAL_ARGUMENT},
                {DOUBLE, null, "1", ILLEGAL_ARGUMENT},
                {DOUBLE, 1, "1", ILLEGAL_ARGUMENT},
                {DOUBLE, 1, 1, ILLEGAL_ARGUMENT},
                {DOUBLE, 1l, 1, ILLEGAL_ARGUMENT},
                {DOUBLE, "hello", new BigInteger("1"), ILLEGAL_ARGUMENT},
                {DOUBLE, "hello", 1, ILLEGAL_ARGUMENT},

                {TIMESTAMP, 1l, "1", MATCH},
                {TIMESTAMP, 1l, 1d, MATCH},
                {TIMESTAMP, new Date(1l), 1d, MATCH},
                {TIMESTAMP, null, 1d, NO_MATCH},

                {TIMESTAMP, 1l, 12d, NO_MATCH},
                {TIMESTAMP, 1, 1d, ILLEGAL_ARGUMENT},
                {TIMESTAMP, "1", 1d, ILLEGAL_ARGUMENT},
                {TIMESTAMP, new BigInteger("1"), 1d, ILLEGAL_ARGUMENT},
                {TIMESTAMP, "hello", 1d, ILLEGAL_ARGUMENT},

                {TIMEUUID, "59ad61d0-c540-11e2-881e-b9e6057626c4", "59ad61d0-c540-11e2-881e-b9e6057626c4", MATCH},
                {TIMEUUID, java.util.UUID.fromString("59ad61d0-c540-11e2-881e-b9e6057626c4"), "59ad61d0-c540-11e2-881e-b9e6057626c4", MATCH},

                {TIMEUUID, java.util.UUID.randomUUID(), "59ad61d0-c540-11e2-881e-b9e6057626c4", NO_MATCH},
                {TIMEUUID, java.util.UUID.randomUUID().toString(), "59ad61d0-c540-11e2-881e-b9e6057626c4", NO_MATCH},
                {TIMEUUID, null, "59ad61d0-c540-11e2-881e-b9e6057626c4", NO_MATCH},
                {TIMEUUID, "59ad61d0-c540-11e2-881e-b9e6057626c4", null, NO_MATCH},

                {TIMEUUID, new Date(1l), "59ad61d0-c540-11e2-881e-b9e6057626c4", ILLEGAL_ARGUMENT},
                {TIMEUUID, 1l, "59ad61d0-c540-11e2-881e-b9e6057626c4", ILLEGAL_ARGUMENT},
                {TIMEUUID, 1, "59ad61d0-c540-11e2-881e-b9e6057626c4", ILLEGAL_ARGUMENT},
                {TIMEUUID, "1", "59ad61d0-c540-11e2-881e-b9e6057626c4", ILLEGAL_ARGUMENT},
                {TIMEUUID, new BigInteger("1"), "59ad61d0-c540-11e2-881e-b9e6057626c4", ILLEGAL_ARGUMENT},
                {TIMEUUID, "hello", "59ad61d0-c540-11e2-881e-b9e6057626c4", ILLEGAL_ARGUMENT},

                {UUID, "59ad61d0-c540-11e2-881e-b9e6057626c4", "59ad61d0-c540-11e2-881e-b9e6057626c4", MATCH},
                {UUID, java.util.UUID.fromString("59ad61d0-c540-11e2-881e-b9e6057626c4"), "59ad61d0-c540-11e2-881e-b9e6057626c4", MATCH},

                {UUID, java.util.UUID.randomUUID(), "59ad61d0-c540-11e2-881e-b9e6057626c4", NO_MATCH},
                {UUID, java.util.UUID.randomUUID().toString(), "59ad61d0-c540-11e2-881e-b9e6057626c4", NO_MATCH},
                {UUID, null, "59ad61d0-c540-11e2-881e-b9e6057626c4", NO_MATCH},
                {UUID, "59ad61d0-c540-11e2-881e-b9e6057626c4", null, NO_MATCH},

                {UUID, new Date(1l), "59ad61d0-c540-11e2-881e-b9e6057626c4", ILLEGAL_ARGUMENT},
                {UUID, 1l, "59ad61d0-c540-11e2-881e-b9e6057626c4", ILLEGAL_ARGUMENT},
                {UUID, 1, "59ad61d0-c540-11e2-881e-b9e6057626c4", ILLEGAL_ARGUMENT},
                {UUID, "1", "59ad61d0-c540-11e2-881e-b9e6057626c4", ILLEGAL_ARGUMENT},
                {UUID, new BigInteger("1"), "59ad61d0-c540-11e2-881e-b9e6057626c4", ILLEGAL_ARGUMENT},
                {UUID, "hello", "59ad61d0-c540-11e2-881e-b9e6057626c4", ILLEGAL_ARGUMENT},

                {INET, InetAddress.getLocalHost(), InetAddress.getLocalHost().getHostAddress(), MATCH},
                {INET, InetAddress.getLocalHost().getHostAddress(), InetAddress.getLocalHost().getHostAddress(), MATCH},

                {INET, InetAddress.getLocalHost(), "192.168.56.56", NO_MATCH},
                {INET, InetAddress.getLocalHost().getHostAddress(), "192.168.56.56", NO_MATCH},

                {INET, null, InetAddress.getLocalHost().getHostAddress(), NO_MATCH},
                {INET, InetAddress.getLocalHost().getHostAddress(), null, NO_MATCH},

                {INET, new Date(1l), InetAddress.getLocalHost().getHostAddress(), ILLEGAL_ARGUMENT},
                {INET, 1l, InetAddress.getLocalHost().getHostAddress(), ILLEGAL_ARGUMENT},
                {INET, 1, InetAddress.getLocalHost().getHostAddress(), ILLEGAL_ARGUMENT},
                {INET, new BigInteger("1"), InetAddress.getLocalHost().getHostAddress(), ILLEGAL_ARGUMENT},

                {INET, new Date(1l), InetAddress.getLocalHost().getHostAddress(), ILLEGAL_ARGUMENT},
                {INET, 1l, InetAddress.getLocalHost().getHostAddress(), ILLEGAL_ARGUMENT},
                {INET, 1, InetAddress.getLocalHost().getHostAddress(), ILLEGAL_ARGUMENT},
                {INET, new BigInteger("1"), InetAddress.getLocalHost().getHostAddress(), ILLEGAL_ARGUMENT},

                {DATE, 1, "1", MATCH},
                {DATE, 1, 1d, MATCH},
                {DATE, "1", 1d, MATCH},
                {DATE, new BigInteger("1"), 1d, MATCH},

                {DATE, null, 1, NO_MATCH},
                {DATE, "hello", 1d, ILLEGAL_ARGUMENT},

                {TIME, 1, "1", MATCH},
                {TIME, 1, 1d, MATCH},
                {TIME, "1", 1d, MATCH},
                {TIME, new BigInteger("1"), 1d, MATCH},

                {TIME, null, 1, NO_MATCH},
                {TIME, "hello", 1d, ILLEGAL_ARGUMENT},

                {TINY_INT, 1, "1", MATCH},
                {TINY_INT, 1, 1d, MATCH},
                {TINY_INT, "1", 1d, MATCH},
                {TINY_INT, new BigInteger("1"), 1d, MATCH},

                {TINY_INT, null, 1, NO_MATCH},
                {TINY_INT, "hello", 1d, ILLEGAL_ARGUMENT},

                {SMALL_INT, 1, "1", MATCH},
                {SMALL_INT, 1, 1d, MATCH},
                {SMALL_INT, "1", 1d, MATCH},
                {SMALL_INT, new BigInteger("1"), 1d, MATCH},

                {SMALL_INT, null, 1, NO_MATCH},
                {SMALL_INT, "hello", 1d, ILLEGAL_ARGUMENT},

                {new SetType(TEXT), Sets.newHashSet("one"), Lists.newArrayList("one"), MATCH},
                {new SetType(TEXT), Sets.newHashSet("one"), Lists.newArrayList("two"), NO_MATCH},
                {new SetType(TEXT), Sets.newHashSet("one"), Lists.newArrayList("one", "two"), NO_MATCH},
                {new SetType(TEXT), null, Lists.newArrayList("one", "two"), NO_MATCH},
                {new SetType(TEXT), Sets.newHashSet("one"), null, NO_MATCH},

                {new SetType(TEXT), new Date(1l), Lists.newArrayList("one"), ILLEGAL_ARGUMENT},
                {new SetType(TEXT), 1l, Lists.newArrayList("one"), ILLEGAL_ARGUMENT},
                {new SetType(TEXT), 1, Lists.newArrayList("one"), ILLEGAL_ARGUMENT},
                {new SetType(TEXT), new BigInteger("1"), Lists.newArrayList("one"), ILLEGAL_ARGUMENT},

                {new SetType(ASCII), Sets.newHashSet("one"), Lists.newArrayList("one"), MATCH},
                {new SetType(ASCII), Sets.newHashSet("one"), Lists.newArrayList("one", "two"), NO_MATCH},
                {new SetType(ASCII), null, Lists.newArrayList("one", "two"), NO_MATCH},
                {new SetType(ASCII), Sets.newHashSet("one"), null, NO_MATCH},

                {new SetType(ASCII), new Date(1l), Lists.newArrayList("one"), ILLEGAL_ARGUMENT},
                {new SetType(ASCII), 1l, Lists.newArrayList("one"), ILLEGAL_ARGUMENT},
                {new SetType(ASCII), 1, Lists.newArrayList("one"), ILLEGAL_ARGUMENT},
                {new SetType(ASCII), new BigInteger("1"), Lists.newArrayList("one"), ILLEGAL_ARGUMENT},

                {new SetType(VARCHAR), Sets.newHashSet("one"), Lists.newArrayList("one"), MATCH},
                {set(VARCHAR), Sets.newHashSet("one"), Lists.newArrayList("one", "two"), NO_MATCH},
                {set(VARCHAR), null, Lists.newArrayList("one", "two"), NO_MATCH},
                {set(VARCHAR), Sets.newHashSet("one"), null, NO_MATCH},

                {set(VARCHAR), new Date(1l), Lists.newArrayList("one"), ILLEGAL_ARGUMENT},
                {set(VARCHAR), 1l, Lists.newArrayList("one"), ILLEGAL_ARGUMENT},
                {set(VARCHAR), 1, Lists.newArrayList("one"), ILLEGAL_ARGUMENT},
                {set(VARCHAR), new BigInteger("1"), Lists.newArrayList("one"), ILLEGAL_ARGUMENT},

                {set(UUID), Sets.newHashSet(java.util.UUID.randomUUID()), Lists.newArrayList("59ad61d0-c540-11e2-881e-b9e6057626c4"), NO_MATCH},
                {set(UUID), Sets.newHashSet(java.util.UUID.randomUUID().toString()), Lists.newArrayList("59ad61d0-c540-11e2-881e-b9e6057626c4"), NO_MATCH},
                {set(UUID), Sets.newHashSet((java.util.UUID) null), Lists.newArrayList("59ad61d0-c540-11e2-881e-b9e6057626c4"), NO_MATCH},
                {set(UUID), Sets.newHashSet("59ad61d0-c540-11e2-881e-b9e6057626c4"), Lists.newArrayList((java.util.UUID) null), NO_MATCH},

                {set(UUID), new Date(1l), Lists.newArrayList("59ad61d0-c540-11e2-881e-b9e6057626c4"), ILLEGAL_ARGUMENT},
                {set(UUID), 1l, Lists.newArrayList("59ad61d0-c540-11e2-881e-b9e6057626c4"), ILLEGAL_ARGUMENT},
                {set(UUID), 1, Lists.newArrayList("59ad61d0-c540-11e2-881e-b9e6057626c4"), ILLEGAL_ARGUMENT},
                {set(UUID), "1", Lists.newArrayList("59ad61d0-c540-11e2-881e-b9e6057626c4"), ILLEGAL_ARGUMENT},
                {set(UUID), new BigInteger("1"), Lists.newArrayList("59ad61d0-c540-11e2-881e-b9e6057626c4"), ILLEGAL_ARGUMENT},
                {set(UUID), "hello", Lists.newArrayList("59ad61d0-c540-11e2-881e-b9e6057626c4"), ILLEGAL_ARGUMENT},

                {list(TEXT), Lists.newArrayList("one"), Lists.newArrayList("one"), MATCH},
                {list(TEXT), Lists.newArrayList("one"), Lists.newArrayList("two"), NO_MATCH},
                {list(TEXT), Lists.newArrayList("one", "two"), Lists.newArrayList("one", "two"), MATCH},
                {list(TEXT), Lists.newArrayList("one", "two"), Lists.newArrayList("two", "one"), NO_MATCH},
                {list(TEXT), Lists.newArrayList("one"), Lists.newArrayList("one", "two"), NO_MATCH},
                {list(TEXT), null, Lists.newArrayList("one", "two"), NO_MATCH},
                {list(TEXT), Lists.newArrayList("one"), null, NO_MATCH},

                {list(TEXT), new Date(1l), Lists.newArrayList("one"), ILLEGAL_ARGUMENT},
                {list(TEXT), 1l, Lists.newArrayList("one"), ILLEGAL_ARGUMENT},
                {list(TEXT), 1, Lists.newArrayList("one"), ILLEGAL_ARGUMENT},
                {list(TEXT), new BigInteger("1"), Lists.newArrayList("one"), ILLEGAL_ARGUMENT},

                {map(TEXT, TEXT), ImmutableMap.of("ONE", "1"), ImmutableMap.of("ONE", "1"), MATCH},
                {map(TEXT, TEXT), ImmutableMap.of(), ImmutableMap.of(), MATCH},
                {map(TEXT, TEXT), ImmutableMap.of("ONE", "1", "TWO", "2"), ImmutableMap.of("ONE", "1"), NO_MATCH},
                {map(TEXT, TEXT), ImmutableMap.of("ONE", "1"), ImmutableMap.of("ONE", "1", "TWO", "2"), NO_MATCH},
                {map(TEXT, TEXT), ImmutableMap.of("ONE", "1"), ImmutableMap.of("ONE", "2"), NO_MATCH},
                {map(TEXT, TEXT), ImmutableMap.of("ONE", "1"), ImmutableMap.of("ONE", "2"), NO_MATCH},
                {map(TEXT, TEXT), ImmutableMap.of("ONE", "1"), ImmutableMap.of("TWO", "1"), NO_MATCH},
                {map(TEXT, TEXT), null, ImmutableMap.of("TWO", "1"), NO_MATCH},
                {map(TEXT, TEXT), ImmutableMap.of("ONE", "1"), null, NO_MATCH},

                {map(TEXT, TEXT), new Date(1l), ImmutableMap.of("ONE", "1"), ILLEGAL_ARGUMENT},
                {map(TEXT, TEXT), 1l, ImmutableMap.of("ONE", "1"), ILLEGAL_ARGUMENT},
                {map(TEXT, TEXT), 1, ImmutableMap.of("ONE", "1"), ILLEGAL_ARGUMENT},
                {map(TEXT, TEXT), new BigInteger("1"), ImmutableMap.of("ONE", "1"), ILLEGAL_ARGUMENT},

                {tuple(TEXT, INT), Lists.newArrayList("one", 1), Lists.newArrayList("one", 1), MATCH},
                {tuple(TEXT, INT), Lists.newArrayList("one", 1), Lists.newArrayList("two", 1), NO_MATCH},
                {tuple(TEXT, INT), Lists.newArrayList("one", 1), Lists.newArrayList("one", 2), NO_MATCH},

                {tuple(TEXT, INT), null, Lists.newArrayList("one", 1), NO_MATCH},
                {tuple(TEXT, INT), Lists.newArrayList("one", 1), null, NO_MATCH},

                {tuple(TEXT, INT), new Date(1l), Lists.newArrayList("one", 1), ILLEGAL_ARGUMENT},
                {tuple(TEXT, INT), 1l, Lists.newArrayList("one", 1), ILLEGAL_ARGUMENT},
                {tuple(TEXT, INT), 1, Lists.newArrayList("one", 1), ILLEGAL_ARGUMENT},
                {tuple(TEXT, INT), new BigInteger("1"), Lists.newArrayList("one", 1), ILLEGAL_ARGUMENT},

                {tuple(TEXT, INT), Lists.newArrayList("one"), Lists.newArrayList("one"), ILLEGAL_ARGUMENT},
        });
    }

    private CqlType type;
    private Object expected;
    private Object actual;
    private Result match;

    public CqlTypesEqualsTest(CqlType type, Object expected, Object actual, Result match) {
        this.type = type;
        this.expected = expected;
        this.actual = actual;
        this.match = match;
    }

    @Test
    public void test() throws Exception {
        switch(match) {
            case MATCH : {
                assertTrue(type.equals(expected, actual));
                break;
            }
            case NO_MATCH : {
                boolean equals = type.equals(expected, actual);
                assertFalse("Expected no match but got: " + equals, equals);
                break;
            }
            case ILLEGAL_ARGUMENT : {
                try {
                    boolean equals = type.equals(expected, actual);
                    fail("Expected Illegal Argument Exception, actual: " + equals);
                } catch (IllegalArgumentException e) {
                    assertTrue(e.getMessage().contains(type.serialise()));
                }
                break;
            }
        }
    }
}