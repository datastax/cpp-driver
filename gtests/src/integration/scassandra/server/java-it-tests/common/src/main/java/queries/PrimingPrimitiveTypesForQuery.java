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
package queries;

import com.google.common.collect.ImmutableMap;
import common.AbstractScassandraTest;
import common.CassandraExecutor;
import common.CassandraRow;
import org.junit.Assert;
import org.junit.Test;
import org.scassandra.cql.CqlType;
import org.scassandra.http.client.ColumnTypes;
import org.scassandra.http.client.PrimingRequest;
import org.scassandra.http.client.Consistency;
import org.scassandra.http.client.Result;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.scassandra.http.client.Consistency.ONE;
import static org.scassandra.http.client.Consistency.TWO;

public class PrimingPrimitiveTypesForQuery extends AbstractScassandraTest {

    public PrimingPrimitiveTypesForQuery(CassandraExecutor cassandraExecutor) {
        super(cassandraExecutor);
    }

    @Test
    public void testNumericTypesAsStrings() {
        String query = "select * from something";
        ImmutableMap<String, ColumnTypes> columnTypes = ImmutableMap.of(
                "int_type", ColumnTypes.Int,
                "bigint_type", ColumnTypes.Bigint,
                "varint_type", ColumnTypes.Varint,
                "double_type", ColumnTypes.Double,
                "float_type", ColumnTypes.Float
        );
        Map<String, CqlType> newTypes = columnTypes.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getType()));
        PrimingRequest prime = PrimingRequest.queryBuilder()
                .withQuery(query)
                .withConsistency(ONE)
                .withColumnTypes(columnTypes)
                .withRows(ImmutableMap.of(
                                "int_type", "123456789",
                                "bigint_type", "2222222222"),
                        ImmutableMap.of(
                                "varint_type", "11111111",
                                "double_type", "0.7829",
                                "float_type", "7.12345"
                        ))
                .build();
        primingClient.primeQuery(prime);

        List<CassandraRow> result = cassandra().executeQuery(query).rows();

        // Stubbing check
        assertEquals(result.size(), 2);
        CassandraRow rowOne = result.get(0);
        assertEquals(123456789, rowOne.getInt("int_type"));
        assertEquals(2222222222l, rowOne.getLong("bigint_type"));
        CassandraRow rowTwo = result.get(1);
        assertEquals(new BigInteger("11111111"), rowTwo.getVarint("varint_type"));
        assertEquals(0.7829, rowTwo.getDouble("double_type"), 0.1);
        assertEquals(7.12345f, rowTwo.getFloat("float_type"), 0.1);

        // Retrieve primes check
        List<PrimingRequest> primingRequests = primingClient.retrieveQueryPrimes();
        assertEquals(1, primingRequests.size());
        assertEquals(query, primingRequests.get(0).getWhen().getQuery());
        assertEquals(Arrays.asList(ONE), primingRequests.get(0).getWhen().getConsistency());
        assertEquals(newTypes, primingRequests.get(0).getThen().getColumnTypes());
    }

    @Test
    public void testNumericTypesAsNumbers() {
        String query = "select * from something";
        ImmutableMap<String, ColumnTypes> columnTypes = ImmutableMap.of(
                "int_type", ColumnTypes.Int,
                "bigint_type", ColumnTypes.Bigint,
                "varint_type", ColumnTypes.Varint,
                "double_type", ColumnTypes.Double,
                "float_type", ColumnTypes.Float
        );
        Map<String, CqlType> newTypes = columnTypes.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getType()));
        PrimingRequest prime = PrimingRequest.queryBuilder()
                .withQuery(query)
                .withConsistency(TWO, Consistency.THREE)
                .withColumnTypes(columnTypes)
                .withRows(ImmutableMap.of(
                                "int_type", 123456789,
                                "bigint_type", 2222222222l),
                        ImmutableMap.of(
                                "varint_type", 11111111,
                                "double_type", 0.7829,
                                "float_type", 7.12345
                        ))
                .build();
        primingClient.primeQuery(prime);

        List<CassandraRow> result = cassandra().executeSimpleStatement(query, "THREE").rows();

        assertEquals(result.size(), 2);
        CassandraRow rowOne = result.get(0);
        assertEquals(123456789, rowOne.getInt("int_type"));
        assertEquals(2222222222l, rowOne.getLong("bigint_type"));
        CassandraRow rowTwo = result.get(1);
        assertEquals(new BigInteger("11111111"), rowTwo.getVarint("varint_type"));
        assertEquals(0.7829, rowTwo.getDouble("double_type"), 0.1);
        assertEquals(7.12345f, rowTwo.getFloat("float_type"), 0.1);

        // Retrieve primes check
        List<PrimingRequest> primingRequests = primingClient.retrieveQueryPrimes();
        assertEquals(Arrays.asList(Consistency.TWO, Consistency.THREE), primingRequests.get(0).getWhen().getConsistency());
        assertEquals(newTypes, primingRequests.get(0).getThen().getColumnTypes());
    }

    @Test
    public void testDecimals() {
        String query = "select * from something";
        ImmutableMap<String, ColumnTypes> columnTypes = ImmutableMap.of(
                "decimal_type", ColumnTypes.Decimal
        );
        PrimingRequest prime = PrimingRequest.queryBuilder()
                .withQuery(query)
                .withConsistency(Consistency.TWO, Consistency.THREE)
                .withColumnTypes(columnTypes)
                .withRows(ImmutableMap.of(
                        "decimal_type", "12.5"
                ))
                .build();
        primingClient.primeQuery(prime);

        List<CassandraRow> result = cassandra().executeSimpleStatement(query, "THREE").rows();

        assertEquals(result.size(), 1);
        CassandraRow rowOne = result.get(0);
        assertEquals(new BigDecimal("12.5"), rowOne.getDecimal("decimal_type"));
    }

    @Test
    public void testCountersAsANumberAndString() {
        String query = "select * from something";
        ImmutableMap<String, ColumnTypes> columnTypes = ImmutableMap.of(
                "counter_type", ColumnTypes.Counter
        );
        Map<String, CqlType> newTypes = columnTypes.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getType()));
        PrimingRequest prime = PrimingRequest.queryBuilder()
                .withQuery(query)
                .withConsistency(Consistency.QUORUM, Consistency.EACH_QUORUM, Consistency.LOCAL_QUORUM)
                .withColumnTypes(columnTypes)
                .withRows(ImmutableMap.of(
                                "counter_type", 123456789),
                        ImmutableMap.of(
                                "counter_type", "11111111"
                        ))
                .build();
        primingClient.primeQuery(prime);

        List<CassandraRow> result = cassandra().executeSimpleStatement(query, "QUORUM").rows();

        assertEquals(result.size(), 2);
        CassandraRow rowOne = result.get(0);
        assertEquals(123456789, rowOne.getLong("counter_type"));
        CassandraRow rowTwo = result.get(1);
        assertEquals(11111111, rowTwo.getLong("counter_type"));

        // Retrieve primes check
        List<PrimingRequest> primingRequests = primingClient.retrieveQueryPrimes();
        assertEquals(Arrays.asList(Consistency.QUORUM, Consistency.EACH_QUORUM, Consistency.LOCAL_QUORUM), primingRequests.get(0).getWhen().getConsistency());
        assertEquals(newTypes, primingRequests.get(0).getThen().getColumnTypes());
    }

    @Test
    public void testStrings() {
        String query = "select * from something";
        ImmutableMap<String, ColumnTypes> columnTypes = ImmutableMap.of(
                "text_type", ColumnTypes.Text,
                "varchar_type", ColumnTypes.Varchar,
                "ascii_type", ColumnTypes.Ascii
        );
        Map<String, CqlType> newTypes = columnTypes.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getType()));
        PrimingRequest prime = PrimingRequest.queryBuilder()
                .withQuery(query)
                .withColumnTypes(columnTypes)
                .withRows(ImmutableMap.of(
                                "text_type", 123456789,
                                "varchar_type", false,
                                "ascii_type", 30),
                        ImmutableMap.of(
                                "text_type", "a string",
                                "ascii_type", 45.5
                        ))
                .build();
        primingClient.primeQuery(prime);

        List<CassandraRow> result = cassandra().executeQuery(query).rows();

        assertEquals(result.size(), 2);
        CassandraRow rowOne = result.get(0);
        assertEquals("123456789", rowOne.getString("text_type"));
        assertEquals("false", rowOne.getString("varchar_type"));
        assertEquals("30", rowOne.getString("ascii_type"));
        CassandraRow rowTwo = result.get(1);
        assertEquals("a string", rowTwo.getString("text_type"));
        assertEquals(null, rowTwo.getString("varchar_type"));
        assertEquals("45.5", rowTwo.getString("ascii_type"));

        // Retrieve primes check
        List<PrimingRequest> primingRequests = primingClient.retrieveQueryPrimes();
        assertEquals(Arrays.asList(Consistency.ANY, Consistency.ONE, Consistency.TWO, Consistency.THREE, Consistency.QUORUM,
                        Consistency.ALL, Consistency.LOCAL_QUORUM, Consistency.EACH_QUORUM, Consistency.SERIAL, Consistency.LOCAL_SERIAL, Consistency.LOCAL_ONE),
                primingRequests.get(0).getWhen().getConsistency());
        assertEquals(newTypes, primingRequests.get(0).getThen().getColumnTypes());
    }

    @Test
    public void testUUIDs() {
        String query = "select * from something";
        ImmutableMap<String, ColumnTypes> columnTypes = ImmutableMap.of(
                "uuid_type", ColumnTypes.Uuid,
                "timeuuid_type", ColumnTypes.Timeuuid
        );
        Map<String, CqlType> newTypes = columnTypes.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getType()));

        PrimingRequest prime = PrimingRequest.queryBuilder()
                .withQuery(query)
                .withColumnTypes(columnTypes)
                .withRows(ImmutableMap.of(
                        "uuid_type", "550e8400-e29b-41d4-a716-446655440000",
                        "timeuuid_type", "59ad61d0-c540-11e2-881e-b9e6057626c4"
                ), ImmutableMap.of(
                        "uuid_type", UUID.fromString("550e8400-e29b-41d4-a716-446655440000"),
                        "timeuuid_type", UUID.fromString("59ad61d0-c540-11e2-881e-b9e6057626c4")
                ))
                .build();
        primingClient.primeQuery(prime);

        List<CassandraRow> result = cassandra().executeQuery(query).rows();

        assertEquals(result.size(), 2);
        CassandraRow rowOne = result.get(0);
        assertEquals(UUID.fromString("550e8400-e29b-41d4-a716-446655440000"), rowOne.getUUID("uuid_type"));
        assertEquals(UUID.fromString("59ad61d0-c540-11e2-881e-b9e6057626c4"), rowOne.getUUID("timeuuid_type"));

        CassandraRow rowTwo = result.get(1);
        assertEquals(UUID.fromString("550e8400-e29b-41d4-a716-446655440000"), rowTwo.getUUID("uuid_type"));
        assertEquals(UUID.fromString("59ad61d0-c540-11e2-881e-b9e6057626c4"), rowTwo.getUUID("timeuuid_type"));

        // Retrieve primes check
        List<PrimingRequest> primingRequests = primingClient.retrieveQueryPrimes();
        assertEquals(Result.success, primingRequests.get(0).getThen().getResult());
        assertEquals(newTypes, primingRequests.get(0).getThen().getColumnTypes());
    }

    @Test
    public void testBooleans() {
        String query = "select * from something";
        ImmutableMap<String, ColumnTypes> columnTypes = ImmutableMap.of(
                "bool_type1", ColumnTypes.Boolean,
                "bool_type2", ColumnTypes.Boolean
        );
        Map<String, CqlType> newTypes = columnTypes.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getType()));
        PrimingRequest prime = PrimingRequest.queryBuilder()
                .withQuery(query)
                .withColumnTypes(columnTypes)
                .withRows(ImmutableMap.of(
                        "bool_type1", "true",
                        "bool_type2", "false"
                ), ImmutableMap.of(
                        "bool_type1", true,
                        "bool_type2", false
                ), ImmutableMap.of(
                        "bool_type1", "TRUE",
                        "bool_type2", "FALSE"
                ))
                .build();
        primingClient.primeQuery(prime);

        List<CassandraRow> result = cassandra().executeQuery(query).rows();

        assertEquals(result.size(), 3);
        CassandraRow rowOne = result.get(0);
        assertEquals(true, rowOne.getBool("bool_type1"));
        assertEquals(false, rowOne.getBool("bool_type2"));
        CassandraRow rowTwo = result.get(0);
        assertEquals(true, rowTwo.getBool("bool_type1"));
        assertEquals(false, rowTwo.getBool("bool_type2"));
        CassandraRow rowThree = result.get(0);
        assertEquals(true, rowThree.getBool("bool_type1"));
        assertEquals(false, rowThree.getBool("bool_type2"));

        // Retrieve primes check
        List<PrimingRequest> primingRequests = primingClient.retrieveQueryPrimes();
        assertEquals(newTypes, primingRequests.get(0).getThen().getColumnTypes());
    }

    @Test
    public void testBlobHexStrings() {
        String query = "select * from something";
        ImmutableMap<String, ColumnTypes> columnTypes = ImmutableMap.of(
                "blob_type1", ColumnTypes.Blob,
                "blob_type2", ColumnTypes.Blob
        );
        Map<String, CqlType> newTypes = columnTypes.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getType()));
        PrimingRequest prime = PrimingRequest.queryBuilder()
                .withQuery(query)
                .withColumnTypes(columnTypes)
                .withRows(ImmutableMap.of(
                        "blob_type1", "0x0000000000000003",
                        "blob_type2", "0x0012345435345345435435"
                ), ImmutableMap.of(
                        "blob_type1", "0x001234",
                        "blob_type2", "0x48656c6c6f"
                ), ImmutableMap.of(
                        "blob_type1", "0x00",
                        "blob_type2", "0x11110000000000000003"
                ))
                .build();
        primingClient.primeQuery(prime);

        List<CassandraRow> result = cassandra().executeQuery(query).rows();

        assertEquals(3, result.size());
        CassandraRow rowOne = result.get(0);
        Assert.assertArrayEquals(new byte[]{0, 0, 0, 0, 0, 0, 0, 3}, byteBufferToArray(rowOne.getBytes("blob_type1")));
        Assert.assertArrayEquals(new byte[]{0, 18, 52, 84, 53, 52, 83, 69, 67, 84, 53}, byteBufferToArray(rowOne.getBytes("blob_type2")));
        CassandraRow rowTwo = result.get(1);
        Assert.assertArrayEquals(new byte[]{0, 18, 52}, byteBufferToArray(rowTwo.getBytes("blob_type1")));
        Assert.assertArrayEquals(new byte[]{72, 101, 108, 108, 111}, byteBufferToArray(rowTwo.getBytes("blob_type2")));
        CassandraRow rowThree = result.get(2);
        Assert.assertArrayEquals(new byte[]{0}, byteBufferToArray(rowThree.getBytes("blob_type1")));
        Assert.assertArrayEquals(new byte[]{17, 17, 0, 0, 0, 0, 0, 0, 0, 3}, byteBufferToArray(rowThree.getBytes("blob_type2")));

        // Retrieve primes check
        List<PrimingRequest> primingRequests = primingClient.retrieveQueryPrimes();
        assertEquals(newTypes, primingRequests.get(0).getThen().getColumnTypes());
    }

    @Test
    public void testInets() throws Exception {
        String query = "select * from something";
        ImmutableMap<String, ColumnTypes> columnTypes = ImmutableMap.of(
                "inet_type1", ColumnTypes.Inet,
                "inet_type2", ColumnTypes.Inet
        );

        PrimingRequest prime = PrimingRequest.queryBuilder()
                .withQuery(query)
                .withColumnTypes(columnTypes)
                .withRows(ImmutableMap.of(
                        "inet_type1", "127.0.0.1",
                        "inet_type2", "fdda:5cc1:23:4::1f"
                ))
                .build();
        primingClient.primeQuery(prime);

        List<CassandraRow> result = cassandra().executeQuery(query).rows();

        assertEquals(1, result.size());
        CassandraRow rowOne = result.get(0);
        assertEquals(InetAddress.getByName("127.0.0.1"), rowOne.getInet("inet_type1"));
        assertEquals(InetAddress.getByName("fdda:5cc1:23:4::1f"), rowOne.getInet("inet_type2"));

    }

    private byte[] byteBufferToArray(ByteBuffer buffer) {
        int length = buffer.remaining();
        byte[] array = new byte[length];
        buffer.get(array, 0, array.length);
        return array;
    }
}
