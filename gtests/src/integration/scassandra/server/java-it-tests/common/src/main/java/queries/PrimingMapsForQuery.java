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
import org.junit.*;
import org.scassandra.cql.CqlType;
import org.scassandra.cql.MapType;
import org.scassandra.http.client.ColumnTypes;
import org.scassandra.http.client.PrimingRequest;
import org.scassandra.http.client.types.ColumnMetadata;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableMap.of;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.scassandra.cql.PrimitiveType.*;
import static org.scassandra.cql.MapType.*;
import static org.scassandra.http.client.types.ColumnMetadata.column;

public class PrimingMapsForQuery extends AbstractScassandraTest {

    public PrimingMapsForQuery(CassandraExecutor cassandraExecutor) {
        super(cassandraExecutor);
    }

    @Test
    public void primingTextMaps() throws Exception {
        String query = "select * from something";
        ImmutableMap<String, ColumnTypes> columnTypes = of(
                "varchar_map", ColumnTypes.VarcharVarcharMap,
                "text_map", ColumnTypes.TextTextMap,
                "ascii_map", ColumnTypes.AsciiAsciiMap
        );
        Map<String, CqlType> newTypes = columnTypes.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getType()));
        ImmutableMap<String, Map<String, String>> rows = ImmutableMap.<String, Map<String, String>>of(
                "varchar_map", of("one", "two"),
                "text_map", of("three", "four"),
                "ascii_map", of()
        );
        PrimingRequest prime = PrimingRequest.queryBuilder()
                .withQuery(query)
                .withColumnTypes(columnTypes)
                .withRows(rows)
                .build();
        AbstractScassandraTest.primingClient.prime(prime);

        List<CassandraRow> result = cassandra().executeQuery(query).rows();

        assertEquals(result.size(), 1);
        CassandraRow rowOne = result.get(0);
        assertEquals(of("one", "two"), rowOne.getMap("varchar_map", String.class, String.class));
        assertEquals(of("three",     "four"), rowOne.getMap("text_map", String.class, String.class));
        assertEquals(of(), rowOne.getMap("ascii_map", String.class, String.class));

        // Retrieve primes check
        List<PrimingRequest> primingRequests = AbstractScassandraTest.primingClient.retrieveQueryPrimes();
        assertEquals(newTypes, primingRequests.get(0).getThen().getColumnTypes());
        assertEquals(Arrays.asList(rows), primingRequests.get(0).getThen().getRows());
    }

    @Test
    public void primingNumberMaps() throws Exception {
        String query = "select * from something";
        ImmutableMap<String, Map<?, ?>> rows = ImmutableMap.<String, Map<?, ?>>of(
                "bigint_map", of(1l, 2l),
                "int_map", of(1, 2),
                "varint_map", of(new BigInteger("1"), new BigInteger("2"))
        );
        PrimingRequest prime = PrimingRequest.queryBuilder()
                .withQuery(query)
                .withColumnTypes(
                        column("bigint_map", map(BIG_INT, BIG_INT)),
                        column("int_map", map(INT, INT)),
                        column("varint_map", map(VAR_INT, VAR_INT))
                )
                .withRows(rows)
                .build();
        AbstractScassandraTest.primingClient.prime(prime);

        List<CassandraRow> result = cassandra().executeQuery(query).rows();

        assertEquals(result.size(), 1);
        CassandraRow rowOne = result.get(0);
        assertEquals(of(1l, 2l), rowOne.getMap("bigint_map", Long.class, Long.class));
        assertEquals(of(1, 2), rowOne.getMap("int_map", Integer.class, Integer.class));
        assertEquals(of(new BigInteger("1"), new BigInteger("2")), rowOne.getMap("varint_map", BigInteger.class, BigInteger.class));

    }

    @Test
    public void primingDecimals() throws Exception {
        String query = "select * from something";
        ImmutableMap<String, Map<?, ?>> rows = ImmutableMap.<String, Map<?, ?>>of(
                "double_map", of(1.0, 2.0),
                "float_map", of(3.0f, 4.0f),
                "decimal_map", of(new BigDecimal("5.0"), new BigDecimal("6.0"))
        );
        PrimingRequest prime = PrimingRequest.queryBuilder()
                .withQuery(query)
                .withColumnTypes(
                        column("double_map", map(DOUBLE, DOUBLE)),
                        column("float_map", map(FLOAT, FLOAT)),
                        column("decimal_map", map(DECIMAL, DECIMAL))
                )
                .withRows(rows)
                .build();
        AbstractScassandraTest.primingClient.prime(prime);

        List<CassandraRow> result = cassandra().executeQuery(query).rows();

        assertEquals(result.size(), 1);
        CassandraRow rowOne = result.get(0);
        assertEquals(of(1.0, 2.0), rowOne.getMap("double_map", Double.class, Double.class));
        assertEquals(of(3.0f, 4.0f), rowOne.getMap("float_map", Float.class, Float.class));
        assertEquals(of(new BigDecimal("5.0"), new BigDecimal("6.0")), rowOne.getMap("decimal_map", BigDecimal.class, BigDecimal.class));
    }

    @Test
    public void primingUUIDsAndTimes() throws Exception {
        String query = "select * from something";
        Map<Date, Date> timestamps = of(new Date(), new Date());
        Map<Long, Long> timestampsAsLongs = timestamps.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().getTime(), e -> e.getValue().getTime()));

        Map<java.util.UUID, java.util.UUID> timeuuids = of(java.util.UUID.fromString("f3861e30-a6d7-11e4-826d-314eafee996d"), java.util.UUID.fromString("f5481980-a6d7-11e4-826d-314eafee996d"));
        Map<java.util.UUID, java.util.UUID> uuids = of(java.util.UUID.randomUUID(), java.util.UUID.randomUUID());
        Map<String, Map<?, ?>> rows = ImmutableMap.<String, Map<?, ?>>of(
                "uuid_map", uuids,
                "timeuuid_map", timeuuids,
                "timestamp_map", timestampsAsLongs
        );
        PrimingRequest prime = PrimingRequest.queryBuilder()
                .withQuery(query)
                .withColumnTypes(
                        column("uuid_map", map(UUID, UUID)),
                        column("timeuuid_map", map(TIMEUUID, TIMEUUID)),
                        column("timestamp_map", map(TIMESTAMP, TIMESTAMP))
                )
                .withRows(rows)
                .build();
        AbstractScassandraTest.primingClient.prime(prime);

        List<CassandraRow> result = cassandra().executeQuery(query).rows();

        assertEquals(result.size(), 1);
        CassandraRow rowOne = result.get(0);
        assertEquals(uuids, rowOne.getMap("uuid_map", java.util.UUID.class, java.util.UUID.class));
        assertEquals(timeuuids, rowOne.getMap("timeuuid_map", java.util.UUID.class, java.util.UUID.class));
        assertEquals(timestamps, rowOne.getMap("timestamp_map", Date.class, Date.class));
    }

    @Test
    public void testBlobBooleanInetMaps() throws Exception {
        String query = "select * from something";
        byte[] blob = new byte[]{0, 18, 52, 84, 53, 52, 83, 69, 67, 84, 53};
        InetAddress inetAddress = InetAddress.getLocalHost();
        Map<String, String> blobs = of("0x0012345435345345435435", "0x0012345435345345435435");

        Map<Boolean, Boolean> booleans = of(true, false);
        Map<InetAddress, InetAddress> inets = of(inetAddress, inetAddress);
        Map<String, Map<?, ?>> rows = ImmutableMap.<String, Map<?, ?>>of(
                "boolean_map", booleans,
                "inet_map", inets,
                "blob_map", blobs
        );
        PrimingRequest prime = PrimingRequest.queryBuilder()
                .withQuery(query)
                .withColumnTypes(
                        column("boolean_map", map(BOOLEAN, BOOLEAN)),
                        column("inet_map", map(INET, INET)),
                        column("blob_map", map(BLOB, BLOB))
                )
                .withRows(rows)
                .build();
        AbstractScassandraTest.primingClient.prime(prime);

        List<CassandraRow> result = cassandra().executeQuery(query).rows();

        assertEquals(result.size(), 1);
        CassandraRow rowOne = result.get(0);
        assertEquals(booleans, rowOne.getMap("boolean_map", Boolean.class, Boolean.class));
        assertEquals(inets, rowOne.getMap("inet_map", InetAddress.class, InetAddress.class));

        Map<ByteBuffer, ByteBuffer> actualBlobMap = rowOne.getMap("blob_map", ByteBuffer.class, ByteBuffer.class);
        Map.Entry<ByteBuffer, ByteBuffer> entry = actualBlobMap.entrySet().iterator().next();
        assertArrayEquals(blob, getArray(entry.getKey()));
        assertArrayEquals(blob, getArray(entry.getValue()));
    }
}
