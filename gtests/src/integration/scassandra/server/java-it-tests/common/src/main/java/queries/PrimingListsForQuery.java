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
import org.junit.Test;
import org.scassandra.cql.CqlType;
import org.scassandra.cql.ListType;
import org.scassandra.cql.PrimitiveType;
import org.scassandra.http.client.ColumnTypes;
import org.scassandra.http.client.PrimingRequest;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.scassandra.cql.ListType.*;

public class PrimingListsForQuery extends AbstractScassandraTest {

    public PrimingListsForQuery(CassandraExecutor cassandraExecutor) {
        super(cassandraExecutor);
    }

    /*
CQL3 data type	Java type
ascii	java.lang.String     - DONE
bigint	long                 - DONE
blob	java.nio.ByteBuffer  - DONE
boolean	boolean              - DONE
counter	long                 - Counters not allowed in lists
decimal	java.math.BigDecimal - DONE
double	double               - DONE
float	float                - DONE
inet	java.net.InetAddress - DONE
int	int                      - DONE
text	java.lang.String     - DONE
timestamp	java.util.Date   - DONE
timeuuid	java.util.UUID   - DONE
uuid	java.util.UUID       - DONE
varchar	java.lang.String     - DONE
varint	java.math.BigInteger - DONE
     */

    @Test
    public void testVarcharTextAndAsciiLists() {
        String query = "select * from something";
        ImmutableMap<String, ColumnTypes> columnTypes = ImmutableMap.of(
                "list_type_varchar", ColumnTypes.VarcharList,
                "list_type_text", ColumnTypes.TextList,
                "list_type_ascii", ColumnTypes.AsciiList
        );
        ImmutableMap<String, CqlType> columnTypesAsNewCqlType = ImmutableMap.of(
                "list_type_varchar", list(PrimitiveType.VARCHAR),
                "list_type_text", list(PrimitiveType.TEXT),
                "list_type_ascii", list(PrimitiveType.ASCII)
        );
        ImmutableMap<String, List<String>> rows = ImmutableMap.of(
                "list_type_varchar", Arrays.asList("one", "two", "three"),
                "list_type_text", Arrays.asList("one", "two", "three"),
                "list_type_ascii", Arrays.asList("one", "two", "three")
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
        assertEquals(Arrays.asList("one", "two", "three"), rowOne.getList("list_type_varchar", String.class));
        assertEquals(Arrays.asList("one", "two", "three"), rowOne.getList("list_type_text", String.class));
        assertEquals(Arrays.asList("one", "two", "three"), rowOne.getList("list_type_ascii", String.class));

        // Retrieve primes check
        List<PrimingRequest> primingRequests = AbstractScassandraTest.primingClient.retrieveQueryPrimes();
        assertEquals(columnTypesAsNewCqlType, primingRequests.get(0).getThen().getColumnTypes());
        assertEquals(Arrays.asList(rows), primingRequests.get(0).getThen().getRows());
    }

    @Test
    public void emptyTextLists() {
        String query = "select * from something";
        ImmutableMap<String, ColumnTypes> columnTypes = ImmutableMap.of(
                "list_type_varchar", ColumnTypes.VarcharList,
                "list_type_text", ColumnTypes.TextList,
                "list_type_ascii", ColumnTypes.AsciiList
        );
        ImmutableMap<String, CqlType> columnTypesAsNewCqlType = ImmutableMap.of(
                "list_type_varchar", list(PrimitiveType.VARCHAR),
                "list_type_text", list(PrimitiveType.TEXT),
                "list_type_ascii", list(PrimitiveType.ASCII)
        );
        ImmutableMap<String, List<String>> rows = ImmutableMap.of(
                "list_type_varchar", Arrays.asList(),
                "list_type_text", Arrays.asList(),
                "list_type_ascii", Arrays.asList()
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
        assertEquals(Collections.<String>emptyList(), rowOne.getList("list_type_varchar", String.class));
        assertEquals(Collections.<String>emptyList(), rowOne.getList("list_type_text", String.class));
        assertEquals(Collections.<String>emptyList(), rowOne.getList("list_type_ascii", String.class));

        // Retrieve primes check
        List<PrimingRequest> primingRequests = AbstractScassandraTest.primingClient.retrieveQueryPrimes();
        assertEquals(columnTypesAsNewCqlType, primingRequests.get(0).getThen().getColumnTypes());
        assertEquals(Arrays.asList(rows), primingRequests.get(0).getThen().getRows());
    }

    @Test
    public void testBigIntLong() {
        String query = "select * from something";
        ImmutableMap<String, ColumnTypes> columnTypes = ImmutableMap.of(
                "list_type_bigint", ColumnTypes.BigintList
        );
        ImmutableMap<String, List<Long>> rows = ImmutableMap.of(
                "list_type_bigint", Arrays.asList(1l, 2l, 3l)
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
        assertEquals(Arrays.asList(1l, 2l, 3l), rowOne.getList("list_type_bigint", Long.class));
    }

    @Test
    public void testBooleanList() {
        String query = "select * from something";
        ImmutableMap<String, ColumnTypes> columnTypes = ImmutableMap.of(
                "list_type_boolean", ColumnTypes.BooleanList
        );
        ImmutableMap<String, List<Boolean>> rows = ImmutableMap.of(
                "list_type_boolean", Arrays.asList(true, false, false)
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
        assertEquals(Arrays.asList(true, false, false), rowOne.getList("list_type_boolean", Boolean.class));
    }

    @Test
    public void testDecimalList() {
        String query = "select * from something";
        ImmutableMap<String, ColumnTypes> columnTypes = ImmutableMap.of(
                "list_type", ColumnTypes.DecimalList
        );
        ImmutableMap<String, List<BigDecimal>> rows = ImmutableMap.of(
                "list_type", Arrays.asList(new BigDecimal("1.1"), new BigDecimal("2.2"))
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
        assertEquals(Arrays.asList(new BigDecimal("1.1"), new BigDecimal("2.2")), rowOne.getList("list_type", BigDecimal.class));
    }

    @Test
    public void testDoubleList() {
        String query = "select * from something";
        ImmutableMap<String, ColumnTypes> columnTypes = ImmutableMap.of(
                "list_type", ColumnTypes.DoubleList
        );
        ImmutableMap<String, List<Double>> rows = ImmutableMap.of(
                "list_type", Arrays.asList(1.0, 2.0)
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
        assertEquals(Arrays.asList(1.0, 2.0), rowOne.getList("list_type", Double.class));
    }

    @Test
    public void testFloatList() {
        String query = "select * from something";
        ImmutableMap<String, ColumnTypes> columnTypes = ImmutableMap.of(
                "list_type", ColumnTypes.FloatList
        );
        ImmutableMap<String, List<Float>> rows = ImmutableMap.of(
                "list_type", Arrays.asList(1.0f, 2.0f)
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
        assertEquals(Arrays.asList(1.0f, 2.0f), rowOne.getList("list_type", Float.class));
    }

    @Test
    public void testInetList() throws UnknownHostException {
        String query = "select * from something";
        InetAddress inetAddress = InetAddress.getLocalHost();
        ImmutableMap<String, ColumnTypes> columnTypes = ImmutableMap.of(
                "list_type", ColumnTypes.InetList
        );
        ImmutableMap<String, List<InetAddress>> rows = ImmutableMap.of(
                "list_type", Arrays.asList(inetAddress)
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
        assertEquals(Arrays.asList(inetAddress), rowOne.getList("list_type", InetAddress.class));
    }

    @Test
    public void testBlobList() throws UnknownHostException {
        String query = "select * from something";
        ImmutableMap<String, ColumnTypes> columnTypes = ImmutableMap.of(
                "list_type", ColumnTypes.BlobList
        );
        ImmutableMap<String, List<String>> rows = ImmutableMap.of(
                "list_type", Arrays.asList("0x0012345435345345435435")
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
        assertArrayEquals(new byte[]{ 0, 18, 52, 84, 53, 52, 83, 69, 67, 84, 53 },
            getArray(rowOne.getList("list_type", ByteBuffer.class).get(0)));
    }

    @Test
    public void testIntList() {
        String query = "select * from something";
        ImmutableMap<String, ColumnTypes> columnTypes = ImmutableMap.of(
                "list_type", ColumnTypes.IntList
        );
        ImmutableMap<String, List<Integer>> rows = ImmutableMap.of(
                "list_type", Arrays.asList(1, 2)
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
        assertEquals(Arrays.asList(1, 2), rowOne.getList("list_type", Integer.class));
    }

    @Test
    public void testTimestampList() {
        String query = "select * from something";
        ImmutableMap<String, ColumnTypes> columnTypes = ImmutableMap.of(
                "list_type", ColumnTypes.TimestampList
        );
        Date now = new Date();
        ImmutableMap<String, List<Long>> rows = ImmutableMap.of(
                "list_type", Arrays.asList(now.getTime())
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
        assertEquals(Arrays.asList(now), rowOne.getList("list_type", Date.class));
    }

    @Test
    public void testTimeUUIDList() {
        String query = "select * from something";
        UUID timeUUID = UUID.fromString("6a8369e0-73f0-11e4-ac06-4b05b98cc84c");
        ImmutableMap<String, ColumnTypes> columnTypes = ImmutableMap.of(
                "list_type", ColumnTypes.TimeuuidList
        );
        ImmutableMap<String, List<UUID>> rows = ImmutableMap.of(
                "list_type", Arrays.asList(timeUUID, timeUUID)
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
        assertEquals(Arrays.asList(timeUUID, timeUUID), rowOne.getList("list_type", UUID.class));
    }

    @Test
    public void testUUIDList() {
        String query = "select * from something";
        UUID uuid = UUID.randomUUID();
        ImmutableMap<String, ColumnTypes> columnTypes = ImmutableMap.of(
                "list_type", ColumnTypes.UuidList
        );
        ImmutableMap<String, List<UUID>> rows = ImmutableMap.of(
                "list_type", Arrays.asList(uuid, uuid)
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
        assertEquals(Arrays.asList(uuid, uuid), rowOne.getList("list_type", UUID.class));
    }

    @Test
    public void testVarIntList() {
        String query = "select * from something";
        ImmutableMap<String, ColumnTypes> columnTypes = ImmutableMap.of(
                "list_type", ColumnTypes.VarintList
        );
        ImmutableMap<String, List<BigInteger>> rows = ImmutableMap.of(
                "list_type", Arrays.asList(new BigInteger("1"), new BigInteger("2"))
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
        assertEquals(Arrays.asList(new BigInteger("1"), new BigInteger("2")), rowOne.getList("list_type", BigInteger.class));
    }

}
