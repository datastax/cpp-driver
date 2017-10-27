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
import com.google.common.collect.Sets;
import common.AbstractScassandraTest;
import common.CassandraExecutor;
import common.CassandraRow;
import org.junit.Test;
import org.scassandra.http.client.ColumnTypes;
import org.scassandra.http.client.PrimingRequest;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class PrimingSetsForQuery extends AbstractScassandraTest {

    public PrimingSetsForQuery(CassandraExecutor cassandraExecutor) {
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
    public void testVarcharTextAndAsciiSets() {
        String query = "select * from something";
        ImmutableMap<String, ColumnTypes> columnTypes = ImmutableMap.of(
                "set_type_varchar", ColumnTypes.VarcharSet,
                "set_type_text", ColumnTypes.TextSet,
                "set_type_ascii", ColumnTypes.AsciiSet
        );
        ImmutableMap<String, Set<String>> rows = ImmutableMap.of(
                "set_type_varchar", Sets.newHashSet("one", "two", "three"),
                "set_type_text", Sets.newHashSet("one", "two", "three"),
                "set_type_ascii", Sets.newHashSet("one", "two", "three")
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
        assertEquals(Sets.newHashSet("one", "two", "three"), rowOne.getSet("set_type_varchar", String.class));
        assertEquals(Sets.newHashSet("one", "two", "three"), rowOne.getSet("set_type_text", String.class));
        assertEquals(Sets.newHashSet("one", "two", "three"), rowOne.getSet("set_type_ascii", String.class));

    }

    @Test
    public void emptyTextSets() {
        String query = "select * from something";
        ImmutableMap<String, ColumnTypes> columnTypes = ImmutableMap.of(
                "set_type_varchar", ColumnTypes.VarcharSet,
                "set_type_text", ColumnTypes.TextSet,
                "set_type_ascii", ColumnTypes.AsciiSet
        );
        ImmutableMap<String, Set<String>> rows = ImmutableMap.of(
                "set_type_varchar", Sets.newHashSet(),
                "set_type_text", Sets.newHashSet(),
                "set_type_ascii", Sets.newHashSet()
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
        assertEquals(Sets.newHashSet(), rowOne.getSet("set_type_varchar", String.class));
        assertEquals(Sets.newHashSet(), rowOne.getSet("set_type_text", String.class));
        assertEquals(Sets.newHashSet(), rowOne.getSet("set_type_ascii", String.class));

    }

    @Test
    public void testBigIntLong() {
        String query = "select * from something";
        ImmutableMap<String, ColumnTypes> columnTypes = ImmutableMap.of(
                "set_type_bigint", ColumnTypes.BigintSet
        );
        ImmutableMap<String, Set<Long>> rows = ImmutableMap.of(
                "set_type_bigint", Sets.newHashSet(1l, 2l, 3l)
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
        assertEquals(Sets.newHashSet(1l, 2l, 3l), rowOne.getSet("set_type_bigint", Long.class));
    }

    @Test
    public void testBooleanList () {
        String query = "select * from something";
        ImmutableMap<String, ColumnTypes> columnTypes = ImmutableMap.of(
                "list_type_boolean", ColumnTypes.BooleanSet
        );
        ImmutableMap<String, Set<Boolean>> rows = ImmutableMap.of(
                "list_type_boolean", Sets.newHashSet(true, false, false)
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
        assertEquals(Sets.newHashSet(true, false, false), rowOne.getSet("list_type_boolean", Boolean.class));
    }

    @Test
    public void testDecimalList () {
        String query = "select * from something";
        ImmutableMap<String, ColumnTypes> columnTypes = ImmutableMap.of(
                "set_type", ColumnTypes.DecimalSet
        );
        ImmutableMap<String, Set<BigDecimal>> rows = ImmutableMap.of(
                "set_type", Sets.newHashSet(new BigDecimal("1.1"), new BigDecimal("2.2"))
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
        assertEquals(Sets.newHashSet(new BigDecimal("1.1"), new BigDecimal("2.2")), rowOne.getSet("set_type", BigDecimal.class));
    }

    @Test
    public void testDoubleList () {
        String query = "select * from something";
        ImmutableMap<String, ColumnTypes> columnTypes = ImmutableMap.of(
                "set_type", ColumnTypes.DoubleSet
        );
        ImmutableMap<String, Set<Double>> rows = ImmutableMap.of(
                "set_type", Sets.newHashSet(1.0, 2.0)
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
        assertEquals(Sets.newHashSet(1.0, 2.0), rowOne.getSet("set_type", Double.class));
    }

    @Test
    public void testFloatList () {
        String query = "select * from something";
        ImmutableMap<String, ColumnTypes> columnTypes = ImmutableMap.of(
                "list_type", ColumnTypes.FloatSet
        );
        ImmutableMap<String, Set<Float>> rows = ImmutableMap.of(
                "list_type", Sets.newHashSet(1.0f, 2.0f)
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
        assertEquals(Sets.newHashSet(1.0f, 2.0f), rowOne.getSet("list_type", Float.class));
    }

    @Test
    public void testInetList () throws UnknownHostException {
        String query = "select * from something";
        InetAddress inetAddress = InetAddress.getLocalHost();
        ImmutableMap<String, ColumnTypes> columnTypes = ImmutableMap.of(
                "list_type", ColumnTypes.InetSet
        );
        ImmutableMap<String, Set<InetAddress>> rows = ImmutableMap.of(
                "list_type", Sets.newHashSet(inetAddress)
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
        assertEquals(Sets.newHashSet(inetAddress), rowOne.getSet("list_type", InetAddress.class));
    }

    @Test
    public void testBlobList () throws UnknownHostException {
        String query = "select * from something";
        ImmutableMap<String, ColumnTypes> columnTypes = ImmutableMap.of(
                "list_type", ColumnTypes.BlobSet
        );
        ImmutableMap<String, Set<String>> rows = ImmutableMap.of(
                "list_type", Sets.newHashSet("0x0012345435345345435435")
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
        assertArrayEquals(new byte[]{0, 18, 52, 84, 53, 52, 83, 69, 67, 84, 53},
                getArray(rowOne.getSet("list_type", ByteBuffer.class).iterator().next()));
    }

    @Test
    public void testIntList () {
        String query = "select * from something";
        ImmutableMap<String, ColumnTypes> columnTypes = ImmutableMap.of(
                "list_type", ColumnTypes.IntSet
        );
        ImmutableMap<String, Set<Integer>> rows = ImmutableMap.of(
                "list_type", Sets.newHashSet(1, 2)
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
        assertEquals(Sets.newHashSet(1, 2), rowOne.getSet("list_type", Integer.class));
    }

    @Test
    public void testTimestampList () {
        String query = "select * from something";
        ImmutableMap<String, ColumnTypes> columnTypes = ImmutableMap.of(
                "list_type", ColumnTypes.TimestampSet
        );
        Date now = new Date();
        ImmutableMap<String, Set<Long>> rows = ImmutableMap.of(
                "list_type", Sets.newHashSet(now.getTime())
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
        assertEquals(Sets.newHashSet(now), rowOne.getSet("list_type", Date.class));
    }

    @Test
    public void testTimeUUIDList () {
        String query = "select * from something";
        UUID timeUUID = UUID.fromString("6a8369e0-73f0-11e4-ac06-4b05b98cc84c");
        ImmutableMap<String, ColumnTypes> columnTypes = ImmutableMap.of(
                "list_type", ColumnTypes.TimeuuidSet
        );
        ImmutableMap<String, Set<UUID>> rows = ImmutableMap.of(
                "list_type", Sets.newHashSet(timeUUID, timeUUID)
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
        assertEquals(Sets.newHashSet(timeUUID, timeUUID), rowOne.getSet("list_type", UUID.class));
    }

    @Test
    public void testUUIDList () {
        String query = "select * from something";
        UUID uuid = UUID.randomUUID();
        ImmutableMap<String, ColumnTypes> columnTypes = ImmutableMap.of(
                "list_type", ColumnTypes.UuidSet
        );
        ImmutableMap<String, Set<UUID>> rows = ImmutableMap.of(
                "list_type", Sets.newHashSet(uuid, uuid)
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
        assertEquals(Sets.newHashSet(uuid, uuid), rowOne.getSet("list_type", UUID.class));
    }

    @Test
    public void testVarIntList () {
        String query = "select * from something";
        ImmutableMap<String, ColumnTypes> columnTypes = ImmutableMap.of(
                "list_type", ColumnTypes.VarintSet
        );
        ImmutableMap<String, Set<BigInteger>> rows = ImmutableMap.of(
                "list_type", Sets.newHashSet(new BigInteger("1"), new BigInteger("2"))
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
        assertEquals(Sets.newHashSet(new BigInteger("1"), new BigInteger("2")), rowOne.getSet("list_type", BigInteger.class));
    }
}
