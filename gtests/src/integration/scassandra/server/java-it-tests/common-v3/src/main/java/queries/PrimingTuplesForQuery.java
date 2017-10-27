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

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TupleValue;
import com.google.common.collect.ImmutableMap;
import common.AbstractScassandraTest;
import common.CassandraExecutorV3;
import common.CassandraRowV3;
import org.junit.Test;
import org.scassandra.cql.PrimitiveType;
import org.scassandra.http.client.PrimingRequest;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.scassandra.cql.TupleType.tuple;
import static org.scassandra.http.client.PrimingRequest.then;
import static org.scassandra.http.client.types.ColumnMetadata.column;

public class PrimingTuplesForQuery extends AbstractScassandraTest<CassandraExecutorV3> {
    public PrimingTuplesForQuery(CassandraExecutorV3 cassandraExecutor) {
        super(cassandraExecutor);
    }

    @Test
    public void testInetVarcharTuple() throws UnknownHostException {
        String query = "select * from something";
        TupleType tupleType = cassandra().tupleType(DataType.inet(), DataType.varchar());
        InetAddress inetAddress = InetAddress.getLocalHost();
        TupleValue tuple = tupleType.newValue(inetAddress, "hello");
        ImmutableMap<String, List<Object>> rows = ImmutableMap.of(
                "tuple_type", Arrays.asList(inetAddress, "hello")
        );
        PrimingRequest prime = PrimingRequest.queryBuilder()
                .withQuery(query)
                .withThen(then()
                        .withColumnTypes(column("tuple_type", tuple(PrimitiveType.INET, PrimitiveType.VARCHAR)))
                        .withRows(rows)
                ).build();
        AbstractScassandraTest.primingClient.prime(prime);

        List<CassandraRowV3> result = cassandra().executeQuery(query).rows();

        assertEquals(result.size(), 1);
        CassandraRowV3 rowOne = result.get(0);
        assertEquals(tuple, rowOne.getTupleValue("tuple_type"));
    }

    @Test
    public void testNestedTuple() throws UnknownHostException {
        String query = "select * from something";
        TupleType innerType = cassandra().tupleType(DataType.inet(), DataType.varchar());
        TupleType tupleType = cassandra().tupleType(innerType, DataType.cint());
        InetAddress inetAddress = InetAddress.getLocalHost();
        TupleValue tuple = tupleType.newValue(innerType.newValue(inetAddress, "hello"), 8855);
        ImmutableMap<String, List<Object>> rows = ImmutableMap.of(
                "tuple_type", Arrays.asList(Arrays.asList(inetAddress, "hello"), 8855)
        );
        PrimingRequest prime = PrimingRequest.queryBuilder()
                .withQuery(query)
                .withThen(then()
                        .withColumnTypes(column("tuple_type", tuple(tuple(PrimitiveType.INET, PrimitiveType.VARCHAR), PrimitiveType.INT)))
                        .withRows(rows)
                ).build();
        AbstractScassandraTest.primingClient.prime(prime);

        List<CassandraRowV3> result = cassandra().executeQuery(query).rows();

        assertEquals(result.size(), 1);
        CassandraRowV3 rowOne = result.get(0);
        assertEquals(tuple, rowOne.getTupleValue("tuple_type"));
    }
}
