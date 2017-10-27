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

import cassandra.CassandraExecutor30;
import com.google.common.collect.ImmutableMap;
import common.CassandraRow;
import org.junit.Test;
import org.scassandra.cql.CqlType;
import org.scassandra.cql.PrimitiveType;
import org.scassandra.http.client.PrimingRequest;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.scassandra.http.client.Consistency.THREE;
import static org.scassandra.http.client.Consistency.TWO;
import static org.scassandra.http.client.PrimingRequest.then;
import static org.scassandra.http.client.types.ColumnMetadata.column;

public class PrimingPrimitiveTypesForQuery30 extends PrimingPrimitiveTypesForQuery {

    public PrimingPrimitiveTypesForQuery30() {
        super(new CassandraExecutor30(scassandra.getBinaryPort()));
    }

    @Test
    public void testNewTypes() {
        String query = "select * from something";
        PrimingRequest prime = PrimingRequest.queryBuilder()
                .withQuery(query)
                .withConsistency(TWO, THREE)
                .withThen(then()
                        .withColumnTypes(
                                column("smallint_type", PrimitiveType.SMALL_INT),
                                column("tinyint_type", PrimitiveType.TINY_INT),
                                column("time_type", PrimitiveType.TIME),
                                column("date_type", PrimitiveType.DATE))
                        .withRows(
                                ImmutableMap.of("smallint_type", 512, "tinyint_type", 5,   "time_type", 8675, "date_type", 2147484648L),
                                ImmutableMap.of("smallint_type", 768, "tinyint_type", 127, "time_type", 3090, "date_type", 2147484678L)
                        ))
                .build();

        primingClient.prime(prime);

        List<CassandraRow> result = cassandra().executeSimpleStatement(query, "THREE").rows();

        long center = (long)Math.pow(2, 31);

        assertEquals(result.size(), 2);
        CassandraRow rowOne = result.get(0);
        assertEquals(512, (int)rowOne.getShort("smallint_type"));
        assertEquals(5, (int)rowOne.getByte("tinyint_type"));
        assertEquals(8675L, rowOne.getTime("time_type"));
        assertEquals(java.time.LocalDate.ofEpochDay(2147484648L-center), rowOne.getDate("date_type"));
        CassandraRow rowTwo = result.get(1);
        assertEquals(768, (int)rowTwo.getShort("smallint_type"));
        assertEquals(127, (int)rowTwo.getByte("tinyint_type"));
        assertEquals(3090L, rowTwo.getTime("time_type"));
        assertEquals(java.time.LocalDate.ofEpochDay(2147484678L-center), rowTwo.getDate("date_type"));

        // Retrieve primes check
        List<PrimingRequest> primingRequests = primingClient.retrieveQueryPrimes();
        assertEquals(Arrays.asList(TWO, THREE), primingRequests.get(0).getWhen().getConsistency());

        Map<String, CqlType> newTypes = prime.getThen().getColumnTypes();
        assertEquals(newTypes, primingRequests.get(0).getThen().getColumnTypes());
    }
}
