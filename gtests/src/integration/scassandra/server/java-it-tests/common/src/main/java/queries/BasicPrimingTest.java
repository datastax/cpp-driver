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
package queries;/*
 * Copyright (C) 2014 Christopher Batey
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

import com.google.common.collect.ImmutableMap;
import common.AbstractScassandraTest;
import common.CassandraExecutor;
import common.CassandraRow;
import org.junit.Test;
import org.scassandra.http.client.ColumnTypes;
import org.scassandra.http.client.PrimingRequest;
import org.scassandra.http.client.Query;

import java.util.List;
import java.util.Map;

import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

abstract public class BasicPrimingTest extends AbstractScassandraTest {

    public BasicPrimingTest(CassandraExecutor cassandraExecutor) {
        super(cassandraExecutor);
    }

    @Test
    public void testUseAndSimpleQueryWithNoPrime() {
        Query expectedSelect = Query.builder().withQuery("select * from people").withConsistency("ONE").build();

        List<CassandraRow> cassandraResult = cassandra().executeQuery("select * from people").rows();

        assertEquals(0, cassandraResult.size());
        List<Query> recordedQueries = activityClient.retrieveQueries();
        assertTrue("Actual queries: " + recordedQueries, recordedQueries.contains(expectedSelect));
    }

    @Test
    public void testPrimeWithRowSingleRow() {
        String query = "select * from people";
        Map<String, String> row = ImmutableMap.of("name", "Chris");
        Map<String, ColumnTypes> columnTypes = ImmutableMap.of("name", ColumnTypes.Varchar);
        PrimingRequest prime = PrimingRequest.queryBuilder()
                .withQuery(query)
                .withRows(row)
                .withColumnTypes(columnTypes)
                .build();
        primingClient.primeQuery(prime);

        List<CassandraRow> cassandraResult = cassandra().executeQuery(query).rows();

        assertEquals(1, cassandraResult.size());
        assertEquals("Chris", cassandraResult.get(0).getString("name"));
    }

    @Test
    public void testPrimingRowsWithDifferentColumns() {
        String query = "select * from people";
        Map<String, String> rowOne = ImmutableMap.of("name", "Chris");
        Map<String, Integer> rowTwo = ImmutableMap.of("age", 15);
        Map<String, ColumnTypes> columnTypes = ImmutableMap.of(
                "name", ColumnTypes.Varchar,
                "age", ColumnTypes.Int);
        PrimingRequest prime = PrimingRequest.queryBuilder()
                .withQuery(query)
                .withRows(rowOne, rowTwo)
                .withColumnTypes(columnTypes)
                .build();
        primingClient.primeQuery(prime);

        List<CassandraRow> allRows = cassandra().executeQuery(query).rows();

        assertEquals(2, allRows.size());
        assertEquals("Chris", allRows.get(0).getString("name"));
        assertEquals(15, allRows.get(1).getInt("age"));
    }

    @Test
    public void testPrimeColumnTypesButProvideARowWithNoColumns() {
        String query = "select * from people";
        Map<String, String> row = ImmutableMap.of("abigint","12345");
        Map<String, ColumnTypes> columnTypes = ImmutableMap.of(
                "adecimal", ColumnTypes.Decimal,
                "float", ColumnTypes.Float,
                "blob", ColumnTypes.Blob,
                "boolean", ColumnTypes.Boolean,
                "abigint", ColumnTypes.Bigint);
        PrimingRequest prime = PrimingRequest.queryBuilder()
                .withQuery(query)
                .withRows(row)
                .withColumnTypes(columnTypes)
                .build();
        primingClient.primeQuery(prime);

        List<CassandraRow> allRows = cassandra().executeQuery(query).rows();

        assertEquals(1, allRows.size());
        assertNull(allRows.get(0).getDecimal("adecimal"));
        assertNull(allRows.get(0).getBytes("blob"));

        // ds driver converts null to false for bools
        assertEquals(false, allRows.get(0).getBool("boolean"));
        assertEquals(0.0f, allRows.get(0).getFloat("float"), 0.1);
        assertEquals(12345, allRows.get(0).getLong("abigint"));
    }

}
