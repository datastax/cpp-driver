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

import com.google.common.base.Optional;
import common.AbstractScassandraTest;
import common.CassandraExecutor;
import common.CassandraRow;
import common.WhereEquals;
import org.junit.Test;
import org.scassandra.http.client.PrimingRequest;
import org.scassandra.http.client.Query;

import java.util.List;
import java.util.UUID;

import static org.junit.Assert.*;
import static org.scassandra.cql.PrimitiveType.TEXT;
import static org.scassandra.cql.PrimitiveType.UUID;
import static org.scassandra.matchers.Matchers.containsQuery;


public class QueryBuilderTest extends AbstractScassandraTest {
    public QueryBuilderTest(CassandraExecutor cassandraExecutor) {
        super(cassandraExecutor);
    }

    @Test
    public void testBasicQueryVerification() {
        Query expectedSelect = Query.builder().withQuery("SELECT * FROM table;").withConsistency("ONE").build();

        List<CassandraRow> cassandraResult = cassandra().executeSelectWithBuilder("table").rows();

        assertEquals(0, cassandraResult.size());
        List<Query> recordedQueries = activityClient.retrieveQueries();
        assertTrue("Actual queries: " + recordedQueries, recordedQueries.contains(expectedSelect));
    }

    @Test
    public void testQueryWithTextClause() {
        String query = "SELECT * FROM table WHERE name=?;";
        Query expectedSelect = Query.builder().withQuery(query)
                .withConsistency("ONE")
                .withVariables("chris")
                .build();
        PrimingRequest prime = PrimingRequest.queryBuilder()
                .withQuery(query)
                .withVariableTypes(TEXT)
                .build();
        AbstractScassandraTest.primingClient.prime(prime);

        WhereEquals<String> clause = new WhereEquals<String>("name", "chris");
        List<CassandraRow> cassandraResult = cassandra().executeSelectWithBuilder("table", Optional.of(clause)).rows();

        assertEquals(0, cassandraResult.size());
        assertThat(activityClient.retrieveQueries(), containsQuery(expectedSelect));
    }

    @Test
    public void testQueryWithUuidClause() {
        String query = "SELECT * FROM table WHERE id=?;";
        UUID uuid = java.util.UUID.randomUUID();
        Query expectedSelect = Query.builder().withQuery(query)
                .withConsistency("ONE")
                .withVariables(uuid)
                .build();
        PrimingRequest prime = PrimingRequest.queryBuilder()
                .withQuery(query)
                .withVariableTypes(UUID)
                .build();
        AbstractScassandraTest.primingClient.prime(prime);

        WhereEquals<UUID> clause = new WhereEquals<UUID>("id", uuid);
        List<CassandraRow> cassandraResult = cassandra().executeSelectWithBuilder("table", Optional.of(clause)).rows();

        assertEquals(0, cassandraResult.size());
        assertThat(activityClient.retrieveQueries(), containsQuery(expectedSelect));
    }
}
