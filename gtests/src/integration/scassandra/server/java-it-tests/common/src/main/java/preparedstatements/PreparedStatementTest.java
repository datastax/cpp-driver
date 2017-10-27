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
package preparedstatements;

import com.google.common.collect.ImmutableMap;
import common.AbstractScassandraTest;
import common.CassandraExecutor;
import common.CassandraResult;
import common.CassandraRow;
import org.junit.Test;
import org.scassandra.http.client.ColumnTypes;
import org.scassandra.http.client.Consistency;
import org.scassandra.http.client.PreparedStatementExecution;
import org.scassandra.http.client.PrimingRequest;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.scassandra.http.client.PrimingRequest.preparedStatementBuilder;

abstract public class PreparedStatementTest extends AbstractScassandraTest {

    public PreparedStatementTest(CassandraExecutor cassandraExecutor) {
        super(cassandraExecutor);
    }

    @Test
    public void preparedStatementWithoutPriming() {
        //given
        //when
        CassandraResult results = cassandra().prepareAndExecute("select * from humans where name = ?", "Chris");

        //then
        assertEquals(0, results.rows().size());
    }

    @Test
    public void primeAndExecuteAPreparedStatement() {
        //given
        String query = "select * from people where name = ?";
        Map<String, String> row = ImmutableMap.of("name", "Chris");
        primingClient.primePreparedStatement(preparedStatementBuilder()
                .withQuery(query)
                .withRows(row)
                .build());

        //when
        CassandraResult results = cassandra().prepareAndExecute(query, "Chris");

        //then
        List<CassandraRow> asList = results.rows();
        assertEquals(1, asList.size());
        assertEquals("Chris", asList.get(0).getString("name"));
    }

    @Test
    public void retrievePreparedStatementPrimes() {
        Map<String, String> row = ImmutableMap.of("name", "Chris");
        PrimingRequest primeOne = preparedStatementBuilder()
                .withConsistency(Consistency.SERIAL, Consistency.THREE)
                .withQuery("select * from people where name = ?")
                .withColumnTypes(ImmutableMap.of("name", ColumnTypes.Text))
                .withVariableTypes(ColumnTypes.Text)
                .withRows(row)
                .build();
        primingClient.prime(primeOne);

        PrimingRequest primeTwo = preparedStatementBuilder()
                .withConsistency(Consistency.ALL, Consistency.EACH_QUORUM)
                .withQuery("some other query ?")
                .withVariableTypes(ColumnTypes.Int)
                .withColumnTypes(ImmutableMap.of("age", ColumnTypes.Int))
                .withRows(ImmutableMap.of("age", "15"))
                .build();
        primingClient.prime(primeTwo);

        List<PrimingRequest> primingRequests = primingClient.retrievePreparedPrimes();

        assertEquals(primingRequests.size(), 2);
        assertEquals(primeOne, primingRequests.get(0));
        assertEquals(primeTwo, primingRequests.get(1));
    }

    @Test
    public void activityVerificationForPreparedPrimeExecutions() {
        //given
        Map<String, String> row = ImmutableMap.of("name", "Chris");
        String preparedStatementText = "select * from people where name = ?";
        primingClient.primePreparedStatement(preparedStatementBuilder()
                .withQuery(preparedStatementText)
                .withRows(row)
                .build());

        //when
        cassandra().prepareAndExecute(preparedStatementText, "Chris");

        //then
        List<PreparedStatementExecution> preparedStatementExecutions = activityClient.retrievePreparedStatementExecutions();
        assertEquals(1, preparedStatementExecutions.size());
        PreparedStatementExecution execution = preparedStatementExecutions.get(0);
        assertEquals(preparedStatementText, execution.getPreparedStatementText());
        assertEquals("Chris", execution.getVariables().get(0));
    }

    @Test
    public void activityVerificationForPreparedStatementThatWasNotPrimed() {
        //given
        String preparedStatementText = "select * from chairs where name = ?";

        //when
        cassandra().prepareAndExecute(preparedStatementText, "Chris");

        //then
        List<PreparedStatementExecution> preparedStatementExecutions = activityClient.retrievePreparedStatementExecutions();
        assertEquals(1, preparedStatementExecutions.size());
        PreparedStatementExecution execution = preparedStatementExecutions.get(0);
        assertEquals(preparedStatementText, execution.getPreparedStatementText());
        assertEquals(1, execution.getVariables().size());
    }

    @Test
    public void prepareForSpecificConsistencyWithoutMatch() {
        //given
        Map<String, String> row = ImmutableMap.of("name", "Chris");
        primingClient.primePreparedStatement(preparedStatementBuilder()
                .withQuery("select * from people where name = ?")
                .withConsistency(Consistency.THREE)
                .withRows(row)
                .build());

        //when
        CassandraResult cassandraResult = cassandra().prepareAndExecuteWithConsistency("select * from people where name = ?", "TWO", "Chris");

        //then
        List<CassandraRow> asList = cassandraResult.rows();
        assertEquals(0, asList.size());
    }

    @Test
    public void prepareForSpecificConsistencyMatch() {
        //given
        Map<String, String> row = ImmutableMap.of("name", "Chris");
        primingClient.primePreparedStatement(preparedStatementBuilder()
                .withQuery("select * from people where name = ?")
                .withConsistency(Consistency.THREE, Consistency.QUORUM)
                .withRows(row)
                .build());

        //when
        CassandraResult cassandraResult = cassandra().prepareAndExecuteWithConsistency("select * from people where name = ?", "QUORUM", "Chris");

        //then
        List<CassandraRow> asList = cassandraResult.rows();
        assertEquals(1, asList.size());
    }
}
