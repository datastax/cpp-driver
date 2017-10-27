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

import cassandra.CassandraExecutor30;
import com.datastax.driver.core.LocalDate;
import org.junit.Test;
import org.scassandra.cql.PrimitiveType;
import org.scassandra.http.client.PreparedStatementExecution;
import org.scassandra.http.client.PrimingRequest;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class PreparedStatementPrimitiveVariableTypes30 extends PreparedStatementPrimitiveVariableTypes {

    public PreparedStatementPrimitiveVariableTypes30() {
        super(new CassandraExecutor30(scassandra.getBinaryPort()));
    }

    @Test
    public void testSmallint() {
        short variable = 512;
        String query = "select * from blah where id = ?";
        PrimingRequest primingRequest = PrimingRequest.preparedStatementBuilder()
                .withQuery(query)
                .withVariableTypes(PrimitiveType.SMALL_INT)
                .build();
        primingClient.prime(primingRequest);

        cassandra().prepareAndExecute(query, variable);

        List<PreparedStatementExecution> preparedStatementExecutions = activityClient.retrievePreparedStatementExecutions();
        assertEquals(1, preparedStatementExecutions.size());
        PreparedStatementExecution preparedStatementExecution = preparedStatementExecutions.get(0);
        assertEquals(512.0, preparedStatementExecution.getVariables().get(0));
    }

    @Test
    public void testTinyint() {
        byte variable = 127;
        String query = "select * from blah where id = ?";
        PrimingRequest primingRequest = PrimingRequest.preparedStatementBuilder()
                .withQuery(query)
                .withVariableTypes(PrimitiveType.TINY_INT)
                .build();
        primingClient.prime(primingRequest);

        cassandra().prepareAndExecute(query, variable);

        List<PreparedStatementExecution> preparedStatementExecutions = activityClient.retrievePreparedStatementExecutions();
        assertEquals(1, preparedStatementExecutions.size());
        PreparedStatementExecution preparedStatementExecution = preparedStatementExecutions.get(0);
        assertEquals(127.0, preparedStatementExecution.getVariables().get(0));
    }

    @Test
    public void testDate() {
        LocalDate variable = LocalDate.fromYearMonthDay(2012, 8, 4);
        String query = "select * from blah where id = ?";
        PrimingRequest primingRequest = PrimingRequest.preparedStatementBuilder()
                .withQuery(query)
                .withVariableTypes(PrimitiveType.DATE)
                .build();
        primingClient.prime(primingRequest);

        cassandra().prepareAndExecute(query, variable);

        List<PreparedStatementExecution> preparedStatementExecutions = activityClient.retrievePreparedStatementExecutions();
        assertEquals(1, preparedStatementExecutions.size());
        PreparedStatementExecution preparedStatementExecution = preparedStatementExecutions.get(0);
        // Date is epoch at 2^31 + days since epoch.
        assertEquals(2147499204.0, preparedStatementExecution.getVariables().get(0));
    }

    @Test
    public void testTime() {
        long variable = 83458345843858438L;
        String query = "select * from blah where id = ?";
        PrimingRequest primingRequest = PrimingRequest.preparedStatementBuilder()
                .withQuery(query)
                .withVariableTypes(PrimitiveType.TIME)
                .build();
        primingClient.prime(primingRequest);

        cassandra().prepareAndExecute(query, variable);

        List<PreparedStatementExecution> preparedStatementExecutions = activityClient.retrievePreparedStatementExecutions();
        assertEquals(1, preparedStatementExecutions.size());
        PreparedStatementExecution preparedStatementExecution = preparedStatementExecutions.get(0);
        assertEquals(83458345843858438.0, preparedStatementExecution.getVariables().get(0));
    }
}
