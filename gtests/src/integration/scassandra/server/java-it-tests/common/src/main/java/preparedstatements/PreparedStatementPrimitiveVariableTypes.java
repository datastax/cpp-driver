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
import org.junit.Test;
import org.scassandra.http.client.ColumnTypes;
import org.scassandra.http.client.PreparedStatementExecution;
import org.scassandra.http.client.PrimingRequest;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

abstract public class PreparedStatementPrimitiveVariableTypes extends AbstractScassandraTest {

    public PreparedStatementPrimitiveVariableTypes(CassandraExecutor cassandraExecutor) {
        super(cassandraExecutor);
    }

    @Test
    public void testUUIDasVariableTypeAndInRow() {
        UUID variable = UUID.randomUUID();
        UUID rowValue = UUID.randomUUID();
        Map<String, String> rows = ImmutableMap.of("field", rowValue.toString());
        String query = "select * from blah where id = ?";
        PrimingRequest primingRequest = PrimingRequest.preparedStatementBuilder()
                .withQuery(query)
                .withVariableTypes(ColumnTypes.Uuid)
                .withColumnTypes(ImmutableMap.of("field", ColumnTypes.Uuid))
                .withRows(rows)
                .build();
        primingClient.prime(primingRequest);

        cassandra().prepareAndExecute(query, variable);

        List<PreparedStatementExecution> preparedStatementExecutions = activityClient.retrievePreparedStatementExecutions();
        assertEquals("Unexpected prepared statement executions " + preparedStatementExecutions, 1, preparedStatementExecutions.size());
        PreparedStatementExecution preparedStatementExecution = preparedStatementExecutions.get(0);
        assertEquals(asList(variable.toString()), preparedStatementExecution.getVariables());
    }

    @Test
    public void testFloat() {
        float variable = 10.6f;
        String query = "select * from blah where id = ?";
        PrimingRequest primingRequest = PrimingRequest.preparedStatementBuilder()
                .withQuery(query)
                .withVariableTypes(ColumnTypes.Float)
                .build();
        primingClient.prime(primingRequest);

        cassandra().prepareAndExecute(query, variable);

        List<PreparedStatementExecution> preparedStatementExecutions = activityClient.retrievePreparedStatementExecutions();
        assertEquals(1, preparedStatementExecutions.size());
        PreparedStatementExecution preparedStatementExecution = preparedStatementExecutions.get(0);
        assertEquals("10.6", preparedStatementExecution.getVariables().get(0));
    }

    @Test
    public void testDouble() {
        double variable = 10.6;
        String query = "select * from blah where id = ?";
        PrimingRequest primingRequest = PrimingRequest.preparedStatementBuilder()
                .withQuery(query)
                .withVariableTypes(ColumnTypes.Double)
                .build();
        primingClient.prime(primingRequest);

        cassandra().prepareAndExecute(query, variable);

        List<PreparedStatementExecution> preparedStatementExecutions = activityClient.retrievePreparedStatementExecutions();
        assertEquals(1, preparedStatementExecutions.size());
        PreparedStatementExecution preparedStatementExecution = preparedStatementExecutions.get(0);
        assertEquals("10.6", preparedStatementExecution.getVariables().get(0));
    }

    @Test
    public void testBigint() {
        Long variable = new Long("10");
        String query = "select * from blah where id = ?";
        PrimingRequest primingRequest = PrimingRequest.preparedStatementBuilder()
                .withQuery(query)
                .withVariableTypes(ColumnTypes.Bigint)
                .build();
        primingClient.prime(primingRequest);

        cassandra().prepareAndExecute(query, variable);

        List<PreparedStatementExecution> preparedStatementExecutions = activityClient.retrievePreparedStatementExecutions();
        assertEquals(1, preparedStatementExecutions.size());
        PreparedStatementExecution preparedStatementExecution = preparedStatementExecutions.get(0);
        assertEquals(10.0, preparedStatementExecution.getVariables().get(0));
    }

    @Test
    public void testNulls() {
        String query = "select * from blah where id = ? and something = ?";
        PrimingRequest primingRequest = PrimingRequest.preparedStatementBuilder()
                .withQuery(query)
                .withVariableTypes(ColumnTypes.Bigint, ColumnTypes.Varchar)
                .build();
        primingClient.prime(primingRequest);

        cassandra().prepareAndExecute(query, null, null);

        List<PreparedStatementExecution> preparedStatementExecutions = activityClient.retrievePreparedStatementExecutions();
        assertEquals(1, preparedStatementExecutions.size());
        PreparedStatementExecution preparedStatementExecution = preparedStatementExecutions.get(0);
        assertEquals(null, preparedStatementExecution.getVariables().get(0));
        assertEquals(null, preparedStatementExecution.getVariables().get(1));
    }

    @Test
    public void testBlobs() {
        byte[] byteArray = new byte[] {1,2,3,4,5};
        ByteBuffer byteBuffer = ByteBuffer.wrap(byteArray);
        String query = "select * from blah where id = ?";
        PrimingRequest primingRequest = PrimingRequest.preparedStatementBuilder()
                .withQuery(query)
                .withVariableTypes(ColumnTypes.Blob)
                .build();
        primingClient.prime(primingRequest);

        cassandra().prepareAndExecute(query, byteBuffer);

        List<PreparedStatementExecution> preparedStatementExecutions = activityClient.retrievePreparedStatementExecutions();
        assertEquals(1, preparedStatementExecutions.size());
        PreparedStatementExecution preparedStatementExecution = preparedStatementExecutions.get(0);
        assertEquals("0x0102030405", preparedStatementExecution.getVariables().get(0));
    }
}
