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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertThat;
import static org.scassandra.matchers.Matchers.preparedStatementRecorded;

abstract public class PreparedStatementPrimitiveVariableWithMatcherTypes extends AbstractScassandraTest {

    public PreparedStatementPrimitiveVariableWithMatcherTypes(CassandraExecutor cassandraExecutor) {
        super(cassandraExecutor);
    }

    @Test
    public void testUUIDasVariableTypeAndInRow() {
        UUID uuid = UUID.randomUUID();
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

        cassandra().prepareAndExecute(query, uuid);

        PreparedStatementExecution expectedStatement = PreparedStatementExecution.builder()
                .withPreparedStatementText(query)
                .withVariables(uuid)
                .build();
        assertThat(activityClient.retrievePreparedStatementExecutions(), preparedStatementRecorded(expectedStatement));
    }

    @Test
    public void testTimeUUIDasVariableType() {
        UUID uuid = cassandra().currentTimeUUID();
        String query = "select * from blah where id = ?";
        PrimingRequest primingRequest = PrimingRequest.preparedStatementBuilder()
                .withQuery(query)
                .withVariableTypes(ColumnTypes.Timeuuid)
                .build();
        primingClient.prime(primingRequest);

        cassandra().prepareAndExecute(query, uuid);

        PreparedStatementExecution expectedStatement = PreparedStatementExecution.builder()
                .withPreparedStatementText(query)
                .withVariables(uuid)
                .build();
        assertThat(activityClient.retrievePreparedStatementExecutions(), preparedStatementRecorded(expectedStatement));
    }

    @Test
    public void testFloatWithMatcher() {
        float variable = 10.6f;
        String query = "select * from blah where id = ?";
        PrimingRequest primingRequest = PrimingRequest.preparedStatementBuilder()
                .withQuery(query)
                .withVariableTypes(ColumnTypes.Float)
                .build();
        primingClient.prime(primingRequest);

        cassandra().prepareAndExecute(query, variable);

        PreparedStatementExecution expectedStatement = PreparedStatementExecution.builder()
                .withPreparedStatementText(query)
                .withVariables("10.6")
                .build();
        assertThat(activityClient.retrievePreparedStatementExecutions(), preparedStatementRecorded(expectedStatement));
    }

    @Test
    public void testInetWithMatcher() throws Exception {
        InetAddress variable = InetAddress.getLocalHost();
        String query = "select * from blah where id = ?";
        PrimingRequest primingRequest = PrimingRequest.preparedStatementBuilder()
                .withQuery(query)
                .withVariableTypes(ColumnTypes.Inet)
                .build();
        primingClient.prime(primingRequest);

        cassandra().prepareAndExecute(query, variable);

        PreparedStatementExecution expectedStatement = PreparedStatementExecution.builder()
                .withPreparedStatementText(query)
                .withVariables(variable)
                .build();
        assertThat(activityClient.retrievePreparedStatementExecutions(), preparedStatementRecorded(expectedStatement));
    }

    @Test
    public void testBlobWithMatcher() throws Exception {
        byte[] byteArray = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        ByteBuffer variable = ByteBuffer.wrap(byteArray);
        String query = "select * from blah where id = ?";
        PrimingRequest primingRequest = PrimingRequest.preparedStatementBuilder()
                .withQuery(query)
                .withVariableTypes(ColumnTypes.Blob)
                .build();
        primingClient.prime(primingRequest);

        cassandra().prepareAndExecute(query, variable);

        PreparedStatementExecution expectedStatement = PreparedStatementExecution.builder()
                .withPreparedStatementText(query)
                .withVariables(variable)
                .build();
        assertThat(activityClient.retrievePreparedStatementExecutions(), preparedStatementRecorded(expectedStatement));
    }

    @Test
    public void testCounterWithMatcher() {
        long variable = 10;
        String query = "select * from blah where id = ?";
        PrimingRequest primingRequest = PrimingRequest.preparedStatementBuilder()
                .withQuery(query)
                .withVariableTypes(ColumnTypes.Counter)
                .build();
        primingClient.prime(primingRequest);

        cassandra().prepareAndExecute(query, variable);

        PreparedStatementExecution expectedStatement = PreparedStatementExecution.builder()
                .withPreparedStatementText(query)
                .withVariables(variable)
                .build();
        assertThat(activityClient.retrievePreparedStatementExecutions(), preparedStatementRecorded(expectedStatement));
    }

    @Test
    public void testBooleanWithMatcher() {
        boolean variable = false;
        String query = "select * from blah where id = ?";
        PrimingRequest primingRequest = PrimingRequest.preparedStatementBuilder()
                .withQuery(query)
                .withVariableTypes(ColumnTypes.Boolean)
                .build();
        primingClient.prime(primingRequest);

        cassandra().prepareAndExecute(query, variable);

        PreparedStatementExecution expectedStatement = PreparedStatementExecution.builder()
                .withPreparedStatementText(query)
                .withVariables(variable)
                .build();
        assertThat(activityClient.retrievePreparedStatementExecutions(), preparedStatementRecorded(expectedStatement));
    }

    @Test
    public void testDoubleWithMatcher() {
        double variable = 10.6;
        String query = "select * from blah where id = ?";
        PrimingRequest primingRequest = PrimingRequest.preparedStatementBuilder()
                .withQuery(query)
                .withVariableTypes(ColumnTypes.Double)
                .build();
        primingClient.prime(primingRequest);

        cassandra().prepareAndExecute(query, variable);

        PreparedStatementExecution expectedStatement = PreparedStatementExecution.builder()
                .withPreparedStatementText(query)
                .withVariables("10.6")
                .build();
        assertThat(activityClient.retrievePreparedStatementExecutions(), preparedStatementRecorded(expectedStatement));
    }

    @Test
    public void testDecimalWithMatcher() {
        BigDecimal variable = new BigDecimal("10.6");
        String query = "select * from blah where id = ?";
        PrimingRequest primingRequest = PrimingRequest.preparedStatementBuilder()
                .withQuery(query)
                .withVariableTypes(ColumnTypes.Decimal)
                .build();
        primingClient.prime(primingRequest);

        cassandra().prepareAndExecute(query, variable);

        PreparedStatementExecution expectedStatement = PreparedStatementExecution.builder()
                .withPreparedStatementText(query)
                .withVariables(variable)
                .build();
        assertThat(activityClient.retrievePreparedStatementExecutions(), preparedStatementRecorded(expectedStatement));
    }

    @Test
    public void testBigintAsInt() {
        Long variable = new Long("10");
        String query = "select * from blah where id = ?";
        PrimingRequest primingRequest = PrimingRequest.preparedStatementBuilder()
                .withQuery(query)
                .withVariableTypes(ColumnTypes.Bigint)
                .build();
        primingClient.prime(primingRequest);

        cassandra().prepareAndExecute(query, variable);

        PreparedStatementExecution expectedStatement = PreparedStatementExecution.builder()
                .withPreparedStatementText(query)
                .withVariables(10)
                .build();
        assertThat(activityClient.retrievePreparedStatementExecutions(), preparedStatementRecorded(expectedStatement));
    }

    @Test
    public void testVarint() {
        BigInteger variable = new BigInteger("10000");
        String query = "select * from blah where id = ?";
        PrimingRequest primingRequest = PrimingRequest.preparedStatementBuilder()
                .withQuery(query)
                .withVariableTypes(ColumnTypes.Varint)
                .build();
        primingClient.prime(primingRequest);

        cassandra().prepareAndExecute(query, variable);

        PreparedStatementExecution expectedStatement = PreparedStatementExecution.builder()
                .withPreparedStatementText(query)
                .withVariables(variable)
                .build();
        assertThat(activityClient.retrievePreparedStatementExecutions(), preparedStatementRecorded(expectedStatement));
    }

    @Test
    public void testBigintAsLong() {
        Long variable = new Long("10");
        String query = "select * from blah where id = ?";
        PrimingRequest primingRequest = PrimingRequest.preparedStatementBuilder()
                .withQuery(query)
                .withVariableTypes(ColumnTypes.Bigint)
                .build();
        primingClient.prime(primingRequest);

        cassandra().prepareAndExecute(query, variable);

        PreparedStatementExecution expectedStatement = PreparedStatementExecution.builder()
                .withPreparedStatementText(query)
                .withVariables(10l)
                .build();
        assertThat(activityClient.retrievePreparedStatementExecutions(), preparedStatementRecorded(expectedStatement));
    }

    @Test
    public void testNullsAsJavaNull() {
        String query = "select * from blah where id = ? and something = ?";
        PrimingRequest primingRequest = PrimingRequest.preparedStatementBuilder()
                .withQuery(query)
                .withVariableTypes(ColumnTypes.Bigint, ColumnTypes.Varchar)
                .build();
        primingClient.prime(primingRequest);

        cassandra().prepareAndExecute(query, null, null);

        PreparedStatementExecution expectedStatement = PreparedStatementExecution.builder()
                .withPreparedStatementText(query)
                .withVariables(null, null)
                .build();
        assertThat(activityClient.retrievePreparedStatementExecutions(), preparedStatementRecorded(expectedStatement));
    }
}
