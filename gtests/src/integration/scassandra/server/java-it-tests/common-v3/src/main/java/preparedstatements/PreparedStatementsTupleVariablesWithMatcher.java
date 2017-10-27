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

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TupleValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import common.AbstractScassandraTest;
import common.CassandraExecutorV3;
import org.hamcrest.MatcherAssert;
import org.junit.Test;
import org.scassandra.cql.PrimitiveType;
import org.scassandra.http.client.PreparedStatementExecution;
import org.scassandra.http.client.PrimingRequest;
import org.scassandra.matchers.Matchers;

import java.util.Map;

import static org.scassandra.cql.TupleType.tuple;

public abstract class PreparedStatementsTupleVariablesWithMatcher extends AbstractScassandraTest<CassandraExecutorV3> {

    public PreparedStatementsTupleVariablesWithMatcher(CassandraExecutorV3 cassandraExecutor) {
        super(cassandraExecutor);
    }

    @Test
    public void textIntTuples() {
        TupleType tupleType = cassandra().tupleType(DataType.text(), DataType.cint());
        TupleValue tuple = tupleType.newValue("one", 1);
        Map<String, ? extends Object> rows = ImmutableMap.of("tuple_field", "hello");
        String query = "insert into set_table (id, tuple_field) values (?, ?)";
        PrimingRequest primingRequest = PrimingRequest.preparedStatementBuilder()
                .withQuery(query)
                .withThen(PrimingRequest.then()
                        .withVariableTypes(PrimitiveType.INT, tuple(PrimitiveType.TEXT, PrimitiveType.INT))
                        .withRows(rows))
                .build();
        AbstractScassandraTest.primingClient.prime(primingRequest);

        cassandra().prepareAndExecute(query, 1, tuple);

        PreparedStatementExecution expectedExecution = PreparedStatementExecution.builder()
                .withConsistency("ONE")
                .withPreparedStatementText(query)
                .withVariables(1, Lists.newArrayList("one", 1))
                .build();
        MatcherAssert.assertThat(AbstractScassandraTest.activityClient.retrievePreparedStatementExecutions(), Matchers.preparedStatementRecorded(expectedExecution));
    }

    @Test
    public void nestedTuples() {
        TupleType innerType = cassandra().tupleType(DataType.ascii(), DataType.cfloat());
        TupleType tupleType = cassandra().tupleType(DataType.cint(), innerType);
        TupleValue tuple = tupleType.newValue(5, innerType.newValue("hello", 3.14f));
        Map<String, ? extends Object> rows = ImmutableMap.of("tuple_field", "hello");
        String query = "insert into set_table (id, tuple_field) values (?, ?)";
        PrimingRequest primingRequest = PrimingRequest.preparedStatementBuilder()
                .withQuery(query)
                .withThen(PrimingRequest.then()
                        .withVariableTypes(PrimitiveType.INT, tuple(PrimitiveType.INT, tuple(PrimitiveType.ASCII, PrimitiveType.FLOAT)))
                        .withRows(rows))
                .build();
        AbstractScassandraTest.primingClient.prime(primingRequest);

        cassandra().prepareAndExecute(query, 1, tuple);

        PreparedStatementExecution expectedExecution = PreparedStatementExecution.builder()
                .withConsistency("ONE")
                .withPreparedStatementText(query)
                .withVariables(1, Lists.newArrayList(5, Lists.newArrayList("hello", "3.14")))
                .build();
        MatcherAssert.assertThat(AbstractScassandraTest.activityClient.retrievePreparedStatementExecutions(), Matchers.preparedStatementRecorded(expectedExecution));
    }
}
