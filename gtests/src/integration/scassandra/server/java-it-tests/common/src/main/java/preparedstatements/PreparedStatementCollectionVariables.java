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
import com.google.common.collect.Sets;
import common.AbstractScassandraTest;
import common.CassandraExecutor;
import org.junit.Test;
import org.scassandra.http.client.ColumnTypes;
import org.scassandra.http.client.PreparedStatementExecution;
import org.scassandra.http.client.PrimingRequest;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

abstract public class PreparedStatementCollectionVariables extends AbstractScassandraTest {

    public PreparedStatementCollectionVariables(CassandraExecutor cassandraExecutor) {
        super(cassandraExecutor);
    }

    @Test
    public void testSetsInVariableTypes() {
        Set<String> setValue = Sets.newHashSet("one", "two", "three");
        Map<String, ? extends Object> rows = ImmutableMap.of("set_field", "hello");
        String query = "insert into set_table (id, textset ) values (?, ?)";
        PrimingRequest primingRequest = PrimingRequest.preparedStatementBuilder()
                .withQuery(query)
                .withVariableTypes(ColumnTypes.Int, ColumnTypes.VarcharSet)
                .withRows(rows)
                .build();
        primingClient.primePreparedStatement(primingRequest);

        cassandra().prepareAndExecute(query, 1, setValue);

        List<PreparedStatementExecution> preparedStatementExecutions = activityClient.retrievePreparedStatementExecutions();
        assertEquals("Unexpected prepared statement executions " + preparedStatementExecutions, 1, preparedStatementExecutions.size());
        PreparedStatementExecution preparedStatementExecution = preparedStatementExecutions.get(0);
        List<Object> variables = preparedStatementExecution.getVariables();
        assertEquals(2, variables.size());
        assertEquals(1.0, variables.get(0));
        assertEquals(setValue, new HashSet((List) variables.get(1)));
    }

    @Test
    public void testListsInVariableTypes() {
        List<String> listValue = Arrays.asList("one", "two", "three");
        Map<String, ? extends Object> rows = ImmutableMap.of("set_field", "hello");
        String query = "insert into list_table (id, textset ) values (?, ?)";
        PrimingRequest primingRequest = PrimingRequest.preparedStatementBuilder()
                .withQuery(query)
                .withVariableTypes(ColumnTypes.Int, ColumnTypes.TextList)
                .withRows(rows)
                .build();
        primingClient.primePreparedStatement(primingRequest);

        cassandra().prepareAndExecute(query, 1, listValue);

        List<PreparedStatementExecution> preparedStatementExecutions = activityClient.retrievePreparedStatementExecutions();
        assertEquals("Unexpected prepared statement executions " + preparedStatementExecutions, 1, preparedStatementExecutions.size());
        PreparedStatementExecution preparedStatementExecution = preparedStatementExecutions.get(0);
        List<Object> variables = preparedStatementExecution.getVariables();
        assertEquals(2, variables.size());
        assertEquals(1.0, variables.get(0));
        assertEquals(listValue, variables.get(1));
    }

    @Test
    public void testMapsInVariableTypes() {
        Map<String, String> mapValue = ImmutableMap.of("one", "two");
        Map<String, ? extends Object> rows = ImmutableMap.of("map_field", "hello");
        String query = "insert into list_table (id, textmap ) values (?, ?)";
        PrimingRequest primingRequest = PrimingRequest.preparedStatementBuilder()
                .withQuery(query)
                .withVariableTypes(ColumnTypes.Int, ColumnTypes.TextTextMap)
                .withRows(rows)
                .build();
        primingClient.primePreparedStatement(primingRequest);

        cassandra().prepareAndExecute(query, 1, mapValue);

        List<PreparedStatementExecution> preparedStatementExecutions = activityClient.retrievePreparedStatementExecutions();
        assertEquals("Unexpected prepared statement executions " + preparedStatementExecutions, 1, preparedStatementExecutions.size());
        PreparedStatementExecution preparedStatementExecution = preparedStatementExecutions.get(0);
        List<Object> variables = preparedStatementExecution.getVariables();
        assertEquals(2, variables.size());
        assertEquals(1.0, variables.get(0));
        assertEquals(mapValue, variables.get(1));
    }

    @Test
    public void testEmptyMapInVariableTypes() {
        Map<String, String> mapValue = ImmutableMap.of();
        Map<String, ? extends Object> rows = ImmutableMap.of("map_field", "hello");
        String query = "insert into list_table (id, textmap ) values (?, ?)";
        PrimingRequest primingRequest = PrimingRequest.preparedStatementBuilder()
                .withQuery(query)
                .withVariableTypes(ColumnTypes.Int, ColumnTypes.TextTextMap)
                .withRows(rows)
                .build();
        primingClient.primePreparedStatement(primingRequest);

        cassandra().prepareAndExecute(query, 1, mapValue);

        List<PreparedStatementExecution> preparedStatementExecutions = activityClient.retrievePreparedStatementExecutions();
        assertEquals("Unexpected prepared statement executions " + preparedStatementExecutions, 1, preparedStatementExecutions.size());
        PreparedStatementExecution preparedStatementExecution = preparedStatementExecutions.get(0);
        List<Object> variables = preparedStatementExecution.getVariables();
        assertEquals(2, variables.size());
        assertEquals(1.0, variables.get(0));
        assertEquals(mapValue, variables.get(1));
    }
}
