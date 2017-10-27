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
package batches;

import com.google.common.collect.Lists;
import common.AbstractScassandraTest;
import common.CassandraExecutor;
import common.CassandraQuery;
import org.junit.Test;
import org.scassandra.http.client.BatchExecution;
import org.scassandra.http.client.BatchQuery;
import org.scassandra.http.client.BatchQueryKind;
import org.scassandra.http.client.BatchType;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.scassandra.cql.PrimitiveType.VARCHAR;

abstract public class BatchActivityVerificationTest extends AbstractScassandraTest {
    public BatchActivityVerificationTest(CassandraExecutor cassandraExecutor) {
        super(cassandraExecutor);
    }

    @Test
    public void executeUnLoggedBatch() {
        cassandra().executeBatch(Lists.newArrayList(
                        new CassandraQuery("select * from blah"),
                        new CassandraQuery("select * from blah2")
                ), BatchType.UNLOGGED
        );

        List<BatchExecution> batches = activityClient.retrieveBatches();

        assertEquals(1, batches.size());
        assertEquals(BatchExecution.builder().withBatchQueries(
                BatchQuery.builder().withQuery("select * from blah").build(),
                BatchQuery.builder().withQuery("select * from blah2").build())
                .withConsistency( "ONE")
                .withBatchType(BatchType.UNLOGGED).build(), batches.get(0));
    }

    @Test
    public void executeLoggedBatch() {
        cassandra().executeBatch(Lists.newArrayList(
                        new CassandraQuery("select * from blah"),
                        new CassandraQuery("select * from blah2")
                ), BatchType.LOGGED
        );

        List<BatchExecution> batches = activityClient.retrieveBatches();

        assertEquals(1, batches.size());
        assertEquals(BatchExecution.builder().withBatchQueries(
                BatchQuery.builder().withQuery("select * from blah").build(),
                BatchQuery.builder().withQuery("select * from blah2").build())
                .withConsistency("ONE").withBatchType(BatchType.LOGGED).build(), batches.get(0));
    }

    @Test
    public void batchWithQueryParametersUnPrimed() {
        cassandra().executeBatch(Lists.newArrayList(
                        new CassandraQuery("select * from blah where blah = ? and wah = ?", "Hello", 1)
                ), BatchType.LOGGED
        );

        List<BatchExecution> batches = activityClient.retrieveBatches();

        assertEquals(1, batches.size());
        assertEquals(BatchExecution.builder().withBatchQueries(
                BatchQuery.builder().withQuery("select * from blah where blah = ? and wah = ?").build())
                .withConsistency("ONE").withBatchType(BatchType.LOGGED).build(), batches.get(0));
    }

    @Test
    public void clearAllRecordedActivity() {
        cassandra().executeBatch(Lists.newArrayList(
                        new CassandraQuery("select * from blah"),
                        new CassandraQuery("select * from blah2")
                ), BatchType.UNLOGGED
        );

        activityClient.clearAllRecordedActivity();
        List<BatchExecution> batches = activityClient.retrieveBatches();

        assertEquals(0, batches.size());
    }

    @Test
    public void clearJustBatchExecution() {
        cassandra().executeBatch(Lists.newArrayList(
                        new CassandraQuery("select * from blah"),
                        new CassandraQuery("select * from blah2")
                ), BatchType.UNLOGGED
        );

        activityClient.clearBatchExecutions();
        List<BatchExecution> batches = activityClient.retrieveBatches();

        assertEquals(0, batches.size());
    }

    @Test
    public void preparedStatementsInBatches() {
        cassandra().executeBatch(Lists.newArrayList(
                        new CassandraQuery("query", "Hello"),
                        new CassandraQuery("prepared statement ? ?",
                                CassandraQuery.QueryType.PREPARED_STATEMENT, "one", "twp")
                ), BatchType.LOGGED
        );

        List<BatchExecution> batches = activityClient.retrieveBatches();

        assertEquals(1, batches.size());
        assertEquals(BatchExecution.builder().withBatchQueries(
                BatchQuery.builder().withQuery("query"),
                BatchQuery.builder().withQuery("prepared statement ? ?").withVariables("one", "twp")
                        .withVariableTypes(VARCHAR, VARCHAR)
                        .withType(BatchQueryKind.prepared_statement))
                .withConsistency("ONE").withBatchType(BatchType.LOGGED).build(), batches.get(0));
    }
}
