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
import org.scassandra.matchers.Matchers;

import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.scassandra.matchers.Matchers.preparedStatementRecorded;

abstract public class PreparedStatementsSetVariablesWithMatcher extends AbstractScassandraTest {

    public PreparedStatementsSetVariablesWithMatcher(CassandraExecutor cassandraExecutor) {
        super(cassandraExecutor);
    }

    @Test
    public void textSets() {
        Set<String> setValue = Sets.newHashSet("one", "two", "three");
        Map<String, ? extends Object> rows = ImmutableMap.of("set_field", "hello");
        String query = "insert into set_table (id, textset ) values (?, ?)";
        PrimingRequest primingRequest = PrimingRequest.preparedStatementBuilder()
                .withQuery(query)
                .withVariableTypes(ColumnTypes.Int, ColumnTypes.VarcharSet)
                .withRows(rows)
                .build();
        primingClient.prime(primingRequest);

        cassandra().prepareAndExecute(query, 1, setValue);

        PreparedStatementExecution expectedExecution = PreparedStatementExecution.builder()
                .withConsistency("ONE")
                .withPreparedStatementText(query)
                .withVariables(1, setValue)
                .build();
        assertThat(activityClient.retrievePreparedStatementExecutions(), preparedStatementRecorded(expectedExecution));
    }
}
