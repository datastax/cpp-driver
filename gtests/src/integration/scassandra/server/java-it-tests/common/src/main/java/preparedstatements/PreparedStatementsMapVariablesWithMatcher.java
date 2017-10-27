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
import com.google.common.collect.Lists;
import common.AbstractScassandraTest;
import common.CassandraExecutor;
import org.junit.Ignore;
import org.junit.Test;
import org.scassandra.cql.MapType;
import org.scassandra.http.client.ColumnTypes;
import org.scassandra.http.client.PreparedStatementExecution;
import org.scassandra.http.client.PrimingRequest;
import org.scassandra.http.client.types.ColumnMetadata;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNot.not;
import static org.scassandra.cql.MapType.map;
import static org.scassandra.cql.PrimitiveType.BIG_INT;
import static org.scassandra.cql.PrimitiveType.INT;
import static org.scassandra.cql.PrimitiveType.VAR_INT;
import static org.scassandra.matchers.Matchers.preparedStatementRecorded;

abstract public class PreparedStatementsMapVariablesWithMatcher extends AbstractScassandraTest {

    public PreparedStatementsMapVariablesWithMatcher(CassandraExecutor cassandraExecutor) {
        super(cassandraExecutor);
    }

    @Test
    public void textMaps() {
        Map<String, String> mapValue = ImmutableMap.of("one", "1", "two", "2");
        String query = "insert into set_table (id, textlist ) values (?, ?)";
        PrimingRequest primingRequest = PrimingRequest.preparedStatementBuilder()
                .withQuery(query)
                .withVariableTypes(ColumnTypes.Int, ColumnTypes.TextTextMap)
                .build();
        primingClient.prime(primingRequest);

        cassandra().prepareAndExecute(query, 1, mapValue);

        PreparedStatementExecution expectedExecution = PreparedStatementExecution.builder()
                .withConsistency("ONE")
                .withPreparedStatementText(query)
                .withVariables(1, mapValue)
                .build();
        assertThat(activityClient.retrievePreparedStatementExecutions(), preparedStatementRecorded(expectedExecution));
    }
    
    @Test
    public void textMapsThatDontMatch() {
        Map<String, String> mapValue = ImmutableMap.of("one", "1", "two", "2");
        String query = "insert into set_table (id, textlist ) values (?, ?)";
        PrimingRequest primingRequest = PrimingRequest.preparedStatementBuilder()
                .withQuery(query)
                .withVariableTypes(ColumnTypes.Int, ColumnTypes.TextTextMap)
                .build();
        primingClient.prime(primingRequest);

        cassandra().prepareAndExecute(query, 1, mapValue);

        PreparedStatementExecution expectedExecution = PreparedStatementExecution.builder()
                .withConsistency("ONE")
                .withPreparedStatementText(query)
                .withVariables(1, ImmutableMap.of("another", "map"))
                .build();
        assertThat(activityClient.retrievePreparedStatementExecutions(), not(preparedStatementRecorded(expectedExecution)));
    }

    @Test
    public void numberMapsInVariables() {
        String query = "insert into table (one, two, three ) values (?, ?, ?)";
        Map<Long, Long> bigIntMap = ImmutableMap.of(1l, 2l);
        Map<Integer, Integer> intMap = ImmutableMap.of(3, 4);
        Map<BigInteger, BigInteger> varIntMap = ImmutableMap.of(new BigInteger("5"), new BigInteger("6"));
        PrimingRequest primingRequest = PrimingRequest.preparedStatementBuilder()
                .withQuery(query)
                .withVariableTypes(
                    map(BIG_INT, BIG_INT),
                    map(INT, INT),
                    map(VAR_INT, VAR_INT)
                )
                .build();
        primingClient.prime(primingRequest);

        cassandra().prepareAndExecute(query, bigIntMap, intMap, varIntMap);

        PreparedStatementExecution expectedExecution = PreparedStatementExecution.builder()
                .withConsistency("ONE")
                .withPreparedStatementText(query)
                .withVariables(bigIntMap, intMap, varIntMap)
                .build();
        assertThat(activityClient.retrievePreparedStatementExecutions(), preparedStatementRecorded(expectedExecution));
    }
}
