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

import common.AbstractScassandraTest;
import common.CassandraExecutor;
import common.CassandraResult;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.scassandra.http.client.Result.success;
import static org.scassandra.http.client.PrimingRequest.preparedStatementBuilder;
import static org.scassandra.http.client.PrimingRequest.then;

abstract public class PreparedStatementDelayTest extends AbstractScassandraTest {

    public PreparedStatementDelayTest(CassandraExecutor cassandraExecutor) {
        super(cassandraExecutor);
    }

    @Test
    public void preparedStatementDelay() {
        //given
        String query = "select * from people where name = ?";
        long primedDelay = 500;
        primingClient.prime(preparedStatementBuilder()
                .withThen(then().withFixedDelay(primedDelay).build())
                .withQuery(query)
                .build());

        //when
        long before = System.currentTimeMillis();
        CassandraResult results = cassandra().prepareAndExecute(query, "Chris");
        long duration = System.currentTimeMillis() - before;

        //then
        assertEquals(success, results.status().getResult());
        assertTrue("Expected delay of " + primedDelay + " got " + duration, duration > primedDelay && duration < (primedDelay + 200));
    }

    @Test
    public void preparedStatementPatternDelay() {
        //given
        long primedDelay = 500;
        primingClient.primePreparedStatement(preparedStatementBuilder()
                .withFixedDelay(primedDelay)
                .withQueryPattern(".*")
                .build());

        //when
        long before = System.currentTimeMillis();
        CassandraResult results = cassandra().prepareAndExecute("select * from people where name = ?", "Chris");
        long duration = System.currentTimeMillis() - before;

        //then
        assertEquals(success, results.status().getResult());
        assertTrue("Expected delay of " + primedDelay + " got " + duration, duration > primedDelay && duration < (primedDelay + 200));
    }
}
