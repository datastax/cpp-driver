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
package queries;

import common.AbstractScassandraTest;
import common.CassandraExecutor;
import org.junit.Test;
import org.scassandra.http.client.PrimingRequest;

import static org.junit.Assert.assertTrue;

abstract public class DelayTest extends AbstractScassandraTest {
    public DelayTest(CassandraExecutor cassandraExecutor) {
        super(cassandraExecutor);
    }

    @Test
    public void testDelayingQuery() {
        //given
        String query = "select * from table";
        long primedDelay = 500;
        PrimingRequest primingRequest = PrimingRequest.queryBuilder()
                .withFixedDelay(primedDelay)
                .withQuery(query)
                .build();
        primingClient.prime(primingRequest);

        //when
        long before = System.currentTimeMillis();
        cassandra().executeQuery(query);
        long duration = System.currentTimeMillis() - before;

        //then
        System.out.println(duration);
        assertTrue("Expected delay of " + primedDelay + " got " + duration, duration > primedDelay && duration < (primedDelay + 200));
    }

    @Test
    public void testDelayingQueryPattern() {
        //given
        String query = "select * from table";
        long primedDelay = 500;
        PrimingRequest primingRequest = PrimingRequest.queryBuilder()
                .withFixedDelay(primedDelay)
                .withQueryPattern("select .*")
                .build();
        primingClient.primeQuery(primingRequest);

        //when
        long before = System.currentTimeMillis();
        cassandra().executeQuery(query);
        long duration = System.currentTimeMillis() - before;

        //then
        System.out.println(duration);
        assertTrue("Expected delay of " + primedDelay + " got " + duration, duration > primedDelay && duration < (primedDelay + 200));
    }
}
