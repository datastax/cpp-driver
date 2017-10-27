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

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import common.AbstractScassandraTest;
import common.CassandraExecutor;
import common.CassandraResult;
import common.CassandraRow;
import org.junit.Test;
import org.scassandra.cql.CqlType;
import org.scassandra.cql.PrimitiveType;
import org.scassandra.http.client.Consistency;
import org.scassandra.http.client.MultiPrimeRequest;
import org.scassandra.http.client.Result;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.scassandra.cql.PrimitiveType.*;
import static org.scassandra.http.client.MultiPrimeRequest.*;

abstract public class PreparedStatementPrimeOnVariables extends AbstractScassandraTest {

    public PreparedStatementPrimeOnVariables(CassandraExecutor cassandraExecutor) {
        super(cassandraExecutor);
    }

    @Test
    public void testPrimeBasedMatchOnConsistency() {
        String query = "select * from person where name = ?";
        MultiPrimeRequest prime = MultiPrimeRequest.request()
                .withWhen(when()
                        .withQuery(query).withConsistency(Consistency.TWO))
                .withThen(then()
                        .withVariableTypes(TEXT)
                        .withOutcomes(
                                outcome(match().withVariableMatchers(exactMatch().withMatcher("Andrew").build()), action().withResult(Result.read_request_timeout))
                        )
                )
                .build();

        primingClient.multiPrime(prime);

        CassandraResult result = cassandra().prepareAndExecute(query, "Andrew");

        // shouldn't match due to consistency
        assertEquals(Result.success, result.status().getResult());
    }

    @Test
    public void testPrimeReturningRows() {
        String query = "select * from person where name = ?";
        Map<String, Object> rows = ImmutableMap.of(
          "name", "Chris",
          "age", 30
        );
        Map<String, CqlType> colTypes = ImmutableMap.of("age", PrimitiveType.INT, "name", PrimitiveType.TEXT);
        MultiPrimeRequest prime = MultiPrimeRequest.request()
                .withWhen(when()
                        .withQuery(query))
                .withThen(then()
                        .withVariableTypes(TEXT)
                        .withOutcomes(
                                outcome(match().withVariableMatchers(exactMatch().withMatcher("Andrew").build()),
                                        action().withRows(rows).withColumnTypes(colTypes))
                        )
                )
                .build();

        primingClient.multiPrime(prime);

        CassandraResult result = cassandra().prepareAndExecute(query, "Andrew");

        assertEquals(Result.success, result.status().getResult());
        List<CassandraRow> returnedRows = result.rows();
        assertEquals("Expected one row", 1, returnedRows.size());
        assertEquals("Chris", returnedRows.get(0).getString("name"));
        assertEquals(30, returnedRows.get(0).getInt("age"));
    }

    @Test
    public void testDelays() {
        String query = "select * from person where name = ?";
        Map<String, Object> rows = ImmutableMap.of(
                "name", "Chris",
                "age", 30
        );
        Map<String, CqlType> colTypes = ImmutableMap.of("age", PrimitiveType.INT, "name", PrimitiveType.TEXT);
        MultiPrimeRequest prime = MultiPrimeRequest.request()
                .withWhen(when()
                        .withQuery(query))
                .withThen(then()
                        .withVariableTypes(TEXT)
                        .withOutcomes(
                                outcome(match().withVariableMatchers(exactMatch().withMatcher("Andrew").build()),
                                        action().withRows(rows).withColumnTypes(colTypes).withFixedDelay(500L))
                        )
                )
                .build();

        primingClient.multiPrime(prime);

        Stopwatch stopwatch = Stopwatch.createStarted();
        CassandraResult result = cassandra().prepareAndExecute(query, "Andrew");
        long elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);

        assertEquals(Result.success, result.status().getResult());
        assertTrue("Expected delay to be greater than 500 milliseconds", elapsed > 500);
    }


    @Test
    public void testPartialMatchWithAnyMatcher() {
        String query = "select * from person where name = ? and age = ?";
        Map<String, Object> rows = ImmutableMap.of(
                "name", "Chris",
                "age", 30
        );
        Map<String, CqlType> colTypes = ImmutableMap.of("age", PrimitiveType.INT, "name", PrimitiveType.TEXT);
        MultiPrimeRequest prime = MultiPrimeRequest.request()
                .withWhen(when()
                        .withQuery(query))
                .withThen(then()
                        .withVariableTypes(TEXT, INT)
                        .withOutcomes(
                                outcome(match().withVariableMatchers(exactMatch("Andrew"), anyMatch()),
                                        action().withRows(rows).withColumnTypes(colTypes)),
                                outcome(match().withVariableMatchers(anyMatch(), anyMatch()),
                                        action().withResult(Result.read_request_timeout).withColumnTypes(colTypes))
                        )
                )
                .build();

        primingClient.multiPrime(prime);

        CassandraResult resultFirstMatch = cassandra().prepareAndExecute(query, "Andrew", 15);
        CassandraResult resultAnyMatch = cassandra().prepareAndExecute(query, "Not Andrew", 20);

        assertEquals(Result.success, resultFirstMatch.status().getResult());
        assertEquals(Result.read_request_timeout, resultAnyMatch.status().getResult());
    }

    @Test
    public void testPrimeBasedOnMatchText() {
        String query = "select * from person where name = ?";
        MultiPrimeRequest prime = MultiPrimeRequest.request()
                .withWhen(when()
                        .withQuery(query))
                .withThen(then()
                        .withVariableTypes(TEXT)
                        .withOutcomes(
                                outcome(match().withVariableMatchers(exactMatch().withMatcher("Chris").build()), action().withResult(Result.success)),
                                outcome(match().withVariableMatchers(exactMatch().withMatcher("Andrew").build()), action().withResult(Result.read_request_timeout))
                        )
                )
                .build();

        primingClient.multiPrime(prime);

        CassandraResult chrisResult = cassandra().prepareAndExecute(query, "Chris");
        CassandraResult andrewResult = cassandra().prepareAndExecute(query, "Andrew");

        assertEquals(Result.success, chrisResult.status().getResult());
        assertEquals(Result.read_request_timeout, andrewResult.status().getResult());
    }

    @Test
    public void testPrimeBasedOnMatcherNumericTypes() {
        String query = "select * from person where i = ? and bi = ? and f = ? and doub = ? and dec = ? and vint = ?";
        MultiPrimeRequest prime = MultiPrimeRequest.request()
                .withWhen(when()
                        .withQuery(query))
                .withThen(then()
                        .withVariableTypes(INT, BIG_INT, FLOAT, DOUBLE, DECIMAL, VAR_INT)
                        .withOutcomes(
                                outcome(match().withVariableMatchers(
                                        exactMatch(1),
                                        exactMatch(2L),
                                        exactMatch(3.0F),
                                        exactMatch(4.0),
                                        exactMatch(new BigDecimal("5.0")),
                                        exactMatch(new BigInteger("6"))), action().withResult(Result.unavailable)),
                                outcome(match().withVariableMatchers(
                                        exactMatch(11),
                                        exactMatch(12L),
                                        exactMatch(13.0F),
                                        exactMatch(14.0),
                                        exactMatch(new BigDecimal("15.0")),
                                        exactMatch(new BigInteger("16"))), action().withResult(Result.write_request_timeout))

                        )
                )
                .build();

        primingClient.multiPrime(prime);

        CassandraResult unavailable = cassandra().prepareAndExecute(query, 1, 2L, 3.0F, 4.0, new BigDecimal("5.0"), new BigInteger("6"));
        CassandraResult writeTimeout = cassandra().prepareAndExecute(query, 11, 12L, 13F, 14.0, new BigDecimal("15.0"), new BigInteger("16"));

        assertEquals(Result.unavailable, unavailable.status().getResult());
        assertEquals(Result.write_request_timeout, writeTimeout.status().getResult());
    }

    @Test
    public void testPrimeBasedOnMatcherUUIDs() {
        String query = "select * from person where u = ? and t = ?";
        java.util.UUID uuidOne = java.util.UUID.randomUUID();
        java.util.UUID tUuidOne = java.util.UUID.fromString("59ad61d0-c540-11e2-881e-b9e6057626c4");
        java.util.UUID uuidTwo = java.util.UUID.randomUUID();
        java.util.UUID tUuidTwo = java.util.UUID.fromString("59ad61d0-c540-11e2-881e-b9e6057626c4");

        MultiPrimeRequest prime = MultiPrimeRequest.request()
                .withWhen(when()
                        .withQuery(query))
                .withThen(then()
                        .withVariableTypes(UUID, TIMEUUID)
                        .withOutcomes(
                                outcome(match().withVariableMatchers(
                                        exactMatch(uuidOne),
                                        exactMatch(tUuidOne)),
                                        action().withResult(Result.read_request_timeout)),
                                outcome(match().withVariableMatchers(
                                        exactMatch(uuidTwo),
                                        exactMatch(tUuidTwo)),
                                        action().withResult(Result.write_request_timeout))

                        )
                )
                .build();

        primingClient.multiPrime(prime);

        CassandraResult readTimeout = cassandra().prepareAndExecute(query, uuidOne, tUuidOne);
        CassandraResult writeTimeout = cassandra().prepareAndExecute(query, uuidTwo, tUuidTwo);

        assertEquals(Result.read_request_timeout, readTimeout.status().getResult());
        assertEquals(Result.write_request_timeout, writeTimeout.status().getResult());
    }

    @Test
    public void testPrimeBasedOnMatchBoolean() {
        String query = "select * from person where clever = ?";
        MultiPrimeRequest prime = MultiPrimeRequest.request()
                .withWhen(when()
                        .withQuery(query))
                .withThen(then()
                        .withVariableTypes(BOOLEAN)
                        .withOutcomes(
                                outcome(match().withVariableMatchers(exactMatch().withMatcher(false).build()), action().withResult(Result.read_request_timeout)),
                                outcome(match().withVariableMatchers(exactMatch().withMatcher(true).build()), action().withResult(Result.write_request_timeout))
                        )
                )
                .build();

        primingClient.multiPrime(prime);

        CassandraResult falseResult = cassandra().prepareAndExecute(query, false);
        CassandraResult trueResult = cassandra().prepareAndExecute(query, true);

        assertEquals(Result.read_request_timeout, falseResult.status().getResult());
        assertEquals(Result.write_request_timeout, trueResult.status().getResult());
    }

    @Test
    public void testPrimeBasedOnMatchInet() throws Exception {
        String query = "select * from person where ip = ?";
        InetAddress localHost = InetAddress.getLocalHost();
        MultiPrimeRequest prime = MultiPrimeRequest.request()
                .withWhen(when()
                        .withQuery(query))
                .withThen(then()
                        .withVariableTypes(INET)
                        .withOutcomes(
                                outcome(match().withVariableMatchers(exactMatch().withMatcher(localHost).build()), action().withResult(Result.read_request_timeout))
                        )
                )
                .build();

        primingClient.multiPrime(prime);

        CassandraResult result = cassandra().prepareAndExecute(query, localHost);

        assertEquals(Result.read_request_timeout, result.status().getResult());
    }

    @Test
    public void testPrimeBasedOnMatchTimestamps() throws Exception {
        String query = "select * from person where time = ?";
        Date today = new Date();
        MultiPrimeRequest prime = MultiPrimeRequest.request()
                .withWhen(when()
                        .withQuery(query))
                .withThen(then()
                        .withVariableTypes(TIMESTAMP)
                        .withOutcomes(
                                outcome(match().withVariableMatchers(exactMatch().withMatcher(today).build()), action().withResult(Result.read_request_timeout))
                        )
                )
                .build();

        primingClient.multiPrime(prime);

        CassandraResult result = cassandra().prepareAndExecute(query, today);

        assertEquals(Result.read_request_timeout, result.status().getResult());
    }

    //todo blobs
}
