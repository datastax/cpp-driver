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

import cassandra.CassandraExecutor30;
import common.CassandraResult;
import org.junit.Test;
import org.scassandra.http.client.MultiPrimeRequest;
import org.scassandra.http.client.Result;

import static org.junit.Assert.assertEquals;
import static org.scassandra.cql.PrimitiveType.SMALL_INT;
import static org.scassandra.cql.PrimitiveType.TINY_INT;
import static org.scassandra.http.client.MultiPrimeRequest.*;

public class PreparedStatementPrimeOnVariablesTest30 extends PreparedStatementPrimeOnVariables {
    public PreparedStatementPrimeOnVariablesTest30() {
        super(new CassandraExecutor30(scassandra.getBinaryPort()));
    }

    @Test
    public void testPrimeBasedOnMatcherNewNumericTypes() {
        String query = "select * from person where sint = ? tint = ?";
        MultiPrimeRequest prime = MultiPrimeRequest.request()
                .withWhen(when()
                        .withQuery(query))
                .withThen(then()
                        .withVariableTypes(SMALL_INT, TINY_INT)
                        .withOutcomes(
                                outcome(match().withVariableMatchers(
                                        exactMatch((short)512),
                                        exactMatch((byte)127)), action().withResult(Result.unavailable)),
                                outcome(match().withVariableMatchers(
                                        exactMatch((short)632),
                                        exactMatch((byte)8)), action().withResult(Result.write_request_timeout))

                        )
                )
                .build();

        primingClient.multiPrime(prime);

        CassandraResult unavailable = cassandra().prepareAndExecute(query, (short)512, (byte)127);
        CassandraResult writeTimeout = cassandra().prepareAndExecute(query, (short)632, (byte)8);

        assertEquals(Result.unavailable, unavailable.status().getResult());
        assertEquals(Result.write_request_timeout, writeTimeout.status().getResult());
    }
}
