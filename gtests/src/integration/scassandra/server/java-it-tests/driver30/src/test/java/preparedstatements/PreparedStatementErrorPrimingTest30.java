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
import org.junit.Ignore;
import org.junit.Test;
import org.scassandra.http.client.*;

import static org.junit.Assert.assertEquals;
import static org.scassandra.http.client.Result.*;

@Ignore
public class PreparedStatementErrorPrimingTest30 extends PreparedStatementErrorPrimingTest {

    public PreparedStatementErrorPrimingTest30() {
        super(new CassandraExecutor30(scassandra.getBinaryPort()));
    }

    @Test
    public void testPrimingReadFailure() {
        ReadFailureConfig readFailureConfig = new ReadFailureConfig(1, 2, 1, true);
        String query = "select * from people";
        PrimingRequest prime = PrimingRequest.preparedStatementBuilder()
                .withQuery(query)
                .withResult(read_failure)
                .withConfig(readFailureConfig)
                .build();
        primingClient.prime(prime);

        CassandraResult cassandraResult = cassandra().prepareAndExecute(query);

        CassandraResult.ResponseStatus status = cassandraResult.status();
        assertEquals(read_failure, status.getResult());
        assertEquals(1, ((CassandraResult.ReadFailureStatus) status).getReceivedAcknowledgements());
        assertEquals(2, ((CassandraResult.ReadFailureStatus) status).getRequiredAcknowledgements());
        assertEquals(1, ((CassandraResult.ReadFailureStatus) status).getNumberOfFailures());
        assertEquals(true, ((CassandraResult.ReadFailureStatus) status).WasDataRetrieved());
    }

    @Test
    public void testPrimingWriteFailure() {
        WriteFailureConfig writeFailureConfig = new WriteFailureConfig(WriteTypePrime.CAS, 2, 3, 1);
        String query = "select * from people";
        PrimingRequest prime = PrimingRequest.preparedStatementBuilder()
                .withQuery(query)
                .withResult(write_failure)
                .withConfig(writeFailureConfig)
                .build();
        primingClient.prime(prime);

        CassandraResult cassandraResult = cassandra().prepareAndExecute(query);

        CassandraResult.ResponseStatus status = cassandraResult.status();
        assertEquals(write_failure, status.getResult());
        assertEquals(2, ((CassandraResult.WriteFailureStatus) status).getReceivedAcknowledgements());
        assertEquals(3, ((CassandraResult.WriteFailureStatus) status).getRequiredAcknowledgements());
        assertEquals(1, ((CassandraResult.WriteFailureStatus) status).getNumberOfFailures());
        assertEquals(WriteTypePrime.CAS, ((CassandraResult.WriteFailureStatus) status).getWriteTypePrime());
    }

    @Test
    public void testPrimingFunctionFailure() {
        FunctionFailureConfig functionFailureConfig = new FunctionFailureConfig("ks", "fun", "ascii,bigint");
        String query = "select * from people";
        PrimingRequest prime = PrimingRequest.preparedStatementBuilder()
                .withQuery(query)
                .withResult(function_failure)
                .withConfig(functionFailureConfig)
                .build();
        primingClient.prime(prime);

        CassandraResult cassandraResult = cassandra().prepareAndExecute(query);

        CassandraResult.ResponseStatus status = cassandraResult.status();
        assertEquals(function_failure, status.getResult());
        // Java driver doesn't currently surface function failure parameters.
    }
}
