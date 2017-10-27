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
import common.CassandraResult;
import org.junit.Ignore;
import org.junit.Test;
import org.scassandra.http.client.*;

import static org.junit.Assert.*;
import static org.scassandra.http.client.Result.*;
import static org.scassandra.http.client.WriteTypePrime.BATCH_LOG;

abstract public class QueryErrorPrimingTest extends AbstractScassandraTest {

    public QueryErrorPrimingTest(CassandraExecutor cassandraExecutor) {
        super(cassandraExecutor);
    }

    @Test
    public void testPrimingReadTimeoutStatementCL() {
        ReadTimeoutConfig config = new ReadTimeoutConfig(2,3,false);
        testPrimingReadTimeout(config, Consistency.LOCAL_QUORUM, Consistency.LOCAL_QUORUM);
    }

    @Test
    public void testPrimingReadTimeoutPrimeCL() {
        ReadTimeoutConfig config = new ReadTimeoutConfig(2,3,false, Consistency.TWO);
        testPrimingReadTimeout(config, Consistency.LOCAL_QUORUM, Consistency.TWO);
    }

    private void testPrimingReadTimeout(ReadTimeoutConfig readTimeoutConfig, Consistency statementCL, Consistency expectedCL) {
        String query = "select * from people";
        PrimingRequest prime = PrimingRequest.queryBuilder()
                .withQuery(query)
                .withResult(read_request_timeout)
                .withConfig(readTimeoutConfig)
                .build();
        primingClient.prime(prime);

        CassandraResult cassandraResult = cassandra().executeSimpleStatement(query, statementCL.name());

        CassandraResult.ResponseStatus status = cassandraResult.status();
        assertEquals(read_request_timeout, status.getResult());
        assertEquals(expectedCL.name(), ((CassandraResult.ReadTimeoutStatus) status).getConsistency());
        assertEquals(2, ((CassandraResult.ReadTimeoutStatus) status).getReceivedAcknowledgements());
        assertEquals(3, ((CassandraResult.ReadTimeoutStatus) status).getRequiredAcknowledgements());
        assertFalse(((CassandraResult.ReadTimeoutStatus) status).WasDataRetrieved());
    }

    @Test
    public void testPrimingWriteTimeoutStatementCL() {
        WriteTimeoutConfig config = new WriteTimeoutConfig(BATCH_LOG, 2, 3);
        testPrimingWriteTimeout(config, Consistency.ALL, Consistency.ALL);
    }

    @Test
    public void testPrimingWriteTimeoutPrimeCL() {
        WriteTimeoutConfig config = new WriteTimeoutConfig(BATCH_LOG, 2, 3, Consistency.ONE);
        testPrimingWriteTimeout(config, Consistency.ALL, Consistency.ONE);
    }

    private void testPrimingWriteTimeout(WriteTimeoutConfig writeTimeoutConfig, Consistency statementCL, Consistency expectedCL) {
        String query = "select * from people";
        PrimingRequest prime = PrimingRequest.queryBuilder()
                .withQuery(query)
                .withResult(write_request_timeout)
                .withConfig(writeTimeoutConfig)
                .build();
        primingClient.prime(prime);

        CassandraResult cassandraResult = cassandra().executeSimpleStatement(query, statementCL.name());

        CassandraResult.ResponseStatus status = cassandraResult.status();
        assertEquals(write_request_timeout, status.getResult());
        assertEquals(expectedCL.name(), ((CassandraResult.WriteTimeoutStatus) status).getConsistency());
        assertEquals(2, ((CassandraResult.WriteTimeoutStatus) status).getReceivedAcknowledgements());
        assertEquals(3, ((CassandraResult.WriteTimeoutStatus) status).getRequiredAcknowledgements());
        assertEquals(BATCH_LOG, ((CassandraResult.WriteTimeoutStatus)status).getWriteTypePrime());
    }

    @Test
    public void testPrimingUnavailableStatementCL() {
        UnavailableConfig config = new UnavailableConfig(4,3);
        testPrimingUnavailable(config, Consistency.LOCAL_QUORUM, Consistency.LOCAL_QUORUM);
    }

    @Test
    public void testPrimingUnavailablePrimeCL() {
        UnavailableConfig config = new UnavailableConfig(4,3, Consistency.THREE);
        testPrimingUnavailable(config, Consistency.LOCAL_QUORUM, Consistency.THREE);
    }

    private void testPrimingUnavailable(UnavailableConfig unavailableConfig, Consistency statementCL, Consistency expectedCL) {
        String query = "select * from people";
        PrimingRequest prime = PrimingRequest.queryBuilder()
                .withQuery(query)
                .withResult(unavailable)
                .withConfig(unavailableConfig)
                .build();
        primingClient.prime(prime);

        CassandraResult cassandraResult = cassandra().executeSimpleStatement(query, statementCL.name());

        CassandraResult.ResponseStatus status = cassandraResult.status();
        assertEquals(unavailable, status.getResult());
        assertEquals(expectedCL.name(), ((CassandraResult.UnavailableStatus) status).getConsistency());
        assertEquals(4, ((CassandraResult.UnavailableStatus) status).getRequiredAcknowledgements());
        assertEquals(3, ((CassandraResult.UnavailableStatus) status).getAlive());
    }

    @Test
    public void testPrimingServerError() {
        String errorMessage = "Arbitrary Server Error";
        ErrorMessageConfig config = new ErrorMessageConfig(errorMessage);
        assertErrorMessageStatus(server_error, config, errorMessage);
    }

    @Test
    public void testPrimingProtocolError() {
        String errorMessage = "Arbitrary Protocol Error";
        ErrorMessageConfig config = new ErrorMessageConfig(errorMessage);
        assertErrorMessageStatus(protocol_error, config, "An unexpected protocol error occurred");
    }

    @Test
    public void testBadCredentials() {
        String errorMessage = "Bad Credentials";
        ErrorMessageConfig config = new ErrorMessageConfig(errorMessage);
        assertErrorMessageStatus(bad_credentials, config,
                String.format("Authentication error on host localhost/127.0.0.1:%s: ", scassandra.getBinaryPort()) + errorMessage);
    }

    @Test
    public void testOverloadedError() {
        ErrorMessageConfig config = new ErrorMessageConfig("");
        assertErrorMessageStatus(overloaded, config, "overloaded");
    }

    @Test
    public void testIsBootstrapping() {
        String errorMessage = "Lay off, i'm bootstrapping.";
        ErrorMessageConfig config = new ErrorMessageConfig(errorMessage);
        assertErrorMessageStatus(is_bootstrapping, config, "bootstrapping");
    }

    @Test
    public void testTruncateError() {
        String errorMessage = "Truncate Failure";
        ErrorMessageConfig config = new ErrorMessageConfig(errorMessage);
        assertErrorMessageStatus(truncate_error, config, errorMessage);
    }

    @Test
    public void testSyntaxError() {
        String errorMessage = "Bad Syntax";
        ErrorMessageConfig config = new ErrorMessageConfig(errorMessage);
        assertErrorMessageStatus(syntax_error, config, errorMessage);
    }

    @Test
    public void testUnauthorized() {
        String errorMessage = "Not allowed to do that";
        ErrorMessageConfig config = new ErrorMessageConfig(errorMessage);
        assertErrorMessageStatus(unauthorized, config, errorMessage);
    }

    @Test
    public void testInvalid() {
        String errorMessage = "Invalid query";
        ErrorMessageConfig config = new ErrorMessageConfig(errorMessage);
        assertErrorMessageStatus(invalid, config, errorMessage);
    }

    @Test
    @Ignore
    // Ignore as this is a legitimate bug in the driver (returns InvalidQueryException instead of
    // InvalidConfigurationInQueryException.
    public void testConfigError() {
        String errorMessage = "Configuration Error 12345";
        ErrorMessageConfig config = new ErrorMessageConfig(errorMessage);
        assertErrorMessageStatus(config_error, config, errorMessage);
    }

    @Test
    public void testAlreadyExists() {
        String keyspace = "hello";
        String table = "world";
        AlreadyExistsConfig config = new AlreadyExistsConfig(keyspace, table);
        assertErrorMessageStatus(already_exists, config, "Table hello.world already exists");

        // keyspace only
        assertErrorMessageStatus(already_exists, new AlreadyExistsConfig(keyspace), "Keyspace hello already exists");
    }

    @Test
    public void testUnprepared() {
        // This should never happen as result of a simple statement,
        // but interesting to see how driver behaves nonetheless.
        String prepareId = "0x86753090";
        UnpreparedConfig config = new UnpreparedConfig(prepareId);
        assertErrorMessageStatus(unprepared, config, "Tried to execute unknown prepared query " + prepareId);
    }

    @Test
    public void testClosedOnRequest() {
        ClosedConnectionConfig config = new ClosedConnectionConfig(ClosedConnectionConfig.CloseType.RESET);
        assertErrorMessageStatus(closed_connection, config,
            String.format("All host(s) tried for query failed (tried: localhost/127.0.0.1:%s (com.datastax.driver.core.TransportException: [localhost/127.0.0.1:%s] Connection has been closed))",
                    scassandra.getBinaryPort(), scassandra.getBinaryPort()),
            server_error);
    }

    private CassandraResult assertErrorMessageStatus(Result result, Config config, String expectedMsg) {
        return assertErrorMessageStatus(result, config, expectedMsg, result);
    }

    private CassandraResult assertErrorMessageStatus(Result result, Config config, String expectedMsg, Result expectedResult) {
        String query = "select * from people";
        String consistency = "LOCAL_ONE";
        PrimingRequest prime = PrimingRequest.queryBuilder()
            .withQuery(query)
            .withResult(result)
            .withConfig(config)
            .build();
        primingClient.prime(prime);

        CassandraResult cassandraResult = cassandra().executeSimpleStatement(query, consistency);

        CassandraResult.ResponseStatus status = cassandraResult.status();
        assertEquals(expectedResult, status.getResult());
        String actualErrorMessage = ((CassandraResult.ErrorMessageStatus) status).getMessage();
        assertTrue("Expected error message to contain: " + expectedMsg + " Got: " + actualErrorMessage, actualErrorMessage.contains(expectedMsg));
        return cassandraResult;
    }
}
