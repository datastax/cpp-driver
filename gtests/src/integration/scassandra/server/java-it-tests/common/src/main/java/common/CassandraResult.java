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
package common;

import org.scassandra.http.client.Result;
import org.scassandra.http.client.WriteTypePrime;

import java.util.List;

public interface CassandraResult<C extends CassandraRow> {
    List<C> rows();

    ResponseStatus status();

    public abstract static class ResponseStatus {

        private final Result result;

        public ResponseStatus(Result result) {
            this.result = result;
        }

        public Result getResult() {
            return result;
        }
    }

    public abstract static class ErrorStatus extends ResponseStatus {
        private final String consistency;
        public ErrorStatus(Result result, String consistency) {
            super(result);
            this.consistency = consistency;
        }

        public String getConsistency() {
            return consistency;
        }

    }

    public static class SuccessStatus extends ResponseStatus {
        public SuccessStatus() {
            super(Result.success);
        }
    }

    public static class ErrorMessageStatus extends ErrorStatus {
        private final String message;

        public ErrorMessageStatus(Result result, String message) {
            super(result, null);
            this.message = message;
        }

        public String getMessage() {
            return message;
        }
    }

    public static class ReadTimeoutStatus extends ErrorStatus {
        private final int receivedAcknowledgements;
        private final int requiredAcknowledgements;
        private final boolean wasDataRetrieved;

        public ReadTimeoutStatus(String consistency, int receivedAcknowledgements, int requiredAcknowledgements, boolean wasDataRetrieved) {
            super(Result.read_request_timeout, consistency);
            this.receivedAcknowledgements = receivedAcknowledgements;
            this.requiredAcknowledgements = requiredAcknowledgements;
            this.wasDataRetrieved = wasDataRetrieved;
        }

        public int getReceivedAcknowledgements() {
            return receivedAcknowledgements;
        }

        public int getRequiredAcknowledgements() {
            return requiredAcknowledgements;
        }

        public boolean WasDataRetrieved() {
            return wasDataRetrieved;
        }
    }

    public static class ReadFailureStatus extends ErrorStatus {
        private final int receivedAcknowledgements;
        private final int requiredAcknowledgements;
        private final int numberOfFailures;
        private final boolean wasDataRetrieved;

        public ReadFailureStatus(String consistency, int receivedAcknowledgements, int requiredAcknowledgements, int numberOfFailures, boolean wasDataRetrieved) {
            super(Result.read_failure, consistency);
            this.receivedAcknowledgements = receivedAcknowledgements;
            this.requiredAcknowledgements = requiredAcknowledgements;
            this.numberOfFailures = numberOfFailures;
            this.wasDataRetrieved = wasDataRetrieved;
        }

        public int getReceivedAcknowledgements() {
            return receivedAcknowledgements;
        }

        public int getRequiredAcknowledgements() {
            return requiredAcknowledgements;
        }

        public int getNumberOfFailures() {
            return numberOfFailures;
        }

        public boolean WasDataRetrieved() {
            return wasDataRetrieved;
        }
    }

    public static class WriteTimeoutStatus extends ErrorStatus {
        private final int receivedAcknowledgements;
        private final int requiredAcknowledgements;
        private final WriteTypePrime writeTypePrime;

        public WriteTimeoutStatus(String consistency, int receivedAcknowledgements, int requiredAcknowledgements, WriteTypePrime writeTypePrime) {
            super(Result.write_request_timeout, consistency);
            this.receivedAcknowledgements = receivedAcknowledgements;
            this.requiredAcknowledgements = requiredAcknowledgements;
            this.writeTypePrime = writeTypePrime;
        }

        public int getReceivedAcknowledgements() {
            return receivedAcknowledgements;
        }

        public int getRequiredAcknowledgements() {
            return requiredAcknowledgements;
        }

        public WriteTypePrime getWriteTypePrime() {
            return writeTypePrime;
        }
    }

    public static class WriteFailureStatus extends ErrorStatus {
        private final int receivedAcknowledgements;
        private final int requiredAcknowledgements;
        private final int numberOfFailures;
        private final WriteTypePrime writeTypePrime;

        public WriteFailureStatus(String consistency, int receivedAcknowledgements, int requiredAcknowledgements, int numberOfFailures, WriteTypePrime writeTypePrime) {
            super(Result.write_failure, consistency);
            this.receivedAcknowledgements = receivedAcknowledgements;
            this.requiredAcknowledgements = requiredAcknowledgements;
            this.numberOfFailures = numberOfFailures;
            this.writeTypePrime = writeTypePrime;
        }

        public int getReceivedAcknowledgements() {
            return receivedAcknowledgements;
        }

        public int getRequiredAcknowledgements() {
            return requiredAcknowledgements;
        }

        public int getNumberOfFailures() {
            return numberOfFailures;
        }

        public WriteTypePrime getWriteTypePrime() {
            return writeTypePrime;
        }
    }

    public static class UnavailableStatus extends ErrorStatus {
        private final int requiredAcknowledgements;
        private final int alive;

        public UnavailableStatus(String consistency, int requiredAcknowledgements, int alive) {
            super(Result.unavailable, consistency);
            this.requiredAcknowledgements = requiredAcknowledgements;
            this.alive = alive;
        }

        public int getRequiredAcknowledgements() {
            return requiredAcknowledgements;
        }

        public int getAlive() {
            return alive;
        }

    }

    public static class FunctionFailureStatus extends ErrorStatus {
        private final String keyspace;
        private final String function;
        private final String argTypes;

        public FunctionFailureStatus(String keyspace, String function, String argTypes) {
            super(Result.function_failure, null);
            this.keyspace = keyspace;
            this.function = function;
            this.argTypes = argTypes;
        }

        public String getKeyspace() {
            return keyspace;
        }

        public String getFunction() {
            return function;
        }

        public String getArgTypes() {
            return argTypes;
        }
    }
}
