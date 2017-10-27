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
package org.scassandra.http.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.scassandra.http.client.BatchType.LOGGED;

public final class BatchExecution {

    private final List<BatchQuery> batchQueries;
    private final String consistency;
    private final String serialConsistency;
    private final BatchType batchType;
    private final Long timestamp;


    private BatchExecution(List<BatchQuery> batchQueries, String consistency, String serialConsistency,
                           BatchType batchType, Long timestamp) {
        this.batchQueries = batchQueries;
        this.consistency = consistency;
        this.serialConsistency = serialConsistency;
        this.batchType = batchType;
        this.timestamp = timestamp;
    }

    public List<BatchQuery> getBatchQueries() {
        return batchQueries;
    }

    public String getConsistency() {
        return consistency;
    }

    public String getSerialConsistency() {
        return serialConsistency;
    }

    public BatchType getBatchType() {
        return batchType;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BatchExecution that = (BatchExecution) o;

        if (batchQueries != null ? !batchQueries.equals(that.batchQueries) : that.batchQueries != null) return false;
        if (consistency != null ? !consistency.equals(that.consistency) : that.consistency != null) return false;
        if (serialConsistency != null ? !serialConsistency.equals(that.serialConsistency) : that.serialConsistency != null)
            return false;
        if (batchType != that.batchType) return false;
        return timestamp != null ? timestamp.equals(that.timestamp) : that.timestamp == null;
    }

    @Override
    public int hashCode() {
        int result = batchQueries != null ? batchQueries.hashCode() : 0;
        result = 31 * result + (consistency != null ? consistency.hashCode() : 0);
        result = 31 * result + (serialConsistency != null ? serialConsistency.hashCode() : 0);
        result = 31 * result + (batchType != null ? batchType.hashCode() : 0);
        result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "BatchExecution{" +
                "batchQueries=" + batchQueries +
                ", consistency='" + consistency + '\'' +
                ", serialConsistency='" + serialConsistency + '\'' +
                ", batchType=" + batchType +
                ", timestamp=" + timestamp +
                '}';
    }

    public static BatchExecutionBuilder builder() {
        return new BatchExecutionBuilder();
    }


    public static class BatchExecutionBuilder {
        private List<BatchQuery> batchQueries;
        private String consistency = "ONE";
        private String serialConsistency;
        private BatchType batchType = LOGGED;
        private Long timestamp;

        private BatchExecutionBuilder() {
        }

        public BatchExecutionBuilder withBatchQueries(List<BatchQuery> batchQueries) {
            this.batchQueries = batchQueries;
            return this;
        }

        public BatchExecutionBuilder withBatchQueries(BatchQuery... batchQueries) {
            this.batchQueries = Arrays.asList(batchQueries);
            return this;
        }

        public BatchExecutionBuilder withBatchQueries(BatchQuery.BatchQueryBuilder... batchQueries) {
            this.batchQueries = new ArrayList<BatchQuery>();
            for (BatchQuery.BatchQueryBuilder bq : batchQueries) {
                this.batchQueries.add(bq.build());
            }
            return this;
        }

        /**
         * Defaults to ONE if not set.
         *
         * @param consistency Query consistency
         * @return this builder
         */
        public BatchExecutionBuilder withConsistency(String consistency) {
            this.consistency = consistency;
            return this;
        }

        public BatchExecutionBuilder withSerialConsistency(String serialConsistency) {
            this.serialConsistency = serialConsistency;
            return this;
        }

        /**
         * Defaults to LOGGED if not set.
         *
         * @param batchType Batch type
         * @return this builder
         */
        public BatchExecutionBuilder withBatchType(BatchType batchType) {
            this.batchType = batchType;
            return this;
        }

        public BatchExecutionBuilder withTimestamp(Long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public BatchExecution build() {
            return new BatchExecution(batchQueries, consistency, serialConsistency, batchType, timestamp);
        }
    }
}
