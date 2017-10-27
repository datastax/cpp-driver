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

import java.util.*;
import java.util.List;

/**
 * This uses inner classes from PrimingRequest for backward compatibility purposes.
 */

public final class BatchPrimingRequest {

    private final BatchWhen when;
    private final PrimingRequest.Then then;

    private BatchPrimingRequest(BatchWhen when, PrimingRequest.Then then) {
        this.when = when;
        this.then = then;
    }

    public static BatchPrimingRequestBuilder batchPrimingRequest() {
        return new BatchPrimingRequestBuilder();
    }

    public static PrimingRequest.Then.ThenBuilder then() {
        return new PrimingRequest.Then.ThenBuilder();
    }

    public BatchWhen getWhen() {
        return when;
    }

    public PrimingRequest.Then getThen() {
        return then;
    }

    public static class BatchPrimingRequestBuilder {
        private PrimingRequest.Then then;
        private List<BatchQueryPrime> queries;
        private List<Consistency> consistency;

        private BatchType type = BatchType.LOGGED;

        private BatchPrimingRequestBuilder() {
        }

        public BatchPrimingRequestBuilder withQueries(BatchQueryPrime... queries) {
            this.queries = Arrays.asList(queries);
            return this;
        }

        public BatchPrimingRequestBuilder withConsistency(Consistency... consistencies) {
            this.consistency = Arrays.asList(consistencies);
            return this;
        }

        public BatchPrimingRequestBuilder withThen(PrimingRequest.Then then) {
            this.then = then;
            return this;
        }

        public BatchPrimingRequestBuilder withThen(PrimingRequest.Then.ThenBuilder then) {
            this.then = then.build();
            return this;
        }

        public BatchPrimingRequestBuilder withType(BatchType type) {
            this.type = type;
            return this;
        }
        public BatchPrimingRequest build() {
            if (then == null) {
                throw new IllegalStateException("Must provide withThen before building");
            }
            return new BatchPrimingRequest(new BatchWhen(consistency, queries, type), then);
        }
    }

    public final static class BatchWhen {
        private final List<Consistency> consistency;
        private final List<BatchQueryPrime> queries;

        private final BatchType batchType;

        private BatchWhen(List<Consistency> consistency, List<BatchQueryPrime> queries, BatchType batchType) {
            this.consistency = consistency;
            this.queries = queries;
            this.batchType = batchType;
        }

        public List<Consistency> getConsistency() {
            return Collections.unmodifiableList(consistency);
        }

        public List<BatchQueryPrime> getQueries() {
            return Collections.unmodifiableList(queries);
        }
        public BatchType getBatchType() {
            return batchType;
        }
    }

}
