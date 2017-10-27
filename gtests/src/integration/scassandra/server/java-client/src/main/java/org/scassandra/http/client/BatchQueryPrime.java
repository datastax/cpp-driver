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

/**
 * Not reusing BatchQuery as we'll want to put regular expressions and variables here
 * at some point
 */
public final class BatchQueryPrime {

    public static BatchQueryPrimeBuilder batchQueryPrime() {
        return new BatchQueryPrimeBuilder();
    }

    public static BatchQueryPrime batchQueryPrime(String query, BatchQueryKind kind) {
        return new BatchQueryPrime(query, kind);
    }

    private final String text;
    private final BatchQueryKind kind;

    private BatchQueryPrime(String text, BatchQueryKind kind) {
        this.text = text;
        this.kind = kind;
    }

    public String getText() {
        return text;
    }

    public BatchQueryKind getKind() {
        return kind;
    }
    @Override
    public String toString() {
        return "BatchQueryPrime{" +
                "text='" + text + '\'' +
                ", kind=" + kind +
                '}';
    }

    public static class BatchQueryPrimeBuilder {
        private String query;
        private BatchQueryKind kind;

        private BatchQueryPrimeBuilder() {
        }

        public BatchQueryPrimeBuilder withQuery(String query) {
            this.query = query;
            return this;
        }

        public BatchQueryPrimeBuilder withKind(BatchQueryKind kind) {
            this.kind = kind;
            return this;
        }

        public BatchQueryPrime build() {
            return new BatchQueryPrime(query, kind);
        }
    }
}
