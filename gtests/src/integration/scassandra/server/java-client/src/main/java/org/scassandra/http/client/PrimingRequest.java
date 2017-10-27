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

import org.scassandra.cql.CqlType;
import org.scassandra.http.client.types.ColumnMetadata;

import java.util.*;

public final class PrimingRequest {

    public static PrimingRequestBuilder queryBuilder() {
        return new PrimingRequestBuilder(PrimingRequestBuilder.PrimeType.QUERY);
    }

    public static PrimingRequestBuilder preparedStatementBuilder() {
        return new PrimingRequestBuilder(PrimingRequestBuilder.PrimeType.PREPARED);
    }

    public static Then.ThenBuilder then() {
        return new Then.ThenBuilder();
    }

    public static Then.ThenBuilder andThen() {
        return then();
    }

    transient PrimingRequestBuilder.PrimeType primeType;

    public static class PrimingRequestBuilder {

        PrimeType type;
        static enum PrimeType {
            QUERY, PREPARED
        }
        private PrimingRequestBuilder(PrimeType type) {
            this.type = type;
        }

        private Consistency[] consistency;

        private List<ColumnMetadata> columnTypesMeta;
        private List<CqlType> variableTypesMeta;
        private String query;
        private String queryPattern;
        private List<Map<String, ?>> rows;
        private Result result = Result.success;
        private Long fixedDelay;
        private Map<String, Object> config = new HashMap<String, Object>();
        private Then then;

        public PrimingRequestBuilder withQuery(String query) {
            this.query = query;
            return this;
        }

        public PrimingRequestBuilder withConsistency(Consistency... consistencies) {
            consistency = consistencies;
            return this;
        }

        public PrimingRequestBuilder withQueryPattern(String queryPattern) {
            this.queryPattern = queryPattern;
            return this;
        }

        /**
         * If this method is used on the builder any call to the deprecated withThen methods
         * will be ignored.
         * @param then The action to take if the prime matches
         * @return this
         */
        public PrimingRequestBuilder withThen(Then then) {
            this.then = then;
            return this;
        }

        /**
         * If this method is used on the builder any call to the deprecated withThen methods
         * will be ignored.
         * @param then The action to take if the prime matches
         * @return this
         */
        public PrimingRequestBuilder withThen(Then.ThenBuilder then) {
            this.then = then.build();
            return this;
        }

        /**
         * @deprecated Use ThenBuilder instead.
         */
        @Deprecated
        public PrimingRequestBuilder withFixedDelay(long fixedDelay) {
            this.fixedDelay = fixedDelay;
            return this;
        }

        /**
         * @deprecated Use ThenBuilder instead.
         */
        @Deprecated
        public PrimingRequestBuilder withRows(List<Map<String, ?>> rows) {
            this.rows = rows;
            return this;
        }

        /**
         * @deprecated Use ThenBuilder instead.
         */
        @Deprecated
        public final PrimingRequestBuilder withRows(Map<String, ? extends Object>... rows) {
            this.rows = Arrays.asList(rows);
            return this;
        }

        /**
         * @deprecated Use ThenBuilder instead.
         */
        @Deprecated
        public PrimingRequestBuilder withResult(Result result) {
            this.result = result;
            return this;
        }

        /**
         * @deprecated Use ThenBuilder instead.
         */
        @Deprecated
        public PrimingRequestBuilder withColumnTypes(ColumnMetadata... columnMetadata) {
            this.columnTypesMeta = Arrays.asList(columnMetadata);
            return this;
        }

        /**
         * @deprecated Use ThenBuilder instead.
         */
        @Deprecated
        public PrimingRequestBuilder withConfig(Config readTimeoutConfig) {
            this.config.putAll(readTimeoutConfig.getProperties());
            return this;
        }

        /**
         * @deprecated Use ColumnMetadata instead. This will be removed in version 1.0
         */
        @Deprecated
        public PrimingRequestBuilder withColumnTypes(Map<String, ColumnTypes> types) {
            List<ColumnMetadata> columnMetadata = new ArrayList<ColumnMetadata>();
            for (Map.Entry<String, ColumnTypes> entry : types.entrySet()) {
                columnMetadata.add(ColumnMetadata.column(entry.getKey(), entry.getValue().getType()));
            }
            this.columnTypesMeta = columnMetadata;
            return this;
        }

        /**
         * @deprecated Use CqlType instead. This will be removed in version 1.0
         */
        @Deprecated
        public PrimingRequestBuilder withVariableTypes(ColumnTypes... variableTypes) {
            List<CqlType> variableTypesMeta = new ArrayList<CqlType>();
            for (ColumnTypes variableType : variableTypes) {
                variableTypesMeta.add(variableType.getType());
            }
            this.variableTypesMeta = variableTypesMeta;
            return this;
        }

        /**
         * @deprecated Use ThenBuilder instead.
         */
        @Deprecated
        public PrimingRequestBuilder withVariableTypes(CqlType... variableTypes) {
            this.variableTypesMeta = Arrays.asList(variableTypes);
            return this;
        }

        public PrimingRequest build() {
            if (query != null && queryPattern != null) {
                throw new IllegalStateException("Can't specify query and queryPattern");
            }

            if (query == null && queryPattern == null) {
                throw new IllegalStateException("Must set either query or queryPattern for PrimingRequest");
            }

            List<Consistency> consistencies = consistency == null ? null : Arrays.asList(consistency);


            if (then == null) {
                List<Map<String, ? extends Object>> rowsDefaultedToEmptyForSuccess = this.rows;

                if (result == Result.success && rows == null) {
                    rowsDefaultedToEmptyForSuccess = Collections.emptyList();
                }
                return new PrimingRequest(type, query, queryPattern, consistencies, rowsDefaultedToEmptyForSuccess, result, columnTypesMeta, variableTypesMeta, fixedDelay, config);
            } else {
                return new PrimingRequest(type, query, queryPattern, consistencies, then);
            }
        }
    }

    private final When when;
    private final Then then;

    private PrimingRequest(PrimingRequestBuilder.PrimeType primeType, String query, String queryPattern, List<Consistency> consistency, List<Map<String, ?>> rows, Result result, List<ColumnMetadata> columnTypes, List<CqlType> variableTypes, Long fixedDelay, Map<String, Object> config) {
        this(primeType, query, queryPattern, consistency, new Then(rows, result, columnTypes, variableTypes, fixedDelay, config));
    }

    private PrimingRequest(PrimingRequestBuilder.PrimeType primeType, String query, String queryPattern, List<Consistency> consistency, Then then) {
        this.primeType = primeType;
        this.when = new When(query, queryPattern, consistency);
        this.then = then;
    }

    public When getWhen() {
        return when;
    }

    public Then getThen() {
        return then;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PrimingRequest that = (PrimingRequest) o;

        if (then != null ? !then.equals(that.then) : that.then != null) return false;
        if (when != null ? !when.equals(that.when) : that.when != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = when != null ? when.hashCode() : 0;
        result = 31 * result + (then != null ? then.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "PrimingRequest{" +
                "when='" + when + '\'' +
                ", then=" + then +
                '}';
    }

    public final static class Then {
        private final List<CqlType> variable_types;
        private final List<Map<String, ? extends Object>> rows;
        private final Result result;
        private final Map<String, CqlType> column_types;
        private final Long fixedDelay;
        private final Map<String, Object> config;

        private Then(List<Map<String, ?>> rows, Result result, List<ColumnMetadata> column_types, List<CqlType> variable_types, Long fixedDelay, Map<String, Object> config) {
            this.rows = rows;
            this.result = result;
            this.variable_types = variable_types;
            this.fixedDelay = fixedDelay;
            this.config = config.isEmpty() ? null : config;

            if (column_types != null) {
                this.column_types = new HashMap<String, CqlType>();
                for (ColumnMetadata column_type : column_types) {
                    this.column_types.put(column_type.getName(), column_type.getType());
                }
            } else {
                this.column_types = null;
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Then then = (Then) o;

            if (column_types != null ? !column_types.equals(then.column_types) : then.column_types != null)
                return false;
            if (config != null ? !config.equals(then.config) : then.config != null) return false;
            if (fixedDelay != null ? !fixedDelay.equals(then.fixedDelay) : then.fixedDelay != null) return false;
            if (result != then.result) return false;
            if (rows != null ? !rows.equals(then.rows) : then.rows != null) return false;
            if (variable_types != null ? !variable_types.equals(then.variable_types) : then.variable_types != null)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result1 = variable_types != null ? variable_types.hashCode() : 0;
            result1 = 31 * result1 + (rows != null ? rows.hashCode() : 0);
            result1 = 31 * result1 + (result != null ? result.hashCode() : 0);
            result1 = 31 * result1 + (column_types != null ? column_types.hashCode() : 0);
            result1 = 31 * result1 + (fixedDelay != null ? fixedDelay.hashCode() : 0);
            result1 = 31 * result1 + (config != null ? config.hashCode() : 0);
            return result1;
        }

        @Override
        public String toString() {
            return "Then{" +
                    "variable_types=" + variable_types +
                    ", rows=" + rows +
                    ", result=" + result +
                    ", column_types=" + column_types +
                    ", fixedDelay=" + fixedDelay +
                    ", config=" + config +
                    '}';
        }

        public List<CqlType> getVariableTypes() {
            return variable_types;
        }

        public List<Map<String, ? extends Object>> getRows() {
            return Collections.unmodifiableList(rows);
        }

        public Result getResult() {
            return result;
        }

        public Map<String, CqlType> getColumnTypes() {
            return column_types;
        }

        public long getFixedDelay() {
            return fixedDelay;
        }

        public Map<String, Object> getConfig() {
            return Collections.unmodifiableMap(config);
        }

        public static class ThenBuilder {
            private List<CqlType> variable_types;
            private List<Map<String, ? extends Object>> rows;
            private Result result;
            private Long fixedDelay;
            private Map<String, Object> config = new HashMap<String, Object>();
            private List<ColumnMetadata> columnTypesMeta;

            ThenBuilder() {
            }

            public ThenBuilder withVariableTypes(List<CqlType> variable_types) {
                this.variable_types = variable_types;
                return this;
            }

            public ThenBuilder withVariableTypes(CqlType... variableTypes) {
                this.variable_types = Arrays.asList(variableTypes);
                return this;
            }

            public ThenBuilder withRows(List<Map<String, ? extends Object>> rows) {
                this.rows = rows;
                return this;
            }

            public final ThenBuilder withRows(Map<String, ? extends Object>... rows) {
                this.rows = Arrays.asList(rows);
                return this;
            }

            public ThenBuilder withResult(Result result) {
                this.result = result;
                return this;
            }

            public ThenBuilder withColumnTypes(ColumnMetadata... columnMetadata) {
                this.columnTypesMeta = Arrays.asList(columnMetadata);
                return this;
            }

            public ThenBuilder withFixedDelay(Long fixedDelay) {
                this.fixedDelay = fixedDelay;
                return this;
            }

            /**
             * @deprecated Use withConfig(Config config) instead.
             */
            @Deprecated
            public ThenBuilder withConfig(Map<String, Object> config) {
                this.config = config;
                return this;
            }

            public ThenBuilder withConfig(Config config) {
                this.config.putAll(config.getProperties());
                return this;
            }

            public Then build() {
                List<Map<String, ? extends Object>> rowsDefaultedToEmptyForSuccess = this.rows;

                if (result == Result.success && rows == null) {
                    rowsDefaultedToEmptyForSuccess = Collections.emptyList();
                }

                return new Then(rowsDefaultedToEmptyForSuccess, result, columnTypesMeta,
                        variable_types, fixedDelay, config);
            }
        }
    }

    public final static class When {
        private final String query;
        private final String queryPattern;
        private final List<Consistency> consistency;

        private When(String query, String queryPattern, List<Consistency> consistency) {
            this.query = query;
            this.consistency = consistency;
            this.queryPattern = queryPattern;
        }

        @Override
        public String toString() {
            return "When{" +
                    "query='" + query + '\'' +
                    ", queryPattern='" + queryPattern + '\'' +
                    ", consistency=" + consistency +
                    '}';
        }

        public String getQuery() {
            return query;
        }

        public List<Consistency> getConsistency() {
            return Collections.unmodifiableList(consistency);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (!(o instanceof When))
                return false;

            When when = (When)o;

            if (consistency != null ? !consistency.equals(when.consistency) : when.consistency != null)
                return false;
            if (query != null ? !query.equals(when.query) : when.query != null)
                return false;
            if (queryPattern != null ? !queryPattern.equals(when.queryPattern) : when.queryPattern != null)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = query != null ? query.hashCode() : 0;
            result = 31 * result + (queryPattern != null ? queryPattern.hashCode() : 0);
            result = 31 * result + (consistency != null ? consistency.hashCode() : 0);
            return result;
        }
    }

}
