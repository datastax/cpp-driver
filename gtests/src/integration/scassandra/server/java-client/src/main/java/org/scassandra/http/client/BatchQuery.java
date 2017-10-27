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
import java.util.Collections;
import java.util.List;

import org.scassandra.cql.CqlType;

public final class BatchQuery {

    private final String query;
    private final BatchQueryKind batchQueryKind;
    private final List<Object> variables;
    private final List<CqlType> variableTypes;

    private BatchQuery(String query, BatchQueryKind batchQueryKind, List<Object> variables, List<CqlType> variableTypes) {
        this.query = query;
        this.batchQueryKind = batchQueryKind;
        this.variables = variables;
        this.variableTypes = variableTypes;
    }

    public String getQuery() {
        return query;
    }

    public BatchQueryKind getBatchQueryKind() {
        return batchQueryKind;
    }

    public List<Object> getVariables() {
        return variables;
    }

    public List<CqlType> getVariableTypes() {
        return variableTypes;
    }

    @Override
    public String toString() {
        return "BatchQuery{" +
                "query='" + query + '\'' +
                ", batchQueryKind=" + batchQueryKind +
                ", variables=" + variables +
                ", variableTypes=" + variableTypes +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BatchQuery that = (BatchQuery) o;

        if (query != null ? !query.equals(that.query) : that.query != null) return false;
        if (batchQueryKind != that.batchQueryKind) return false;
        if (variables != null ? !variables.equals(that.variables) : that.variables != null) return false;
        return variableTypes != null ? variableTypes.equals(that.variableTypes) : that.variableTypes == null;
    }

    @Override
    public int hashCode() {
        int result = query != null ? query.hashCode() : 0;
        result = 31 * result + (batchQueryKind != null ? batchQueryKind.hashCode() : 0);
        result = 31 * result + (variables != null ? variables.hashCode() : 0);
        result = 31 * result + (variableTypes != null ? variableTypes.hashCode() : 0);
        return result;
    }

    public static BatchQueryBuilder builder() {
        return new BatchQueryBuilder();
    }

    public static class BatchQueryBuilder {
        private String query;
        private BatchQueryKind batchQueryKind = BatchQueryKind.query;
        private List<Object> variables = new ArrayList<Object>();
        private List<CqlType> variableTypes = Collections.emptyList();

        private BatchQueryBuilder() {
        }

        public BatchQueryBuilder withQuery(String query) {
            this.query = query;
            return this;
        }

        public BatchQueryBuilder withType(BatchQueryKind type) {
            this.batchQueryKind = type;
            return this;
        }

        public BatchQueryBuilder withVariables(Object... variables) {
            this.variables = Arrays.asList(variables);
            return this;
        }

        public BatchQueryBuilder withVariableTypes(CqlType... variableTypes) {
            this.variableTypes = Arrays.asList(variableTypes);
            return this;
        }

        public BatchQuery build() {
            return new BatchQuery(query, batchQueryKind, variables, variableTypes);
        }
    }
}
