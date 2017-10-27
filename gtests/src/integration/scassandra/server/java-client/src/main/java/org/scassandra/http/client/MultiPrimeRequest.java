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

import java.util.*;

public final class MultiPrimeRequest {

    public static MultiPrimeRequestBuilder request() {
        return new MultiPrimeRequestBuilder();
    }

    public static Then.Builder then() { return new Then.Builder(); }
    public static When.Builder when() { return new When.Builder(); }
    public static Criteria.Builder match() { return new Criteria.Builder(); }
    public static Action.Builder action() { return new Action.Builder(); }
    public static Outcome outcome(Criteria.Builder match, Action.Builder action) {
        return new Outcome(match.build(), action.build());
    }
    public static ExactMatch.Builder exactMatch() { return new ExactMatch.Builder(); }
    public static VariableMatch exactMatch(Object o) { return new ExactMatch.Builder().withMatcher(o).build(); }
    public static VariableMatch anyMatch() { return new AnyMatch(); }


    private final When when;
    private final Then then;

    private MultiPrimeRequest(When when, Then then) {
        this.when = when;
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

        MultiPrimeRequest that = (MultiPrimeRequest) o;

        if (when != null ? !when.equals(that.when) : that.when != null) return false;
        return then != null ? then.equals(that.then) : that.then == null;

    }

    @Override
    public int hashCode() {
        int result = when != null ? when.hashCode() : 0;
        result = 31 * result + (then != null ? then.hashCode() : 0);
        return result;
    }

    public final static class Action {
        private final List<Map<String, ? extends Object>> rows;
        private final Result result;
        private final Map<String, CqlType> column_types;
        private final Long fixedDelay;
        private final Map<String, Object> config;

        private Action(List<Map<String, ? extends Object>> rows, Result result, Map<String, CqlType> column_types, Long fixedDelay, Map<String, Object> config) {
            this.rows = rows;
            this.result = result;
            this.column_types = column_types;
            this.fixedDelay = fixedDelay;
            this.config = config;
        }

        public List<Map<String, ? extends Object>> getRows() {
            return rows;
        }

        public Result getResult() {
            return result;
        }

        public Long getFixedDelay() {
            return fixedDelay;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Action aAction = (Action) o;

            if (rows != null ? !rows.equals(aAction.rows) : aAction.rows != null) return false;
            if (result != aAction.result) return false;
            if (column_types != null ? !column_types.equals(aAction.column_types) : aAction.column_types != null)
                return false;
            if (fixedDelay != null ? !fixedDelay.equals(aAction.fixedDelay) : aAction.fixedDelay != null) return false;
            return config != null ? config.equals(aAction.config) : aAction.config == null;

        }

        @Override
        public int hashCode() {
            int result1 = rows != null ? rows.hashCode() : 0;
            result1 = 31 * result1 + (result != null ? result.hashCode() : 0);
            result1 = 31 * result1 + (column_types != null ? column_types.hashCode() : 0);
            result1 = 31 * result1 + (fixedDelay != null ? fixedDelay.hashCode() : 0);
            result1 = 31 * result1 + (config != null ? config.hashCode() : 0);
            return result1;
        }


        public static class Builder {
            private List<Map<String, ? extends Object>> rows;
            private Result result;
            private Map<String, CqlType> column_types;
            private Long fixedDelay;
            private Map<String, Object> config;

            private Builder() {
            }

            public Builder withRows(List<Map<String, ? extends Object>> rows) {
                this.rows = rows;
                return this;
            }

            final public Builder withRows(Map<String, ? extends Object>... rows) {
                this.rows = Arrays.asList(rows);
                return this;
            }

            public Builder withResult(Result result) {
                this.result = result;
                return this;
            }

            public Builder withColumnTypes(Map<String, CqlType> column_types) {
                this.column_types = column_types;
                return this;
            }

            public Builder withFixedDelay(Long fixedDelay) {
                this.fixedDelay = fixedDelay;
                return this;
            }

            public Builder withConfig(Map<String, Object> config) {
                this.config = config;
                return this;
            }

            public Action build() {
                return new Action(rows, result, column_types, fixedDelay, config);
            }
        }
    }

    public enum MatchType {
        exact,
        any
    }

    public static abstract class VariableMatch {
        private MatchType type = type();

        protected abstract MatchType type();

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            VariableMatch that = (VariableMatch) o;

            return type == that.type;
        }

        @Override
        public int hashCode() {
            return type != null ? type.hashCode() : 0;
        }
    }

    public static class AnyMatch extends VariableMatch {
        @Override
        public MatchType type() {
            return MatchType.any;
        }
    }

    public final static class ExactMatch extends VariableMatch {
        private final Object matcher;

        public ExactMatch(Object matcher) {
            this.matcher = matcher;
        }

        @Override
        public MatchType type() {
            return MatchType.exact;
        }

        public Object matcher() {
            return matcher;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ExactMatch that = (ExactMatch) o;

            return matcher != null ? matcher.equals(that.matcher) : that.matcher == null;

        }

        @Override
        public int hashCode() {
            return matcher != null ? matcher.hashCode() : 0;
        }

        public static class Builder {
            private Object matcher;

            private Builder() {
            }

            public Builder withMatcher(Object matcher) {
                this.matcher = matcher;
                return this;
            }

            public ExactMatch build() {
                return new ExactMatch(matcher);
            }
        }
    }

    public final static class Criteria {
        private final List<VariableMatch> variable_matcher;

        public Criteria(List<VariableMatch> variable_matcher) {
            this.variable_matcher = variable_matcher;
        }

        public List<VariableMatch> getVariableMatchers() {
            return variable_matcher;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Criteria criteria = (Criteria) o;

            return variable_matcher != null ? variable_matcher.equals(criteria.variable_matcher) : criteria.variable_matcher == null;

        }

        @Override
        public int hashCode() {
            return variable_matcher != null ? variable_matcher.hashCode() : 0;
        }

        public static class Builder {
            private List<VariableMatch> variable_matcher;

            private Builder() {
            }

            public Builder withVariableMatchers(VariableMatch... variable_matcher) {
                this.variable_matcher = Arrays.asList(variable_matcher);
                return this;
            }

            public Criteria build() {
                return new Criteria(variable_matcher);
            }
        }
    }

    public final static class Then {

        private final List<CqlType> variable_types;
        private final List<Outcome> outcomes;

        public Then(List<CqlType> variable_types, List<Outcome> outcomes) {
            this.variable_types = variable_types;
            this.outcomes = outcomes;
        }

        public List<CqlType> getVariableTypes() {
            return variable_types;
        }

        public List<Outcome> getOutcomes() {
            return outcomes;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Then then = (Then) o;

            if (variable_types != null ? !variable_types.equals(then.variable_types) : then.variable_types != null)
                return false;
            return outcomes != null ? outcomes.equals(then.outcomes) : then.outcomes == null;

        }

        @Override
        public int hashCode() {
            int result = variable_types != null ? variable_types.hashCode() : 0;
            result = 31 * result + (outcomes != null ? outcomes.hashCode() : 0);
            return result;
        }


        public static class Builder {
            private List<CqlType> variable_types;
            private List<Outcome> outcomes;

            private Builder() {
            }

            public Builder withVariableTypes(List<CqlType> variable_types) {
                this.variable_types = variable_types;
                return this;
            }

            public Builder withVariableTypes(CqlType... variable_types) {
                this.variable_types = Arrays.asList(variable_types);
                return this;
            }

            public Builder withOutcomes(Outcome... outcomes) {
                this.outcomes = Arrays.asList(outcomes);
                return this;
            }

            public Then build() {
                return new Then(variable_types, outcomes);
            }
        }
    }

    public static class Outcome {
        private Criteria criteria;
        private Action action;

        public Outcome(Criteria criteria, Action action) {
            this.criteria = criteria;
            this.action = action;
        }

        public Action getAction() {
            return action;
        }

        public Criteria getCriteria() {
            return criteria;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Outcome outcome = (Outcome) o;

            if (criteria != null ? !criteria.equals(outcome.criteria) : outcome.criteria != null) return false;
            return !(action != null ? !action.equals(outcome.action) : outcome.action != null);

        }

        @Override
        public int hashCode() {
            int result = criteria != null ? criteria.hashCode() : 0;
            result = 31 * result + (action != null ? action.hashCode() : 0);
            return result;
        }
    }

    public final static class When {
        private final String query;
        private final List<Consistency> consistency;

        private When(String query, List<Consistency> consistency) {
            this.query = query;
            this.consistency = consistency;
        }

        public String getQuery() {
            return query;
        }

        public List<Consistency> getConsistency() {
            return Collections.unmodifiableList(consistency);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            When when = (When) o;

            if (query != null ? !query.equals(when.query) : when.query != null) return false;
            return consistency != null ? consistency.equals(when.consistency) : when.consistency == null;

        }

        @Override
        public int hashCode() {
            int result = query != null ? query.hashCode() : 0;
            result = 31 * result + (consistency != null ? consistency.hashCode() : 0);
            return result;
        }

        public static class Builder {
            private String query;
            private List<Consistency> consistency;

            private Builder() {
            }

            public Builder withQuery(String query) {
                this.query = query;
                return this;
            }

            public Builder withConsistency(List<Consistency> consistency) {
                this.consistency = consistency;
                return this;
            }

            public Builder withConsistency(Consistency consistency, Consistency... consistencies) {
                this.consistency = new ArrayList<Consistency>();
                this.consistency.add(consistency);
                this.consistency.addAll(Arrays.asList(consistencies));
                return this;
            }

            public When build() {
                return new When(query, consistency);
            }
        }
    }

    public static class MultiPrimeRequestBuilder {
        private When when;
        private Then then;

        private MultiPrimeRequestBuilder() {
        }

        public static MultiPrimeRequestBuilder multiPrimeRequest() {
            return new MultiPrimeRequestBuilder();
        }

        public MultiPrimeRequestBuilder withWhen(When.Builder when) {
            this.when = when.build();
            return this;
        }

        public MultiPrimeRequestBuilder withThen(Then.Builder then) {
            this.then = then.build();
            return this;
        }

        public MultiPrimeRequest build() {
            return new MultiPrimeRequest(when, then);
        }
    }
}
