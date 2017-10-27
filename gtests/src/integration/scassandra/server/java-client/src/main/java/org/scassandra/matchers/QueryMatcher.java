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
package org.scassandra.matchers;

import org.hamcrest.Description;
import org.scassandra.cql.CqlType;
import org.scassandra.http.client.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

class QueryMatcher extends ScassandraMatcher<List<Query>, Query> {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryMatcher.class);

    private Query expectedQuery;

    public  QueryMatcher(Query expectedQuery) {
        if (expectedQuery == null) throw new IllegalArgumentException("null query");
        this.expectedQuery = expectedQuery;
    }

    @Override
    protected boolean match(Query actualQuery) {
        if (!actualQuery.getConsistency().equals(expectedQuery.getConsistency()))
            return false;
        if (!actualQuery.getQuery().equals(expectedQuery.getQuery()))
            return false;
        List<Object> expectedVariables = expectedQuery.getVariables();

        List<CqlType> variableTypes = actualQuery.getVariableTypes();
        List<Object> actualVariables = actualQuery.getVariables();
        return checkVariables(expectedVariables, variableTypes, actualVariables);
    }


    @Override
    public void describeMismatchSafely(List<Query> actual, Description description) {
        description.appendText("the following queries were executed: ");
        for (Query query : actual) {
            description.appendText("\n" + query);
        }
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("Expected query " + expectedQuery + " to be executed");
    }
}