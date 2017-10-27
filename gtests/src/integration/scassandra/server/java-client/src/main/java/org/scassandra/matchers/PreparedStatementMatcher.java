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
import org.scassandra.http.client.PreparedStatementExecution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PreparedStatementMatcher extends ScassandraMatcher<List<PreparedStatementExecution>, PreparedStatementExecution> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PreparedStatementMatcher.class);

    private PreparedStatementExecution expectedPreparedStatementExecution;

    public PreparedStatementMatcher(PreparedStatementExecution expectedPreparedStatementExecution) {
        if (expectedPreparedStatementExecution == null)
            throw new IllegalArgumentException("null expectedPreparedStatementExecution");
        this.expectedPreparedStatementExecution = expectedPreparedStatementExecution;
    }

    @Override
    public void describeMismatchSafely(List<PreparedStatementExecution> preparedStatementExecutions, Description description) {
        description.appendText("the following prepared statements were executed: ");
        for (PreparedStatementExecution preparedStatement : preparedStatementExecutions) {
            description.appendText("\n" + preparedStatement);
        }
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("Expected prepared statement " + expectedPreparedStatementExecution + " to be executed");
    }

    /*
    The server sends back all floats and doubles as strings to preserve accuracy so we convert the
    actual variable to the expected variables type
     */
    @Override
    protected boolean match(PreparedStatementExecution actualPreparedStatementExecution) {
        List<CqlType> variableTypes = actualPreparedStatementExecution.getVariableTypes();
        List<Object> actualVariables = actualPreparedStatementExecution.getVariables();

        if (!actualPreparedStatementExecution.getConsistency().equals(expectedPreparedStatementExecution.getConsistency()))
            return false;
        if (!actualPreparedStatementExecution.getPreparedStatementText().equals(expectedPreparedStatementExecution.getPreparedStatementText()))
            return false;
        List<Object> expectedVariables = expectedPreparedStatementExecution.getVariables();

        return checkVariables(expectedVariables, variableTypes, actualVariables);
    }
}
