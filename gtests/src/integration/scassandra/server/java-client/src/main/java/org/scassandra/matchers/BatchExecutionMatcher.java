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
import org.scassandra.http.client.BatchExecution;
import org.scassandra.http.client.BatchQuery;

import java.util.List;

public class BatchExecutionMatcher extends ScassandraMatcher<List<BatchExecution>, BatchExecution> {

    private final BatchExecution expectedBatchExecution;

    BatchExecutionMatcher(BatchExecution expectedBatchExecution) {
        if (expectedBatchExecution == null)
            throw new IllegalArgumentException("null expectedBatchExecution");
        this.expectedBatchExecution = expectedBatchExecution;
    }

    @Override
    public void describeMismatchSafely(List<BatchExecution> batchExecutions, Description description) {
        description.appendText("the following batches were executed: ");
        for (BatchExecution batchExecution : batchExecutions) {
            description.appendText("\n" + batchExecution);
        }
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("Expected batch " + expectedBatchExecution + " to be executed");
    }

    @Override
    protected boolean match(BatchExecution actualBatchExecution) {
        if (!actualBatchExecution.getConsistency().equals(expectedBatchExecution.getConsistency())) {
            return false;
        }

        if (!actualBatchExecution.getBatchType().equals(expectedBatchExecution.getBatchType())) {
            return false;
        }

        List<BatchQuery> expectedBatchQueries = expectedBatchExecution.getBatchQueries();
        List<BatchQuery> actualBatchQueries = actualBatchExecution.getBatchQueries();

        for (int i = 0; i < expectedBatchExecution.getBatchQueries().size(); i++) {
            BatchQuery expectedBatchQuery = expectedBatchQueries.get(0);
            BatchQuery actualBatchQuery = actualBatchQueries.get(0);

            if (!expectedBatchQuery.getQuery().equals(actualBatchQuery.getQuery())) {
                return false;
            }

            if (!expectedBatchQuery.getBatchQueryKind().equals(actualBatchQuery.getBatchQueryKind())) {
                return false;
            }

            List<Object> expectedVariables = expectedBatchQuery.getVariables();
            List<CqlType> actualVariableTypes = actualBatchQuery.getVariableTypes();
            List<Object> actualVariables = actualBatchQuery.getVariables();

            if (!checkVariables(expectedVariables, actualVariableTypes, actualVariables)) {
                return false;
            }
        }

        return true;
    }
}
