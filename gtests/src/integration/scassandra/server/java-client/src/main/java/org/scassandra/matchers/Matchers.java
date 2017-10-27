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

import org.scassandra.http.client.BatchExecution;
import org.scassandra.http.client.PreparedStatementExecution;
import org.scassandra.http.client.Query;

public class Matchers {

    public static QueryMatcher containsQuery(Query query) {
        return new QueryMatcher(query);
    }

    public static PreparedStatementMatcher preparedStatementRecorded(PreparedStatementExecution query) {
        return new PreparedStatementMatcher(query);
    }

    public static BatchExecutionMatcher batchExecutionRecorded(BatchExecution batchExecution) {
        return new BatchExecutionMatcher(batchExecution);
    }

}
