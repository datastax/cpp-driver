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
package preparedstatements;

import common.AbstractScassandraTest;
import common.CassandraExecutor;
import org.junit.Test;
import org.scassandra.http.client.PreparedStatementPreparation;

import java.util.List;

import static org.junit.Assert.assertEquals;

abstract public class PreparedStatementPreparationTest extends AbstractScassandraTest {

    public PreparedStatementPreparationTest(CassandraExecutor cassandraExecutor) {
        super(cassandraExecutor);
    }

    @Test
    public void activityVerificationForPreparedPrimesThatAreExecuted() {
        //given
        String preparedStatementText = "select * from people where name = ?";

        //when
        cassandra().prepareAndExecute(preparedStatementText, "Chris");

        //then
        List<PreparedStatementPreparation> preparations = activityClient.retrievePreparedStatementPreparations();
        assertEquals(1, preparations.size());
        PreparedStatementPreparation preparation = preparations.get(0);
        assertEquals(preparedStatementText, preparation.getPreparedStatementText());
    }


    @Test
    public void activityVerificationForPreparedPrimesThatAreNotExecuted() {
        //given
        String preparedStatementText = "select * from people where name = ?";

        //when
        cassandra().prepare(preparedStatementText);

        //then
        List<PreparedStatementPreparation> preparations = activityClient.retrievePreparedStatementPreparations();
        assertEquals(1, preparations.size());
        PreparedStatementPreparation preparation = preparations.get(0);
        assertEquals(preparedStatementText, preparation.getPreparedStatementText());
    }

    @Test
    public void clearingRecordedActivityForPreparedPrimes() {
        //given
        String preparedStatementText = "select * from people where name = ?";

        //when
        cassandra().prepare(preparedStatementText);

        //then
        activityClient.clearPreparedStatementPreparations();
        List<PreparedStatementPreparation> preparations = activityClient.retrievePreparedStatementPreparations();
        assertEquals(0, preparations.size());
    }

}
