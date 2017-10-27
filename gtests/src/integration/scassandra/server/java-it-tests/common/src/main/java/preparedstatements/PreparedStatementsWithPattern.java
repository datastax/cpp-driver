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

import com.google.common.collect.ImmutableMap;
import common.AbstractScassandraTest;
import common.CassandraExecutor;
import common.CassandraResult;
import common.CassandraRow;
import org.junit.Test;
import org.scassandra.http.client.PrimingRequest;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

abstract public class PreparedStatementsWithPattern extends AbstractScassandraTest {

    public PreparedStatementsWithPattern(CassandraExecutor cassandraExecutor) {
        super(cassandraExecutor);
    }

    @Test
    public void primingWithADotStar() throws Exception {
        //given
        Map<String, String> row = ImmutableMap.of("name", "Chris");
        primingClient.primePreparedStatement(PrimingRequest.preparedStatementBuilder()
                .withQueryPattern(".*")
                .withRows(row)
                .build());

        //when
        CassandraResult results = cassandra().prepareAndExecute("select * from people where name = ?", "Chris");

        //then
        List<CassandraRow> asList = results.rows();
        assertEquals(1, asList.size());
        assertEquals("Chris", asList.get(0).getString("name"));
    }

}
