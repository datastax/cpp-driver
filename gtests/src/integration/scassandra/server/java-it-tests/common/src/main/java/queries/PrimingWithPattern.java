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
package queries;

import com.google.common.collect.ImmutableMap;
import common.AbstractScassandraTest;
import common.CassandraExecutor;
import common.CassandraResult;
import common.CassandraRow;
import org.junit.Test;
import org.scassandra.http.client.PrimingRequest;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.scassandra.http.client.PrimingRequest.then;

public class PrimingWithPattern extends AbstractScassandraTest {

    public PrimingWithPattern(CassandraExecutor cassandraExecutor) {
        super(cassandraExecutor);
    }

    @Test
    public void testQueryPatternWithRegexAtEnd() throws Exception {
        //given
        PrimingRequest primingRequest = PrimingRequest.queryBuilder()
                .withQueryPattern("select name from people where name = .*")
                .withThen(then().withRows(ImmutableMap.of("name", "Chris")).build())
                .build();
        primingClient.prime(primingRequest);

        //when
        CassandraResult results = cassandra().executeQuery("select name from people where name = 'Chris'");

        //then
        List<CassandraRow> rows = results.rows();
        assertEquals(1, rows.size());
        assertEquals("Chris", rows.get(0).getString("name"));
    }

    @Test
    public void testQueryPatternWithRegexForColumnNames() throws Exception {
        //given
        PrimingRequest primingRequest = PrimingRequest.queryBuilder()
                .withQueryPattern("select .*? from people where name = 'Chris'")
                .withRows(ImmutableMap.of("name", "Chris", "age", "29"))
                .build();
        primingClient.primeQuery(primingRequest);

        //when
        CassandraResult results = cassandra().executeQuery("select name, age from people where name = 'Chris'");

        //then
        List<CassandraRow> rows = results.rows();
        assertEquals(1, rows.size());
        assertEquals("Chris", rows.get(0).getString("name"));
        assertEquals("29", rows.get(0).getString("age"));
    }
}
