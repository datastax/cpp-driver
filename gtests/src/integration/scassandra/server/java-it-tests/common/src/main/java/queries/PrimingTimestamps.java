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
import org.scassandra.http.client.ColumnTypes;
import org.scassandra.http.client.PrimingRequest;

import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class PrimingTimestamps extends AbstractScassandraTest {

    public PrimingTimestamps(CassandraExecutor cassandraExecutor) {
        super(cassandraExecutor);
    }

    @Test
    public void testPrimeWithRows() {
        String query = "select * from people";
        long now = System.currentTimeMillis();
        String nowAsString = String.valueOf(System.currentTimeMillis());
        Date nowAsDate = new Date(now);
        Map<String, ? extends Object> row = ImmutableMap.of(
                "atimestamp", new Long(nowAsDate.getTime()),
                "atimestamp2", nowAsString
        );
        Map<String, ColumnTypes> types = ImmutableMap.of(
                "atimestamp", ColumnTypes.Timestamp,
                "atimestamp2", ColumnTypes.Timestamp);
        PrimingRequest prime = PrimingRequest.queryBuilder()
                .withQuery(query)
                .withRows(row)
                .withColumnTypes(types)
                .build();
        primingClient.primeQuery(prime);

        CassandraResult result = cassandra().executeQuery(query);

        List<CassandraRow> allRows = result.rows();
        assertEquals(1, allRows.size());
        assertEquals(nowAsDate, allRows.get(0).getTimestamp("atimestamp"));
        assertEquals(new Date(Long.valueOf(nowAsString)), allRows.get(0).getTimestamp("atimestamp2"));
    }
}
