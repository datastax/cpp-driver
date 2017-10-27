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

import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.scassandra.server.priming.ErrorConstants;

import java.util.Map;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.scassandra.http.client.PrimingRequest.then;

public class BatchPrimingRequestTest {

    @Test
    public void buildsRequestWithCustomConfiguration() throws Throwable {
        //given
        Map<String, String> expectedConfig = ImmutableMap.of(
                ErrorConstants.ReceivedResponse(), "0",
                ErrorConstants.RequiredResponse(), "1",
                ErrorConstants.WriteType(), WriteTypePrime.BATCH.toString());

        //when
        BatchPrimingRequest batchPrimingRequest = BatchPrimingRequest.batchPrimingRequest()
                .withQueries(BatchQueryPrime.batchQueryPrime()
                        .withKind(BatchQueryKind.query)
                        .withQuery("select * from blah")
                        .build())
                .withThen(then().withConfig(new WriteTimeoutConfig(WriteTypePrime.BATCH, 0, 1)))
                .build();

        //then
        assertEquals(expectedConfig, batchPrimingRequest.getThen().getConfig());
    }

    @Test
    public void returnsThen() {
        //given
        PrimingRequest.Then expectedThen = BatchPrimingRequest.then().build();

        //when
        BatchPrimingRequest batchPrimingRequest = BatchPrimingRequest.batchPrimingRequest()
                .withQueries(BatchQueryPrime.batchQueryPrime()
                        .withKind(BatchQueryKind.query)
                        .withQuery("select * from blah")
                        .build())
                .withThen(then())
                .build();

        //then
        assertEquals(expectedThen, batchPrimingRequest.getThen());
    }

    @Test
    public void returnsWhen() {
        //given
        BatchQueryPrime batchQuery = BatchQueryPrime.batchQueryPrime()
                .withKind(BatchQueryKind.query)
                .withQuery("select * from blah")
                .build();

        //when
        BatchPrimingRequest batchPrimingRequest = BatchPrimingRequest.batchPrimingRequest()
                .withConsistency(Consistency.ALL)
                .withQueries(batchQuery)
                .withType(BatchType.UNLOGGED)
                .withThen(then())
                .build();

        //then
        assertEquals(asList(Consistency.ALL), batchPrimingRequest.getWhen().getConsistency());
        assertEquals(asList(batchQuery), batchPrimingRequest.getWhen().getQueries());
        assertEquals(BatchType.UNLOGGED, batchPrimingRequest.getWhen().getBatchType());
    }
}
