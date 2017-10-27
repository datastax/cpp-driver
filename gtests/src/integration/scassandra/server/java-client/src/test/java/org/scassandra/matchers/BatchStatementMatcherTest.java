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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.junit.Test;
import org.scassandra.http.client.BatchExecution;
import org.scassandra.http.client.BatchQuery;
import org.scassandra.http.client.BatchType;
import org.scassandra.http.client.Consistency;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.UUID;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.UUID.fromString;
import static java.util.UUID.randomUUID;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.scassandra.cql.ListType.list;
import static org.scassandra.cql.MapType.map;
import static org.scassandra.cql.PrimitiveType.*;
import static org.scassandra.cql.SetType.set;
import static org.scassandra.http.client.BatchQueryKind.prepared_statement;
import static org.scassandra.http.client.BatchQueryKind.query;
import static org.scassandra.http.client.BatchType.LOGGED;
import static org.scassandra.http.client.BatchType.UNLOGGED;
import static org.scassandra.matchers.Matchers.batchExecutionRecorded;

public class BatchStatementMatcherTest {

    private final BatchQuery unmatchingBatchQuery = BatchQuery.builder()
            .withQuery("some query that does not match")
            .withType(query)
            .withVariableTypes(SMALL_INT)
            .withVariables(1)
            .build();

    private final BatchExecution unmatchingBatchExecution = BatchExecution.builder()
            .withBatchQueries(unmatchingBatchQuery)
            .withBatchType(BatchType.COUNTER)
            .withConsistency(Consistency.ALL.name())
            .build();

    @Test
    public void matchBatchType() throws Exception {
        //given
        BatchExecution actualExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder().withQuery("some query").build(), unmatchingBatchQuery)
                .withBatchType(LOGGED)
                .build();

        BatchExecution expectedExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder().withQuery("some query").build())
                .withBatchType(LOGGED)
                .build();

        BatchExecutionMatcher underTest = batchExecutionRecorded(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(newArrayList(actualExecution, unmatchingBatchExecution));

        //then
        assertTrue(matched);
    }

    @Test
    public void mismatchBatchType() throws Exception {
        //given
        BatchExecution actualExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder().withQuery("some query").build(), unmatchingBatchQuery)
                .withBatchType(LOGGED)
                .build();

        BatchExecution expectedExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder().withQuery("some query").build())
                .withBatchType(UNLOGGED)
                .build();

        BatchExecutionMatcher underTest = batchExecutionRecorded(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(newArrayList(actualExecution, unmatchingBatchExecution));

        //then
        assertFalse(matched);
    }

    @Test
    public void matchBatchQueryType() throws Exception {
        //given
        BatchExecution actualExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder()
                                .withType(prepared_statement)
                                .withQuery("some query")
                                .build(),
                        unmatchingBatchQuery)
                .build();

        BatchExecution expectedExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder()
                        .withType(prepared_statement)
                        .withQuery("some query")
                        .build())
                .build();

        BatchExecutionMatcher underTest = batchExecutionRecorded(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(newArrayList(actualExecution, unmatchingBatchExecution));

        //then
        assertTrue(matched);
    }

    @Test
    public void mismatchingBatchQueryType() throws Exception {
        //given
        BatchExecution actualExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder()
                                .withType(query)
                                .withQuery("some query")
                                .build(),
                        unmatchingBatchQuery)
                .build();

        BatchExecution expectedExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder()
                        .withType(prepared_statement)
                        .withQuery("some query")
                        .build())
                .build();

        BatchExecutionMatcher underTest = batchExecutionRecorded(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(newArrayList(actualExecution, unmatchingBatchExecution));

        //then
        assertFalse(matched);
    }

    @Test
    public void matchWithJustQuery() throws Exception {
        //given
        BatchExecution actualExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder().withQuery("some query").build(), unmatchingBatchQuery)
                .build();

        BatchExecution expectedExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder().withQuery("some query").build())
                .build();

        BatchExecutionMatcher underTest = batchExecutionRecorded(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(newArrayList(actualExecution, unmatchingBatchExecution));

        //then
        assertTrue(matched);
    }

    @Test
    public void mismatchingConsistency() throws Exception {
        //given
        BatchExecution actualExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder().withQuery("some query").build(), unmatchingBatchQuery)
                .withConsistency("QUORUM")
                .build();

        BatchExecution expectedExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder().withQuery("some query").build())
                .build();

        BatchExecutionMatcher underTest = batchExecutionRecorded(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(newArrayList(actualExecution, unmatchingBatchExecution));

        //then
        assertFalse(matched);
    }

    @Test
    public void mismatchingQueryText() throws Exception {
        //given
        BatchExecution actualExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder().withQuery("some peculiar query").build(), unmatchingBatchQuery)
                .build();

        BatchExecution expectedExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder().withQuery("some query").build())
                .build();

        BatchExecutionMatcher underTest = batchExecutionRecorded(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(newArrayList(actualExecution, unmatchingBatchExecution));

        //then
        assertFalse(matched);
    }

    @Test
    public void mismatchingStringVariables() throws Exception {
        //given
        BatchExecution actualExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder()
                                .withQuery("same query")
                                .withVariableTypes(TEXT, TEXT, TEXT)
                                .withVariables("one", "two", "not three!!")
                                .build(),
                        unmatchingBatchQuery)
                .build();

        BatchExecution expectedExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder()
                        .withQuery("same query")
                        .withVariables("one", "two", "three")
                        .build())
                .build();

        BatchExecutionMatcher underTest = batchExecutionRecorded(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(newArrayList(actualExecution, unmatchingBatchExecution));

        //then
        assertFalse(matched);
    }

    @Test
    public void matchingStringVariables() throws Exception {
        //given
        BatchExecution actualExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder()
                                .withQuery("same query")
                                .withVariableTypes(TEXT, ASCII, VARCHAR)
                                .withVariables("one", "two", "three")
                                .build(),
                        unmatchingBatchQuery)
                .build();

        BatchExecution expectedExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder()
                        .withQuery("same query")
                        .withVariables("one", "two", "three")
                        .build())
                .build();

        BatchExecutionMatcher underTest = batchExecutionRecorded(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(newArrayList(actualExecution, unmatchingBatchExecution));

        //then
        assertTrue(matched);
    }

    @Test
    public void matchingNumbers() throws Exception {
        //given
        BatchExecution actualExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder()
                                .withQuery("same query")
                                .withVariableTypes(BIG_INT, INT, VAR_INT, INT)
                                .withVariables(1d, 2d, 3d, 4d)
                                .build(),
                        unmatchingBatchQuery)
                .build();

        BatchExecution expectedExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder()
                        .withQuery("same query")
                        .withVariables("1", 2, new BigInteger("3"), "4")
                        .build())
                .build();

        BatchExecutionMatcher underTest = batchExecutionRecorded(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(newArrayList(actualExecution, unmatchingBatchExecution));

        //then
        assertTrue(matched);
    }

    @Test
    public void matchingNonDecimalTypes() throws Exception {
        //given
        BatchExecution actualExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder()
                                .withQuery("same query")
                                .withVariableTypes(BIG_INT, COUNTER, INT, VAR_INT)
                                .withVariables(1d, 2d, 3d, 4d)
                                .build(),
                        unmatchingBatchQuery)
                .build();

        BatchExecution expectedExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder()
                        .withQuery("same query")
                        .withVariables(1, 2, 3, "4")
                        .build())
                .build();

        BatchExecutionMatcher underTest = batchExecutionRecorded(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(newArrayList(actualExecution, unmatchingBatchExecution));

        //then
        assertTrue(matched);
    }

    @Test
    public void numbersMatchingFloats() throws Exception {
        //given
        BatchExecution actualExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder()
                        .withQuery("same query")
                        .withVariableTypes(FLOAT, FLOAT, FLOAT, FLOAT)
                        .withVariables("1", "2", "3.44", "4.4440")
                        .build(),
                        unmatchingBatchQuery)
                .build();

        BatchExecution expectedExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder()
                        .withQuery("same query")
                        .withVariables("1", "2", "3.440000", "4.4440")
                        .build())
                .build();

        BatchExecutionMatcher underTest = batchExecutionRecorded(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(newArrayList(actualExecution, unmatchingBatchExecution));

        //then
        assertTrue(matched);
    }

    @Test
    public void matchingBlobsWithByteBuffer() throws Exception {
        //given
        byte[] byteArray = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        ByteBuffer byteBuffer = ByteBuffer.wrap(byteArray);

        BatchExecution actualExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder()
                        .withQuery("same query")
                        .withVariableTypes(BLOB)
                        .withVariables("0x0102030405060708090a")
                        .build(),
                        unmatchingBatchQuery)
                .build();

        BatchExecution expectedExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder()
                        .withQuery("same query")
                        .withVariables(byteBuffer)
                        .build())
                .build();

        BatchExecutionMatcher underTest = batchExecutionRecorded(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(newArrayList(actualExecution, unmatchingBatchExecution));

        //then
        assertTrue(matched);
    }

    @Test
    public void numbersMatchingBlobsWhenNotInActualReturnsFalse() throws Exception {
        //given
        byte[] byteArray = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        ByteBuffer byteBuffer = ByteBuffer.wrap(byteArray);

        BatchExecution actualExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder()
                        .withQuery("same query")
                        .withVariableTypes(BLOB)
                        .withVariables("1.0")
                        .build(),
                        unmatchingBatchQuery)
                .build();

        BatchExecution expectedExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder()
                        .withQuery("same query")
                        .withVariables(byteBuffer)
                        .build())
                .build();

        BatchExecutionMatcher underTest = batchExecutionRecorded(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(newArrayList(actualExecution, unmatchingBatchExecution));

        //then
        assertFalse(matched);
    }

    @Test
    public void inetMatching() throws Exception {
        //given
        InetAddress inetAddress = InetAddress.getLocalHost();

        BatchExecution actualExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder()
                        .withQuery("same query")
                        .withVariableTypes(INET)
                        .withVariables(inetAddress.getHostAddress())
                        .build(),
                        unmatchingBatchQuery)
                .build();

        BatchExecution expectedExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder()
                        .withQuery("same query")
                        .withVariables(inetAddress)
                        .build())
                .build();

        BatchExecutionMatcher underTest = batchExecutionRecorded(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(newArrayList(actualExecution, unmatchingBatchExecution));

        //then
        assertTrue(matched);
    }

    @Test
    public void decimalMatchingAsBigDecimal() throws Exception {
        //given
        BigDecimal decimal = new BigDecimal(90);

        BatchExecution actualExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder()
                        .withQuery("same query")
                        .withVariableTypes(DECIMAL)
                        .withVariables("90")
                        .build(),
                        unmatchingBatchQuery)
                .build();

        BatchExecution expectedExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder()
                        .withQuery("same query")
                        .withVariables(decimal)
                        .build())
                .build();

        BatchExecutionMatcher underTest = batchExecutionRecorded(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(newArrayList(actualExecution, unmatchingBatchExecution));

        //then
        assertTrue(matched);
    }

    @Test
    public void numbersMatchingUUIDsWithUUIDClass() throws Exception {
        //given
        UUID uuid = randomUUID();
        UUID theSame = fromString(uuid.toString());

        BatchExecution actualExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder()
                        .withQuery("same query")
                        .withVariableTypes(UUID)
                        .withVariables(theSame.toString())
                        .build(),
                        unmatchingBatchQuery)
                .build();

        BatchExecution expectedExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder()
                        .withQuery("same query")
                        .withVariables(uuid)
                        .build())
                .build();

        BatchExecutionMatcher underTest = batchExecutionRecorded(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(newArrayList(actualExecution, unmatchingBatchExecution));

        //then
        assertTrue(matched);
    }

    @Test
    public void numbersMatchingUUIDsWithUUIDString() throws Exception {
        //given
        UUID uuid = randomUUID();
        UUID theSame = fromString(uuid.toString());

        BatchExecution actualExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder()
                                .withQuery("same query")
                                .withVariableTypes(UUID)
                                .withVariables(theSame.toString())
                                .build(),
                        unmatchingBatchQuery)
                .build();

        BatchExecution expectedExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder()
                        .withQuery("same query")
                        .withVariables(uuid.toString())
                        .build())
                .build();

        BatchExecutionMatcher underTest = batchExecutionRecorded(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(newArrayList(actualExecution, unmatchingBatchExecution));

        //then
        assertTrue(matched);
    }

    @Test
    public void matchNonNumberAgainstNumberShouldBeFalse() throws Exception {
        //given
        BatchExecution actualExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder()
                        .withQuery("same query")
                        .withVariableTypes(TEXT)
                        .withVariables("NaN")
                        .build(),
                        unmatchingBatchQuery)
                .build();

        BatchExecution expectedExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder()
                        .withQuery("same query")
                        .withVariables(1.0)
                        .build())
                .build();

        BatchExecutionMatcher underTest = batchExecutionRecorded(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(newArrayList(actualExecution, unmatchingBatchExecution));

        //then
        assertFalse(matched);
    }

    @Test
    public void lessVariablesIsFalse() throws Exception {
        //given
        BatchExecution actualExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder()
                        .withQuery("same query")
                        .withVariableTypes(INT, INT)
                        .withVariables(1, 2)
                        .build(),
                        unmatchingBatchQuery)
                .build();

        BatchExecution expectedExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder()
                        .withQuery("same query")
                        .withVariables(1, 2, 3)
                        .build())
                .build();

        BatchExecutionMatcher underTest = batchExecutionRecorded(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(newArrayList(actualExecution, unmatchingBatchExecution));

        //then
        assertFalse(matched);
    }

    @Test
    public void moreVariablesIsFalse() throws Exception {
        //given
        BatchExecution actualExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder()
                        .withQuery("same query")
                        .withVariableTypes(INT, INT, INT, INT)
                        .withVariables(1, 2, 3, 4)
                        .build(),
                        unmatchingBatchQuery)
                .build();

        BatchExecution expectedExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder()
                        .withQuery("same query")
                        .withVariables(1, 2, 3)
                        .build())
                .build();

        BatchExecutionMatcher underTest = batchExecutionRecorded(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(newArrayList(actualExecution, unmatchingBatchExecution));

        //then
        assertFalse(matched);
    }

    @Test
    public void matchNullAgainstSomethingElseIsFalse() throws Exception {
        //given
        BatchExecution actualExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder()
                        .withQuery("same query")
                        .withVariables(1.0)
                        .withVariableTypes(INT)
                        .build(),
                        unmatchingBatchQuery)
                .build();

        BatchExecution expectedExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder()
                        .withQuery("same query")
                        .withVariables(new Object[]{null})
                        .build())
                .build();

        BatchExecutionMatcher underTest = batchExecutionRecorded(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(newArrayList(actualExecution, unmatchingBatchExecution));

        //then
        assertFalse(matched);
    }

    @Test
    public void matchesDateWhenSentBackAsLong() throws Exception {
        //given
        Date date = new Date();

        BatchExecution actualExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder()
                        .withQuery("same query")
                        .withVariableTypes(TIMESTAMP)
                        .withVariables((double) date.getTime())
                        .build(),
                        unmatchingBatchQuery)
                .build();

        BatchExecution expectedExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder()
                        .withQuery("same query")
                        .withVariables(date)
                        .build())
                .build();

        BatchExecutionMatcher underTest = batchExecutionRecorded(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(newArrayList(actualExecution, unmatchingBatchExecution));

        //then
        assertTrue(matched);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentIfVariableListNotTheSameAsNumberOfVariables() throws Exception {
        BatchExecution actualExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder()
                        .withQuery("same query")
                        .withVariableTypes(TIMESTAMP, TIMESTAMP)
                        .withVariables(new Date())
                        .build(),
                        unmatchingBatchQuery)
                .build();

        BatchExecution expectedExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder()
                        .withQuery("same query")
                        .withVariables(new Date())
                        .build())
                .build();

        BatchExecutionMatcher underTest = batchExecutionRecorded(expectedExecution);

        //when
        underTest.matchesSafely(newArrayList(unmatchingBatchExecution, actualExecution));
    }

    @Test
    public void matchingTextSets() throws Exception {
        //given
        Set<String> setOfStrings = Sets.newHashSet("1", "2");

        BatchExecution actualExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder()
                        .withQuery("same query")
                        .withVariables(new ArrayList<String>(setOfStrings))
                        .withVariableTypes(set(TEXT))
                        .build(),
                        unmatchingBatchQuery)
                .build();

        BatchExecution expectedExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder()
                        .withQuery("same query")
                        .withVariables(setOfStrings)
                        .build())
                .build();


        BatchExecutionMatcher underTest = batchExecutionRecorded(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(newArrayList(actualExecution, unmatchingBatchExecution));

        //then
        assertTrue(matched);
    }

    @Test
    public void matchingTextLists() throws Exception {
        //given
        List<String> listOfText = newArrayList("1", "2");

        BatchExecution actualExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder()
                        .withQuery("same query")
                        .withVariables(new ArrayList<String>(listOfText))
                        .withVariableTypes(list(TEXT))
                        .build(),
                        unmatchingBatchQuery)
                .build();

        BatchExecution expectedExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder()
                        .withQuery("same query")
                        .withVariables(new ArrayList<String>(listOfText))
                        .build())
                .build();

        BatchExecutionMatcher underTest = batchExecutionRecorded(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(newArrayList(actualExecution, unmatchingBatchExecution));

        //then
        assertTrue(matched);
    }

    @Test
    public void matchingTextMaps() throws Exception {
        //given
        Map<String, String> mapOfTextText = ImmutableMap.of("one", "1");

        BatchExecution actualExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder()
                        .withQuery("same query")
                        .withVariables(mapOfTextText)
                        .withVariableTypes(map(TEXT, TEXT))
                        .build(),
                        unmatchingBatchQuery)
                .build();

        BatchExecution expectedExecution = BatchExecution.builder()
                .withBatchQueries(BatchQuery.builder()
                        .withQuery("same query")
                        .withVariables(mapOfTextText)
                        .build())
                .build();

        BatchExecutionMatcher underTest = batchExecutionRecorded(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(newArrayList(actualExecution, unmatchingBatchExecution));

        //then
        assertTrue(matched);
    }
}
