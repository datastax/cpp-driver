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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Test;
import org.scassandra.http.client.ColumnTypes;
import org.scassandra.http.client.PreparedStatementExecution;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.scassandra.http.client.ColumnTypes.*;

public class PreparedStatementMatcherTest {

    @Test
    public void matchWithJustQuery() throws Exception {
        //given
        PreparedStatementExecution actualExecution = PreparedStatementExecution.builder()
                .withPreparedStatementText("some query")
                .build();
        PreparedStatementExecution expectedExecution = PreparedStatementExecution.builder()
                .withPreparedStatementText("some query")
                .build();

        PreparedStatementMatcher underTest = new PreparedStatementMatcher(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(Lists.newArrayList(actualExecution));

        //then
        assertTrue(matched);
    }

    @Test
    public void mismatchingConsistency() throws Exception {
        //given
        PreparedStatementExecution actualExecution = PreparedStatementExecution.builder()
                .withPreparedStatementText("some query")
                .withConsistency("QUORUM")
                .build();
        PreparedStatementExecution expectedExecution = PreparedStatementExecution.builder()
                .withPreparedStatementText("some query")
                .build();

        PreparedStatementMatcher underTest = new PreparedStatementMatcher(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(Lists.newArrayList(actualExecution));

        //then
        assertFalse(matched);
    }

    @Test
    public void mismatchingQueryText() throws Exception {
        //given
        PreparedStatementExecution actualExecution = PreparedStatementExecution.builder()
                .withPreparedStatementText("some different query")
                .build();
        PreparedStatementExecution expectedExecution = PreparedStatementExecution.builder()
                .withPreparedStatementText("some query")
                .build();

        PreparedStatementMatcher underTest = new PreparedStatementMatcher(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(Lists.newArrayList(actualExecution));

        //then
        assertFalse(matched);
    }

    @Test
    public void mismatchingStringVariables() throws Exception {
        //given
        PreparedStatementExecution actualExecution = PreparedStatementExecution.builder(Text, Text, Text)
                .withPreparedStatementText("same query")
                .withVariables("one", "two", "not three!!")
                .build();
        PreparedStatementExecution expectedExecution = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables("one", "two", "three")
                .build();

        PreparedStatementMatcher underTest = new PreparedStatementMatcher(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(Lists.newArrayList(actualExecution));

        //then
        assertFalse(matched);
    }

    @Test
    public void matchingStringVariables() throws Exception {
        //given
        PreparedStatementExecution actualExecution = PreparedStatementExecution.builder(Text, Ascii, Varchar)
                .withPreparedStatementText("same query")
                .withVariables("one", "two", "three")
                .build();
        PreparedStatementExecution expectedExecution = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables("one", "two", "three")
                .build();
        PreparedStatementMatcher underTest = new PreparedStatementMatcher(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(Lists.newArrayList(actualExecution));

        //then
        assertTrue(matched);
    }

    @Test
    public void matchingNumbers() throws Exception {
        //given
        PreparedStatementExecution actualExecution = PreparedStatementExecution.builder(Bigint, Int, Varint, Int)
                .withPreparedStatementText("same query")
                .withVariables(1d, 2d, 3d, 4d)
                .build();
        PreparedStatementExecution expectedExecution = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables("1", 2, new BigInteger("3"), "4")
                .build();

        PreparedStatementMatcher underTest = new PreparedStatementMatcher(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(Lists.newArrayList(actualExecution));

        //then
        assertTrue(matched);
    }

    @Test
    public void matchingNonDecimalTypes() throws Exception {
        //given
        PreparedStatementExecution actualExecution = PreparedStatementExecution.builder(ColumnTypes.Bigint, Counter, Int, Varint)
                .withPreparedStatementText("same query")
                .withVariables(1d, 2d, 3d, 4d)
                .build();
        PreparedStatementExecution expectedExecution = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables(1, 2, 3, "4")
                .build();

        PreparedStatementMatcher underTest = new PreparedStatementMatcher(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(Lists.newArrayList(actualExecution));

        //then
        assertTrue(matched);
    }

    @Test
    public void numbersMatchingFloats() throws Exception {
        //given
        PreparedStatementExecution actualExecution = PreparedStatementExecution.builder(Float, Float, Float, Float)
                .withPreparedStatementText("same query")
                .withVariables("1", "2", "3.44", "4.4440")
                .build();
        PreparedStatementExecution expectedExecution = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables("1", "2", "3.440000", "4.4440")
                .build();

        PreparedStatementMatcher underTest = new PreparedStatementMatcher(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(Lists.newArrayList(actualExecution));

        //then
        assertTrue(matched);
    }

    @Test
    public void matchingBlobsWithByteBuffer() throws Exception {
        //given
        byte[] byteArray = new byte[] {1,2,3,4,5,6,7,8,9,10};
        ByteBuffer byteBuffer = ByteBuffer.wrap(byteArray);
        PreparedStatementExecution actualExecution = PreparedStatementExecution.builder(Blob)
                .withPreparedStatementText("same query")
                .withVariables("0x0102030405060708090a")
                .build();
        PreparedStatementExecution expectedExecution = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables(byteBuffer)
                .build();

        PreparedStatementMatcher underTest = new PreparedStatementMatcher(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(Lists.newArrayList(actualExecution));

        //then
        assertTrue(matched);
    }

    @Test
    public void numbersMatchingBlobsWhenNotInActualReturnsFalse() throws Exception {
        //given
        byte[] byteArray = new byte[] {1,2,3,4,5,6,7,8,9,10};
        ByteBuffer byteBuffer = ByteBuffer.wrap(byteArray);
        PreparedStatementExecution actualExecution = PreparedStatementExecution.builder(Blob)
                .withPreparedStatementText("same query")
                .withVariables(1.0)
                .build();
        PreparedStatementExecution expectedExecution = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables(byteBuffer)
                .build();

        PreparedStatementMatcher underTest = new PreparedStatementMatcher(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(Lists.newArrayList(actualExecution));

        //then
        assertFalse(matched);
    }

    @Test
    public void inetMatching() throws Exception {
        //given
        InetAddress inetAddress = InetAddress.getLocalHost();
        PreparedStatementExecution actualExecution = PreparedStatementExecution.builder(Inet)
                .withPreparedStatementText("same query")
                .withVariables(inetAddress.getHostAddress())
                .build();
        PreparedStatementExecution expectedExecution = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables(inetAddress)
                .build();

        PreparedStatementMatcher underTest = new PreparedStatementMatcher(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(Lists.newArrayList(actualExecution));

        //then
        assertTrue(matched);
    }

    @Test
    public void decimalMatchingAsBigDecimal() throws Exception {
        //given
        BigDecimal decimal = new BigDecimal(90);
        PreparedStatementExecution actualExecution = PreparedStatementExecution.builder(Decimal)
                .withPreparedStatementText("same query")
                .withVariables("90")
                .build();
        PreparedStatementExecution expectedExecution = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables(decimal)
                .build();

        PreparedStatementMatcher underTest = new PreparedStatementMatcher(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(Lists.newArrayList(actualExecution));

        //then
        assertTrue(matched);
    }

    @Test
    public void numbersMatchingUUIDsWithUUIDClass() throws Exception {
        //given
        UUID uuid = UUID.randomUUID();
        UUID theSame = UUID.fromString(uuid.toString());
        PreparedStatementExecution actual = PreparedStatementExecution.builder(Uuid)
                .withPreparedStatementText("same query")
                .withVariables(theSame.toString())
                .build();
        PreparedStatementExecution expected = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables(uuid)
                .build();

        PreparedStatementMatcher underTest = new PreparedStatementMatcher(expected);

        //when
        boolean matched = underTest.matchesSafely(Lists.newArrayList(actual));

        //then
        assertTrue(matched);
    }
    @Test
    public void numbersMatchingUUIDsWithUUIDAsString() throws Exception {
        //given
        UUID uuid = UUID.randomUUID();
        UUID theSame = UUID.fromString(uuid.toString());
        PreparedStatementExecution actual = PreparedStatementExecution.builder(Uuid)
                .withPreparedStatementText("same query")
                .withVariables(theSame.toString())
                .build();
        PreparedStatementExecution expected = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables(uuid)
                .build();

        PreparedStatementMatcher underTest = new PreparedStatementMatcher(expected);

        //when
        boolean matched = underTest.matchesSafely(Lists.newArrayList(actual));

        //then
        assertTrue(matched);
    }

    @Test
    public void matchNonNumberAgainstNumberShouldBeFalse() throws Exception {
        //given
        PreparedStatementExecution actualExecution = PreparedStatementExecution.builder(Text)
                .withPreparedStatementText("same query")
                .withVariables("NaN")
                .build();
        PreparedStatementExecution expectedExecution = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables(1.0)
                .build();

        PreparedStatementMatcher underTest = new PreparedStatementMatcher(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(Lists.newArrayList(actualExecution));

        //then
        assertFalse(matched);
    }

    @Test
    public void lessVariablesIsFalse() throws Exception {
        //given
        PreparedStatementExecution actualExecution = PreparedStatementExecution.builder(Int, Int)
                .withPreparedStatementText("same query")
                .withVariables(1,2)
                .build();
        PreparedStatementExecution expectedExecution = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables(1,2,3)
                .build();

        PreparedStatementMatcher underTest = new PreparedStatementMatcher(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(Lists.newArrayList(actualExecution));

        //then
        assertFalse(matched);
    }

    @Test
    public void moreVariablesIsFalse() throws Exception {
        //given
        PreparedStatementExecution actualExecution = PreparedStatementExecution.builder(Int, Int, Int, Int)
                .withPreparedStatementText("same query")
                .withVariables(1,2, 3, 4)
                .build();
        PreparedStatementExecution expectedExecution = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables(1,2,3)
                .build();

        PreparedStatementMatcher underTest = new PreparedStatementMatcher(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(Lists.newArrayList(actualExecution));

        //then
        assertFalse(matched);
    }

    @Test
    public void matchNullAgainstSomethingElseIsFalse() throws Exception {
        //given

        PreparedStatementExecution actualExecution = PreparedStatementExecution.builder(Int)
                .withPreparedStatementText("same query")
                .withVariables(1.0)
                .build();
        List<Object> variables = new ArrayList<Object>();
        variables.add(null);
        PreparedStatementExecution expectedExecution = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables(variables)
                .build();

        PreparedStatementMatcher underTest = new PreparedStatementMatcher(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(Lists.newArrayList(actualExecution));

        //then
        assertFalse(matched);
    }

    @Test
    public void matchesDateWhenSentBackAsLong() throws Exception {
        //given
        Date date = new Date();
        PreparedStatementExecution actualExecution = PreparedStatementExecution.builder(Timestamp)
                .withPreparedStatementText("same query")
                .withVariables((double) date.getTime())
                .build();
        PreparedStatementExecution expectedExecution = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables(date)
                .build();

        PreparedStatementMatcher underTest = new PreparedStatementMatcher(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(Lists.newArrayList(actualExecution));

        //then
        assertTrue(matched);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentIfVariableListNotTheSameAsNumberOfVariables() throws Exception {
        PreparedStatementExecution actualExecution = PreparedStatementExecution.builder(Timestamp, Timestamp)
                .withPreparedStatementText("same query")
                .withVariables(new Date())
                .build();
        PreparedStatementExecution expectedExecution = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables(new Date())
                .build();

        PreparedStatementMatcher underTest = new PreparedStatementMatcher(expectedExecution);

        //when
        underTest.matchesSafely(Lists.newArrayList(actualExecution));

    }

    @Test
    public void matchingTextSets() throws Exception {
        //given
        Set<String> setOfStrings = Sets.newHashSet("1", "2");
        PreparedStatementExecution nonMatchingActual = PreparedStatementExecution.builder(Double)
                .withPreparedStatementText("same query")
                .withVariables(1d)
                .build();
        PreparedStatementExecution actualExecution = PreparedStatementExecution.builder(TextSet)
                .withPreparedStatementText("same query")
                .withVariables(new ArrayList<String>(setOfStrings))
                .build();
        PreparedStatementExecution expectedExecution = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables(setOfStrings)
                .build();

        PreparedStatementMatcher underTest = new PreparedStatementMatcher(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(Lists.newArrayList(nonMatchingActual, actualExecution));

        //then
        assertTrue(matched);
    }

    @Test
    public void matchingTextLists() throws Exception {
        //given
        List<String> listOfText = Lists.newArrayList("1", "2");
        PreparedStatementExecution nonMatchingActual = PreparedStatementExecution.builder(Double)
                .withPreparedStatementText("same query")
                .withVariables(1d)
                .build();
        PreparedStatementExecution actualExecution = PreparedStatementExecution.builder(TextList)
                .withPreparedStatementText("same query")
                .withVariables(new ArrayList<String>(listOfText))
                .build();
        PreparedStatementExecution expectedExecution = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables(listOfText)
                .build();

        PreparedStatementMatcher underTest = new PreparedStatementMatcher(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(Lists.newArrayList(actualExecution, nonMatchingActual));

        //then
        assertTrue(matched);
    }
    
    @Test
    public void matchingTextMaps() throws Exception {
        //given
        Map<String, String> map = ImmutableMap.of("one", "1");
        PreparedStatementExecution actualExecution = PreparedStatementExecution.builder(TextTextMap)
                .withPreparedStatementText("same query")
                .withVariables(map)
                .build();
        PreparedStatementExecution nonMatchingActual = PreparedStatementExecution.builder(Double)
                .withPreparedStatementText("same query")
                .withVariables(1d)
                .build();
        PreparedStatementExecution expectedExecution = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables(map)
                .build();

        PreparedStatementMatcher underTest = new PreparedStatementMatcher(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(Lists.newArrayList(actualExecution, nonMatchingActual));

        //then
        assertTrue(matched);
    }
}