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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Test;
import org.scassandra.http.client.Query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.scassandra.cql.PrimitiveType.*;
import static org.scassandra.cql.SetType.set;

public class QueryMatcherTest {

    @Test
    public void matchesOnQuery() throws Exception {
        Query queryToMatchAgainst = Query.builder()
                .withQuery("the query")
                .build();

        Query queryWithSameText = Query.builder()
                .withQuery("the query")
                .build();

        QueryMatcher underTest = new QueryMatcher(queryToMatchAgainst);

        boolean matched = underTest.matchesSafely(Arrays.asList(queryWithSameText));

        assertTrue(matched);
    }

    @Test
    public void mismatchingConsistency() throws Exception {
        //given
        Query actualExecution = Query.builder()
                .withQuery("some query")
                .withConsistency("QUORUM")
                .build();
        Query expectedExecution = Query.builder()
                .withQuery("some query")
                .build();

        QueryMatcher underTest = new QueryMatcher(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(Lists.newArrayList(actualExecution));

        //then
        assertFalse(matched);
    }

    @Test
    public void matchingStringVariables() throws Exception {
        //given
        Query actualExecution = Query.builder(TEXT, ASCII, VARCHAR)
                .withQuery("same query")
                .withVariables("one", "two", "three")
                .build();
        Query expectedExecution = Query.builder()
                .withQuery("same query")
                .withVariables("one", "two", "three")
                .build();
        QueryMatcher underTest = new QueryMatcher(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(Lists.newArrayList(actualExecution));

        //then
        assertTrue(matched);
    }

    @Test
    public void matchingTextSets() throws Exception {
        //given
        Set<String> setOfStrings = Sets.newHashSet("1", "2");
        Query nonMatchingActual = Query.builder(DOUBLE)
                .withQuery("same query")
                .withVariables(1d)
                .build();
        Query actualExecution = Query.builder(set(TEXT))
                .withQuery("same query")
                .withVariables(new ArrayList<String>(setOfStrings))
                .build();
        Query expectedExecution = Query.builder()
                .withQuery("same query")
                .withVariables(setOfStrings)
                .build();

        QueryMatcher underTest = new QueryMatcher(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(Lists.newArrayList(nonMatchingActual, actualExecution));

        //then
        assertTrue(matched);
    }
}