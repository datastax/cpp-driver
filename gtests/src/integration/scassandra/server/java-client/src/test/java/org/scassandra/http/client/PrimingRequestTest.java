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

import org.junit.Test;
import nl.jqno.equalsverifier.EqualsVerifier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class PrimingRequestTest {
    @Test
    public void throwsIllegalStateExceptionIfNoQuerySpecified() {
        //given
        //when
        try {
            PrimingRequest.queryBuilder()
                    .build();
            fail("Expected illegal state exception");
        } catch (IllegalStateException e) {
            //then
            assertEquals("Must set either query or queryPattern for PrimingRequest", e.getMessage());
        }
    }

    @Test
    public void throwsIllegalStateExceptionIfQueryAndPatternSpecified() {
        //given
        //when
        try {
            PrimingRequest.queryBuilder()
                    .withQuery("select something")
                    .withQueryPattern("select something where name = .*")
                    .build();
            fail("Expected illegal state exception");
        } catch (IllegalStateException e) {
            //then
            assertEquals("Can't specify query and queryPattern", e.getMessage());
        }
    }

    @Test
    public void testEqualsContract() {
        EqualsVerifier.forClass(PrimingRequest.When.class).allFieldsShouldBeUsed().verify();
        EqualsVerifier.forClass(PrimingRequest.Then.class).allFieldsShouldBeUsed().verify();
        EqualsVerifier.forClass(PrimingRequest.class).allFieldsShouldBeUsed().verify();
    }
}
