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

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Test;

public class MultiPrimeRequestTest {

    @Test
    public void testEqualsContract() {
        EqualsVerifier.forClass(MultiPrimeRequest.class).allFieldsShouldBeUsed().verify();
        EqualsVerifier.forClass(MultiPrimeRequest.Action.class).allFieldsShouldBeUsed().verify();
        EqualsVerifier.forClass(MultiPrimeRequest.Criteria.class).allFieldsShouldBeUsed().verify();
        EqualsVerifier.forClass(MultiPrimeRequest.When.class).allFieldsShouldBeUsed().verify();
        EqualsVerifier.forClass(MultiPrimeRequest.Then.class).allFieldsShouldBeUsed().verify();
        EqualsVerifier.forClass(MultiPrimeRequest.ExactMatch.class).allFieldsShouldBeUsedExcept("type").withRedefinedSuperclass().verify();
    }
}