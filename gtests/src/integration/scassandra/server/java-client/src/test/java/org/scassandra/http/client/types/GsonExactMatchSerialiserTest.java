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
package org.scassandra.http.client.types;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Test;
import org.scassandra.http.client.MultiPrimeRequest;

import static org.junit.Assert.assertEquals;

public class GsonExactMatchSerialiserTest {

    private final Gson gson = new GsonBuilder()
            .registerTypeAdapter(MultiPrimeRequest.VariableMatch.class, new GsonVariableMatchDeserialiser())
            .registerTypeAdapter(MultiPrimeRequest.ExactMatch.class, new GsonExactMatchSerialiser())
            .enableComplexMapKeySerialization()
            .create();

    @Test
    public void serialisesExactMatchers() throws Exception {
        //given
        MultiPrimeRequest.ExactMatch exactMatch = new MultiPrimeRequest.ExactMatch("hello");

        //when
        String json = gson.toJson(new Request(exactMatch));

        //then
        assertEquals("{\"variableMatch\":{\"matcher\":\"hello\",\"type\":\"exact\"}}", json);
    }

    private class Request {
        private final MultiPrimeRequest.VariableMatch variableMatch;

        private Request(MultiPrimeRequest.VariableMatch variableMatch) {
            this.variableMatch = variableMatch;
        }
    }
}