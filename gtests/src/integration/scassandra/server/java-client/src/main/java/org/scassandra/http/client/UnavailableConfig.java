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
import org.scassandra.server.priming.ErrorConstants;

import java.util.Map;

public class UnavailableConfig extends Config {

    private final int requiredAcknowledgements;
    private final int alive;
    private final Consistency consistencyLevel;

    public UnavailableConfig(int requiredAcknowledgements, int alive) {
        this(requiredAcknowledgements, alive, null);
    }

    public UnavailableConfig(int requiredAcknowledgements, int alive, Consistency consistencyLevel) {
        this.requiredAcknowledgements = requiredAcknowledgements;
        this.alive = alive;
        this.consistencyLevel = consistencyLevel;
    }

    @Override
    Map<String, ?> getProperties() {
        ImmutableMap.Builder<String,String> builder = ImmutableMap.<String,String>builder()
                .put(ErrorConstants.Alive(), String.valueOf(this.alive))
                .put(ErrorConstants.RequiredResponse(), String.valueOf(this.requiredAcknowledgements));

        if(consistencyLevel != null) {
            builder.put(ErrorConstants.ConsistencyLevel(), String.valueOf(consistencyLevel.toString()));
        }

        return builder.build();
    }
}
