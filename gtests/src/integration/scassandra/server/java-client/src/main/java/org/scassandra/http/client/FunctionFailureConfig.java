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

public class FunctionFailureConfig extends Config {

    private final String keyspace;
    private final String function;
    private final String argTypes;

    public FunctionFailureConfig(String keyspace, String function, String argTypes) {
        this.keyspace = keyspace;
        this.function = function;
        this.argTypes = argTypes;
    }

    @Override
    Map<String, ?> getProperties() {
        ImmutableMap.Builder<String,String> builder = ImmutableMap.<String,String>builder()
                .put(ErrorConstants.Keyspace(), String.valueOf(this.keyspace))
                .put(ErrorConstants.Function(), String.valueOf(this.function))
                .put(ErrorConstants.Argtypes(), String.valueOf(this.argTypes));
        return builder.build();
    }
}
