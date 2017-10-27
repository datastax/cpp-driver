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

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.scassandra.server.priming.ErrorConstants;

public class UnpreparedConfig extends Config {

    private String prepareId;

    private String message;

    public UnpreparedConfig(String prepareId) {
        this(prepareId, null);
    }

    public UnpreparedConfig(String prepareId, String message) {
        this.prepareId = prepareId;
        this.message = message;
    }

    @Override
    Map<String, ?> getProperties() {
        ImmutableMap.Builder<String, String> mapBuilder = ImmutableMap.builder();
        mapBuilder.put(ErrorConstants.PrepareId(), prepareId);
        if(message != null) {
            mapBuilder.put(ErrorConstants.Message(), message);
        }
        return mapBuilder.build();
    }
}
