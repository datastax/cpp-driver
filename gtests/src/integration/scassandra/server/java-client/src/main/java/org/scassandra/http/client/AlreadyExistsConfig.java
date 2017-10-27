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

public class AlreadyExistsConfig extends Config {

    private String keyspace;
    private String table;
    private String message;

    public AlreadyExistsConfig(String keyspace) {
        this(keyspace, "");
    }

    public AlreadyExistsConfig(String keyspace, String table) {
        this(keyspace, table, null);
    }

    public AlreadyExistsConfig(String keyspace, String table, String message) {
        this.keyspace = keyspace;
        this.table = table;
        this.message = message;
    }

    @Override
    Map<String, ?> getProperties() {
        ImmutableMap.Builder<String, String> mapBuilder = ImmutableMap.builder();
        mapBuilder.put(ErrorConstants.Keyspace(), keyspace);
        mapBuilder.put(ErrorConstants.Table(), table);
        if(message != null) {
            mapBuilder.put(ErrorConstants.Message(), message);
        }
        return mapBuilder.build();
    }
}
