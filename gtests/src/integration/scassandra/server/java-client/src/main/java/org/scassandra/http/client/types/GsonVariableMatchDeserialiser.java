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

import com.google.gson.*;
import org.scassandra.http.client.MultiPrimeRequest;

import java.lang.reflect.Type;

public class GsonVariableMatchDeserialiser implements JsonDeserializer<MultiPrimeRequest.VariableMatch> {
    @Override
    public MultiPrimeRequest.VariableMatch deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
        JsonObject jsonObject = (JsonObject) json;

        String type = jsonObject.get("type").getAsString();

        if (type.equalsIgnoreCase("any")) {
            return new MultiPrimeRequest.AnyMatch();
        } else if (type.equalsIgnoreCase("exact")) {
            String matcher = jsonObject.get("matcher").getAsString();
            return new MultiPrimeRequest.ExactMatch(matcher);
        } else {
            throw new JsonParseException("Unexpected variable matcher type received: " + type);
        }
    }
}
