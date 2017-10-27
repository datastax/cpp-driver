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

public class GsonExactMatchSerialiser implements JsonSerializer<MultiPrimeRequest.ExactMatch> {
    @Override
    public JsonElement serialize(MultiPrimeRequest.ExactMatch src, Type typeOfSrc, JsonSerializationContext context) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.add("matcher", context.serialize(src.matcher()));
        jsonObject.addProperty("type", src.type().toString());
        return jsonObject;
    }
}
