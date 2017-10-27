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
package org.scassandra.cql;

abstract public class CqlType {
    public abstract String serialise();
    public abstract boolean equals(Object expected, Object actual);

    protected IllegalArgumentException throwInvalidType(Object expected, Object actual, CqlType instance) {
        return new IllegalArgumentException(String.format("Invalid expected value (%s,%s) for variable of types %s, the value was %s for valid types see: %s",
                expected,
                expected.getClass().getSimpleName(),
                instance.serialise(),
                actual,
                "http://www.scassandra.org/java-client/column-types/"
        ));
    }

}
