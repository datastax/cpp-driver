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

import java.math.BigInteger;

public class CqlVarint extends PrimitiveType {
    CqlVarint() {
        super("varint");
    }
    @Override
    public boolean equals(Object expected, Object actual) {
        if (expected == null) return actual == null;
        if (actual == null) return expected == null;

        Long typedActual = getActualValueLong(actual);

        if (expected instanceof BigInteger) {
            return expected.equals(new BigInteger(typedActual.toString()));
        } else if (expected instanceof String) {
            try {
                return new BigInteger((String) expected).equals(new BigInteger(typedActual.toString()));
            } catch (NumberFormatException e) {
                throw throwInvalidType(expected, actual, this);
            }
        } else {
            throw throwInvalidType(expected, actual, this);
        }
    }
}
