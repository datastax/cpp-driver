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

import java.util.Arrays;
import java.util.List;

public class TupleType extends CqlType {

    private final CqlType[] types;

    public static TupleType tuple(CqlType... types) {
        return new TupleType(types);
    }

    TupleType(CqlType... types) {
        this.types = types;
    }

    @Override
    public String serialise() {
        StringBuilder s = new StringBuilder("tuple<");
        for (int i = 0; i < types.length; i++) {
            CqlType type = types[i];
            s.append(type.serialise());
            if (i < types.length - 1) {
                s.append(",");
            }
        }
        s.append(">");
        return s.toString();
    }

    @Override
    public boolean equals(Object expected, Object actual) {
        if (expected == null) return actual == null;
        if (actual == null) return false;

        if (expected instanceof List) {
            final List<?> typedExpected = (List<?>) expected;
            final List<?> actualList = (List<?>) actual;

            if (typedExpected.size() != actualList.size()) return false;

            // expect exact elements
            if (types == null || typedExpected.size() != types.length)
                throw throwInvalidType(expected, actual, this);

            for (int i = 0; i < actualList.size(); i++) {
                CqlType type = types[i];
                if (!type.equals(typedExpected.get(i), actualList.get(i))) return false;
            }
            return true;
        } else {
            throw throwInvalidType(expected, actual, this);
        }
    }


    public CqlType[] getTypes() {
        return types;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TupleType)) return false;

        TupleType tupleType = (TupleType) o;

        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        return Arrays.equals(types, tupleType.types);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(types);
    }
}