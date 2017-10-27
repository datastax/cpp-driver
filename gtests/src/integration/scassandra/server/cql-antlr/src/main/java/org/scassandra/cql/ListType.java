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

import java.util.List;

public class ListType extends CqlType {
    private final CqlType type;

    public static ListType list(CqlType type) {
        return new ListType(type);
    }

    ListType(CqlType type) {
        this.type = type;
    }

    @Override
    public String serialise() {
        return String.format("list<%s>", type.serialise());
    }

    @Override
    public boolean equals(Object expected, Object actual) {
        if (expected == null) return actual == null;
        if (actual == null) return false;

        if (expected instanceof List) {
            final List<?> typedExpected = (List<?>) expected;
            final List<?> actualList = (List<?>) actual;

            if (typedExpected.size() != actualList.size()) return false;

            for (int i = 0; i < actualList.size(); i++) {
                if (!type.equals(typedExpected.get(i), actualList.get(i))) return false;
            }
            return true;
        } else {
            throw throwInvalidType(expected, actual, this);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ListType setType = (ListType) o;

        if (type != null ? !type.equals(setType.type) : setType.type != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return type != null ? type.hashCode() : 0;
    }

    @Override
    public String toString() {
        return this.serialise();
    }

    public CqlType getType() {
        return type;
    }
}
