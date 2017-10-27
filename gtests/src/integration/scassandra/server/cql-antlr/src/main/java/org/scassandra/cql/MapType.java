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

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

import java.util.Map;

public class MapType extends CqlType {
    private CqlType keyType;
    private CqlType valueType;

    public static MapType map(CqlType keyType, CqlType valueType) {
        return new MapType(keyType, valueType);
    }

    MapType(CqlType keyType, CqlType valueType) {
        this.keyType = keyType;
        this.valueType = valueType;
    }

    @Override
    public String serialise() {
        return String.format("map<%s,%s>", keyType.serialise(), valueType.serialise());
    }

    @Override
    public boolean equals(Object expected, Object actual) {
        if (expected == null) return actual == null;
        if (actual == null) return false;

        if (expected instanceof Map) {
            final Map<?,?> typedExpected = (Map<?, ?>) expected;
            final Map<?,?> actualMap = (Map<?, ?>) actual;

            if (typedExpected.size() != actualMap.size()) return false;

            for (final Map.Entry<?, ?> eachExpected : typedExpected.entrySet()) {
                boolean match = Iterables.any(actualMap.keySet(), new Predicate<Object>() {
                    @Override
                    public boolean apply(Object eachActualKey) {
                        Object eachActual = actualMap.get(eachActualKey);
                        return keyType.equals(eachExpected.getKey(), eachActualKey) && valueType.equals(eachExpected.getValue(), eachActual);
                    }
                });

                if (!match) return false;
            }
            return true;
        } else {
            throw throwInvalidType(expected, actual, this);
        }
    }

    @Override
    public String toString() {
        return this.serialise();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MapType mapType1 = (MapType) o;

        if (keyType != null ? !keyType.equals(mapType1.keyType) : mapType1.keyType != null) return false;
        if (valueType != null ? !valueType.equals(mapType1.valueType) : mapType1.valueType != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = keyType != null ? keyType.hashCode() : 0;
        result = 31 * result + (valueType != null ? valueType.hashCode() : 0);
        return result;
    }

    public CqlType getKeyType() {
        return keyType;
    }

    public CqlType getValueType() {
        return valueType;
    }
}
