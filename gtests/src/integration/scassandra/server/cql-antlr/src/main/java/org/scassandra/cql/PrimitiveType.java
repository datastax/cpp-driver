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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

abstract public class PrimitiveType extends CqlType {

    private static final Map<String, PrimitiveType> mapping = new HashMap<String, PrimitiveType>();

    public static final PrimitiveType VARCHAR = registerPrimitive(new CqlVarchar());
    public static final PrimitiveType TEXT = registerPrimitive(new CqlText());
    public static final PrimitiveType ASCII = registerPrimitive(new CqlAscii());
    public static final PrimitiveType DOUBLE = registerPrimitive(new CqlDouble());
    public static final PrimitiveType FLOAT = registerPrimitive(new CqlFloat());
    public static final PrimitiveType INET = registerPrimitive(new CqlInet());
    public static final PrimitiveType INT = registerPrimitive(new CqlInt());
    public static final PrimitiveType BIG_INT = registerPrimitive(new CqlBigInt());
    public static final PrimitiveType TIMESTAMP = registerPrimitive(new CqlTimestamp());
    public static final PrimitiveType TIMEUUID = registerPrimitive(new CqlTimeuuid());
    public static final PrimitiveType UUID = registerPrimitive(new CqlUuid());
    public static final PrimitiveType VAR_INT = registerPrimitive(new CqlVarint());
    public static final PrimitiveType BLOB = registerPrimitive(new CqlBlob());
    public static final PrimitiveType BOOLEAN = registerPrimitive(new CqlBoolean());
    public static final PrimitiveType COUNTER = registerPrimitive(new CqlCounter());
    public static final PrimitiveType DECIMAL = registerPrimitive(new CqlDecimal());
    public static final PrimitiveType DATE = registerPrimitive(new CqlDate());
    public static final PrimitiveType SMALL_INT = registerPrimitive(new CqlSmallInt());
    public static final PrimitiveType TIME = registerPrimitive(new CqlTime());
    public static final PrimitiveType TINY_INT = registerPrimitive(new CqlTinyInt());

    private final String columnType;

    private static PrimitiveType registerPrimitive(PrimitiveType primitiveType) {
        mapping.put(primitiveType.serialise(), primitiveType);
        return primitiveType;
    }

    public static PrimitiveType fromName(String name) {
        return mapping.get(name);
    }

    PrimitiveType(String columnType) {
        this.columnType = columnType;
    }

    @Override
    public String serialise() {
        return columnType;
    }

    @Override
    public String toString() {
        return serialise();
    }

    @Override
    public boolean equals(Object expected, Object actual) {
        return false;
    }

    protected boolean equalsForLongType(Object expected, Object actual, CqlType columnTypes) {

        if (expected == null) return actual == null;
        if (actual == null) return false;

        Long typedActual = getActualValueLong(actual);

        if (expected instanceof Integer) {
            return ((Integer) expected).longValue() == typedActual;
        } else if (expected instanceof Long) {
            return expected.equals(typedActual);
        } else if (expected instanceof BigInteger) {
            return expected.equals(new BigInteger(typedActual.toString()));
        } else if (expected instanceof String) {
            return compareStringInteger(expected, typedActual, columnTypes);
        } else {
            throw throwInvalidType(expected, actual, columnTypes);
        }
    }

    protected long getActualValueLong(Object actual) {
        if (actual instanceof Double) {
            return ((Double) actual).longValue();
        } else {
            return Long.parseLong(actual.toString());
        }
    }

    protected boolean compareStringInteger(Object expected, Long actual, CqlType instance) {
        try {
            return new BigInteger((String) expected).equals(new BigInteger(actual.toString()));
        } catch (NumberFormatException e) {
            throw throwInvalidType(expected, actual, instance);
        }
    }

    protected IllegalArgumentException throwNullError(Object actual, CqlType instance) {
        return new IllegalArgumentException(String.format("Invalid expected value (null) for variable of types %s, the value was %s for valid types see: %s",
                instance.serialise(),
                actual,
                "http://www.scassandra.org/java-client/column-types/"
        ));
    }

    protected boolean equalsDecimalType(Object expected, Object actual, CqlType columnTypes) {
        if (expected == null) {
            throw throwNullError(actual, columnTypes);
        } else if (actual == null) {
            return false;
        }

        if (expected instanceof String) {
            try {
                return (new BigDecimal(expected.toString()).compareTo(new BigDecimal(actual.toString())) == 0);
            } catch (NumberFormatException e) {
                throw throwInvalidType(expected, actual, columnTypes);
            }
        } else if (expected instanceof BigDecimal) {
            return ((BigDecimal) expected).compareTo(new BigDecimal(actual.toString())) == 0;
        } else {
            throw throwInvalidType(expected, actual, columnTypes);
        }
    }

    protected boolean equalsForUUID(Object expected, Object actual, CqlType columnTypes) {
        if (expected == null) return actual == null;
        if (actual == null) return false;

        if (expected instanceof String) {
            try {
                return java.util.UUID.fromString(expected.toString()).equals(java.util.UUID.fromString(actual.toString()));
            } catch (Exception e) {
                throw throwInvalidType(expected, actual, columnTypes);
            }
        } else if (expected instanceof java.util.UUID) {
            return expected.toString().equals(actual);
        } else {
            throw throwInvalidType(expected, actual, columnTypes);
        }
    }

}
