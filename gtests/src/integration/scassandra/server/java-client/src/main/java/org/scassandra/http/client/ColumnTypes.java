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

import org.scassandra.cql.CqlType;
import org.scassandra.cql.PrimitiveType;

import static org.scassandra.cql.ListType.list;
import static org.scassandra.cql.MapType.map;
import static org.scassandra.cql.PrimitiveType.*;
import static org.scassandra.cql.SetType.set;

/**
 * This won't be an enum in version 1.0 where we'll make a breaking change to represent
 * types using classes like Jackson
 *
 * @deprecated Use CqlType instead.
 */
@Deprecated()
public enum ColumnTypes {

    Ascii,
    Bigint,
    Blob,
    Boolean,
    Counter,
    Decimal,
    Double,
    Float,
    Int,
    Timestamp,
    Varchar,
    Varint,
    Timeuuid,
    Uuid,
    Inet,
    Text,
    VarcharSet {
        @Override
        public CqlType getType() {
            return set(VARCHAR);
        }
    },
    AsciiSet {
        @Override
        public CqlType getType() {
            return set(ASCII);
        }
    },
    TextSet {
        @Override
        public CqlType getType() {
            return set(TEXT);
        }
    },
    BigintSet {
        @Override
        public CqlType getType() {
            return set(BIG_INT);
        }
    },

    BlobSet {
        @Override
        public CqlType getType() {
            return set(BLOB);
        }
    },
    BooleanSet {
        @Override
        public CqlType getType() {
            return set(BOOLEAN);
        }
    },
    DecimalSet {
        @Override
        public CqlType getType() {
            return set(DECIMAL);
        }
    },
    DoubleSet {
        @Override
        public CqlType getType() {
            return set(DOUBLE);
        }
    },
    FloatSet {
        @Override
        public CqlType getType() {
            return set(FLOAT);
        }
    },
    InetSet {
        @Override
        public CqlType getType() {
            return set(INET);
        }
    },
    IntSet {
        @Override
        public CqlType getType() {
            return set(INT);
        }
    },
    TimestampSet {
        @Override
        public CqlType getType() {
            return set(TIMESTAMP);
        }
    },
    TimeuuidSet {
        @Override
        public CqlType getType() {
            return set(TIMEUUID);
        }
    },
    UuidSet {
        @Override
        public CqlType getType() {
            return set(UUID);
        }
    },
    VarintSet {
        @Override
        public CqlType getType() {
            return set(VAR_INT);
        }
    },

    VarcharList {
        @Override
        public CqlType getType() {
            return list(PrimitiveType.VARCHAR);
        }
    },
    AsciiList {
        @Override
        public CqlType getType() {
            return list(ASCII);
        }
    },
    TextList {
        @Override
        public CqlType getType() {
            return list(TEXT);
        }
    },
    BigintList {
        @Override
        public CqlType getType() {
            return list(BIG_INT);
        }
    },
    BlobList {
        @Override
        public CqlType getType() {
            return list(BLOB);
        }
    },
    BooleanList {
        @Override
        public CqlType getType() {
            return list(BOOLEAN);
        }
    },
    DecimalList {
        @Override
        public CqlType getType() {
            return list(DECIMAL);
        }
    },
    DoubleList {
        @Override
        public CqlType getType() {
            return list(DOUBLE);
        }
    },
    FloatList {
        @Override
        public CqlType getType() {
            return list(FLOAT);
        }
    },

    InetList {
        @Override
        public CqlType getType() {
            return list(INET);
        }
    },
    IntList {
        @Override
        public CqlType getType() {
            return list(INT);
        }
    },
    TimestampList {
        @Override
        public CqlType getType() {
            return list(TIMESTAMP);
        }
    },
    TimeuuidList {
        @Override
        public CqlType getType() {
            return list(TIMEUUID);
        }
    },
    UuidList {
        @Override
        public CqlType getType() {
            return list(UUID);
        }
    },
    VarintList {
        @Override
        public CqlType getType() {
            return list(VAR_INT);
        }
    },
    VarcharVarcharMap {
        @Override
        public CqlType getType() {
            return map(PrimitiveType.VARCHAR, PrimitiveType.VARCHAR);
        }
    },
    VarcharTextMap  {
        @Override
        public CqlType getType() {
            return map(PrimitiveType.VARCHAR, PrimitiveType.TEXT);
        }
    },
    VarcharAsciiMap {
        @Override
        public CqlType getType() {
            return map(PrimitiveType.VARCHAR, PrimitiveType.ASCII);
        }
    },
    TextVarcharMap  {
        @Override
        public CqlType getType() {
            return map(PrimitiveType.TEXT, PrimitiveType.VARCHAR);
        }
    },
    TextTextMap {
        @Override
        public CqlType getType() {
            return map(PrimitiveType.TEXT, PrimitiveType.TEXT);
        }
    },
    TextAsciiMap  {
        @Override
        public CqlType getType() {
            return map(PrimitiveType.TEXT, PrimitiveType.ASCII);
        }
    },
    AsciiVarcharMap  {
        @Override
        public CqlType getType() {
            return map(PrimitiveType.ASCII, PrimitiveType.VARCHAR);
        }
    },
    AsciiTextMap  {
        @Override
        public CqlType getType() {
            return map(PrimitiveType.ASCII, PrimitiveType.TEXT);
        }
    },
    AsciiAsciiMap  {
        @Override
        public CqlType getType() {
            return map(PrimitiveType.ASCII, PrimitiveType.ASCII);
        }
    };

    /**
     * To offer backward compatibility
     */
    public CqlType getType() {
        return PrimitiveType.fromName(this.name().toLowerCase());
    }
}
