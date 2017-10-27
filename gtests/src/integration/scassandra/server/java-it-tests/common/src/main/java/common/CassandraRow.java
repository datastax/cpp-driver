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
package common;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.*;

public interface CassandraRow {

    Date getTimestamp(String columnName);

    String getString(String name);

    int getInt(String age);

    BigDecimal getDecimal(String adecimal);

    ByteBuffer getBytes(String blob);

    boolean getBool(String aBoolean);

    double getFloat(String name);

    long getLong(String name);

    <T> Set<T> getSet(String name, Class<T> setType);

    <T> List<T> getList(String name, Class<T> listType);

    <K, V> Map<K, V>getMap(String name, Class<K> keyType, Class<V> valueType);

    InetAddress getInet(String name);

    BigInteger getVarint(String name);

    double getDouble(String name);

    UUID getUUID(String name);

    Short getShort(String name);

    Byte getByte(String name);

    long getTime(String name);

    LocalDate getDate(String name);
}
