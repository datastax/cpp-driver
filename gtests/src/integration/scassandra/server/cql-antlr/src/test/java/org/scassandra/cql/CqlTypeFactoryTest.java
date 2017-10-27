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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class CqlTypeFactoryTest {

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"map<ascii,ascii>", new MapType(PrimitiveType.ASCII, PrimitiveType.ASCII)},
                {"map<blob,inet>", new MapType(PrimitiveType.BLOB, PrimitiveType.INET)},
                {"map<uuid,timeuuid>", new MapType(PrimitiveType.UUID, PrimitiveType.TIMEUUID)},
                {"map<list<text>,set<int>>", new MapType(new ListType(PrimitiveType.TEXT), new SetType(PrimitiveType.INT))},
                {"set<ascii>", new SetType(PrimitiveType.ASCII)},
                {"set<inet>", new SetType(PrimitiveType.INET)},
                {"set<map<int,text>>", new SetType(new MapType(PrimitiveType.INT, PrimitiveType.TEXT))},
                {"list<ascii>", new ListType(PrimitiveType.ASCII)},
                {"list<boolean>", new ListType(PrimitiveType.BOOLEAN)},
                {"list<decimal>", new ListType(PrimitiveType.DECIMAL)},
                {"list<set<text>>", new ListType(new SetType(PrimitiveType.TEXT))},
                {"tuple<int,inet,date>", new TupleType(PrimitiveType.INT, PrimitiveType.INET, PrimitiveType.DATE)},
                {"ascii", PrimitiveType.ASCII},
                {"varchar", PrimitiveType.VARCHAR},
                {"bigint", PrimitiveType.BIG_INT},
                {"blob", PrimitiveType.BLOB},
                {"boolean", PrimitiveType.BOOLEAN},
                {"counter", PrimitiveType.COUNTER},
                {"decimal", PrimitiveType.DECIMAL},
                {"double", PrimitiveType.DOUBLE},
                {"float", PrimitiveType.FLOAT},
                {"int", PrimitiveType.INT},
                {"timestamp", PrimitiveType.TIMESTAMP},
                {"varint", PrimitiveType.VAR_INT},
                {"timeuuid", PrimitiveType.TIMEUUID},
                {"inet", PrimitiveType.INET},
                {"text", PrimitiveType.TEXT},
                {"date", PrimitiveType.DATE},
                {"smallint", PrimitiveType.SMALL_INT},
                {"time", PrimitiveType.TIME},
                {"tinyint", PrimitiveType.TINY_INT},
                {"tuple<tuple<int,inet,list<date>>,map<text,set<varchar>>>",
                        new TupleType(
                                new TupleType(PrimitiveType.INT, PrimitiveType.INET, new ListType(PrimitiveType.DATE)),
                                new MapType(PrimitiveType.TEXT, new SetType(PrimitiveType.VARCHAR)))},
        });
    }

    private String input;

    private CqlType expectedType;

    public CqlTypeFactoryTest(String input, CqlType expectedType) {
        this.input = input;
        this.expectedType = expectedType;
    }

    private CqlTypeFactory underTest = new CqlTypeFactory();

    @Test
    public void test() throws Exception {
        assertEquals(expectedType, underTest.buildType(input));
    }
}