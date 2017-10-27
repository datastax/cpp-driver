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

import com.google.common.base.Preconditions;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.NotNull;
import org.scassandra.antlr4.CqlTypesBaseListener;
import org.scassandra.antlr4.CqlTypesLexer;
import org.scassandra.antlr4.CqlTypesParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Stack;

public class CqlTypeFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(CqlTypeFactory.class);

    public CqlType buildType(String typeString) {
        CqlTypesLexer lexer = new CqlTypesLexer(new ANTLRInputStream(typeString));
        CqlTypesParser parser = new CqlTypesParser(new CommonTokenStream(lexer));

        parser.addErrorListener(new BaseErrorListener() {
            @Override
            public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e) {
                throw new IllegalArgumentException(msg);
            }
        });

        CqlTypesBaseListenerImpl listener = new CqlTypesBaseListenerImpl();
        parser.addParseListener(listener);

        parser.data_type();
        return listener.getCqlType();
    }

    private static class CqlTypesBaseListenerImpl extends CqlTypesBaseListener {

        private Stack<Stack<CqlType>> stack = new Stack<Stack<CqlType>>();

        private Stack<CqlType> inProgress = new Stack<CqlType>();

        private CqlType cqlType;

        void pushStack() {
            if (!inProgress.isEmpty()) {
                stack.push(inProgress);
                inProgress = new Stack<CqlType>();
            }
        }

        void popStack(CqlType typeInProgress) {
            Preconditions.checkState(inProgress.isEmpty(), "Current inProgress stack is not empty");
            // no more stacks, this is the final type.
            if (stack.isEmpty()) {
                this.cqlType = typeInProgress;
                this.inProgress.clear();
            } else {
                // remaining stacks, we have more work to do.
                inProgress = stack.pop();
                inProgress.push(typeInProgress);
            }
        }

        @Override
        public void enterData_type(@NotNull CqlTypesParser.Data_typeContext ctx) {
            LOGGER.debug("Type begins: " + ctx.start.getText());
        }

        @Override
        public void exitData_type(@NotNull CqlTypesParser.Data_typeContext ctx) {
            LOGGER.debug("Type ends: " + ctx.start.getText());
        }

        @Override
        public void exitNative_type(@NotNull CqlTypesParser.Native_typeContext ctx) {
            String text = ctx.start.getText();
            PrimitiveType primitiveType = PrimitiveType.fromName(text);
            if (inProgress.isEmpty()) {
                this.cqlType = primitiveType;
            } else {
                inProgress.push(primitiveType);
            }
        }

        @Override
        public void enterMap_type(@NotNull CqlTypesParser.Map_typeContext ctx) {
            pushStack();
            this.inProgress.push(new MapType(null, null));
        }

        @Override
        public void exitMap_type(@NotNull CqlTypesParser.Map_typeContext ctx) {
            CqlType value = inProgress.pop();
            CqlType key = inProgress.pop();
            inProgress.pop();
            popStack(new MapType(key, value));
        }

        @Override
        public void enterSet_type(@NotNull CqlTypesParser.Set_typeContext ctx) {
            pushStack();
            this.inProgress.push(new SetType(null));
        }

        @Override
        public void exitSet_type(@NotNull CqlTypesParser.Set_typeContext ctx) {
            CqlType value = inProgress.pop();
            inProgress.pop();
            popStack(new SetType(value));
        }

        @Override
        public void enterList_type(@NotNull CqlTypesParser.List_typeContext ctx) {
            pushStack();
            this.inProgress.push(new ListType(null));
        }

        @Override
        public void exitList_type(@NotNull CqlTypesParser.List_typeContext ctx) {
            CqlType value = inProgress.pop();
            inProgress.pop();
            popStack(new ListType(value));
        }

        @Override
        public void enterTuple_type(@NotNull CqlTypesParser.Tuple_typeContext ctx) {
            pushStack();
            this.inProgress.push(new TupleType(null));
        }

        @Override
        public void exitTuple_type(@NotNull CqlTypesParser.Tuple_typeContext ctx) {
            List<CqlType> types = new ArrayList<CqlType>();
            while (true) {
                CqlType type = inProgress.pop();
                if (type instanceof TupleType) {
                    if (((TupleType) type).getTypes() == null) {
                        // Reverse list as stack is FILO.
                        Collections.reverse(types);
                        // encountered empty tuple, we know to stop.
                        popStack(new TupleType(types.toArray(new CqlType[types.size()])));
                        break;
                    }
                }
                types.add(type);
            }
        }

        public CqlType getCqlType() {
            return cqlType;
        }
    }
}
