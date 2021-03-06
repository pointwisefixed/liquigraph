/**
 * Copyright 2014-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.liquigraph.connector.io;

import org.liquigraph.model.CompoundQuery;
import org.liquigraph.model.Precondition;
import org.liquigraph.model.PreconditionQuery;
import org.liquigraph.model.SimpleQuery;

import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;

public class PreconditionPrinter {

    public Collection<String> print(Precondition precondition) {
        if (precondition == null) {
            return newArrayList();
        }
        Collection<String> lines = newArrayList();
        lines.add(String.format("//Liquigraph precondition[if-not-met: %s]", precondition.getPolicy()));
        lines.add(traverseQuery(precondition.getQuery()));
        return lines;
    }

    private String traverseQuery(PreconditionQuery query) {
        if (query instanceof SimpleQuery) {
            return ((SimpleQuery) query).getQuery();
        }
        if (query instanceof CompoundQuery) {
            CompoundQuery compoundQuery = (CompoundQuery) query;
            return compoundQuery.compose(
                traverseQuery(compoundQuery.getFirstQuery()),
                traverseQuery(compoundQuery.getSecondQuery())
            );
        }
        throw new IllegalArgumentException(String.format("Unsupported query type <%s>", query.getClass().getName()));
    }
}
