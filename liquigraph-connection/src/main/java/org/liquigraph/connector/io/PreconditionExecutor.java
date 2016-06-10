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

import org.liquigraph.connector.connection.ConnectionWrapper;
import org.liquigraph.connector.connection.ResultSetWrapper;
import org.liquigraph.connector.connection.StatementWrapper;
import org.liquigraph.model.exception.PreconditionExecutionException;
import org.liquigraph.model.CompoundQuery;
import org.liquigraph.model.Precondition;
import org.liquigraph.model.PreconditionQuery;
import org.liquigraph.model.SimpleQuery;

import static com.google.common.base.Preconditions.checkArgument;

public class PreconditionExecutor {

    public final PreconditionResult executePrecondition(ConnectionWrapper connection, Precondition precondition) {
        checkArgument(connection != null, "Connection should not be null");
        return new PreconditionResult(
            precondition.getPolicy(),
            applyPrecondition(connection, precondition.getQuery())
        );
    }

    private boolean applyPrecondition(ConnectionWrapper connection, PreconditionQuery query) {
        if (query instanceof SimpleQuery) {
            SimpleQuery simpleQuery = (SimpleQuery) query;
            return execute(connection, simpleQuery.getQuery());
        }
        if (query instanceof CompoundQuery) {
            CompoundQuery compoundQuery = (CompoundQuery) query;
            return compoundQuery.compose(
                applyPrecondition(connection, compoundQuery.getFirstQuery()),
                applyPrecondition(connection, compoundQuery.getSecondQuery())
            );
        }
        throw new IllegalArgumentException(String.format("Unsupported query type <%s>", query.getClass().getName()));
    }

    private boolean execute(ConnectionWrapper connection, String query) {
        try (StatementWrapper statement = connection.createStatement();
             ResultSetWrapper resultSet = statement.executeQuery(query)) {

            resultSet.next();
            boolean rst = resultSet.getBoolean("result");
            return rst;
        }
        catch (Exception e) {
            throw new PreconditionExecutionException("%nError executing precondition:%n" +
               "\tMake sure your query <%s> yields exactly one column named or aliased 'result'.%n" +
               "\tActual cause: %s", query, e.getMessage());
        }
    }
}
