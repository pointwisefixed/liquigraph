/**
 * Copyright 2014-2016 the original author or authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.liquigraph.core.io;

import com.google.common.collect.Lists;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.liquigraph.connector.connection.ConnectionWrapper;
import org.liquigraph.connector.connection.StatementWrapper;
import org.liquigraph.connector.io.PreconditionExecutor;
import org.liquigraph.connector.io.PreconditionResult;
import org.liquigraph.core.EmbeddedGraphDatabaseRule;
import org.liquigraph.model.*;
import org.liquigraph.model.exception.PreconditionExecutionException;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static org.assertj.core.api.Assertions.assertThat;

public class PreconditionExecutorTest {

    @Rule public EmbeddedGraphDatabaseRule graphDatabaseRule = new EmbeddedGraphDatabaseRule("neotest");

    @Rule public ExpectedException thrown = ExpectedException.none();

    private PreconditionExecutor executor = new PreconditionExecutor();

    @Test
    public void executes_simple_precondition() throws Exception {
        ConnectionWrapper connection = graphDatabaseRule.connection();
        try (StatementWrapper ignored = connection.createStatement()) {
            PreconditionResult result = executor.executePrecondition(connection,
                simplePrecondition(PreconditionErrorPolicy.MARK_AS_EXECUTED, "RETURN true AS result"));

            assertThat(result.errorPolicy()).isEqualTo(PreconditionErrorPolicy.MARK_AS_EXECUTED);
            assertThat(result.executedSuccessfully()).isTrue();
        }
    }

    @Test
    public void executes_nested_and_precondition_queries() throws Exception {
        ConnectionWrapper connection = graphDatabaseRule.connection();
        try (StatementWrapper ignored = connection.createStatement()) {
            PreconditionResult result = executor.executePrecondition(connection,
                andPrecondition(PreconditionErrorPolicy.FAIL, "RETURN true AS result", "RETURN false AS result"));

            assertThat(result.errorPolicy()).isEqualTo(PreconditionErrorPolicy.FAIL);
            assertThat(result.executedSuccessfully()).isFalse();
        }
    }

    @Test
    public void executes_nested_or_precondition_queries() throws Exception {
        ConnectionWrapper connection = graphDatabaseRule.connection();
        try (StatementWrapper ignored = connection.createStatement()) {
            PreconditionResult result = executor.executePrecondition(connection,
                orPrecondition(PreconditionErrorPolicy.CONTINUE, "RETURN true AS result", "RETURN false AS result"));

            assertThat(result.errorPolicy()).isEqualTo(PreconditionErrorPolicy.CONTINUE);
            assertThat(result.executedSuccessfully()).isTrue();
        }
    }

    @Test
    public void executes_nested_mixed_precondition_queries_like_a_charm() throws Exception {
        Precondition precondition = precondition(PreconditionErrorPolicy.MARK_AS_EXECUTED);
        AndQuery andQuery = new AndQuery();
        andQuery.setPreconditionQueries(
            newArrayList(orPreconditionQuery("RETURN false AS result", "RETURN true AS result"),
                simplePreconditionQuery("RETURN true AS result")));
        precondition.setQuery(andQuery);

        ConnectionWrapper connection = graphDatabaseRule.connection();
        try (StatementWrapper ignored = connection.createStatement()) {
            PreconditionResult result = executor.executePrecondition(connection, precondition);

            assertThat(result.errorPolicy()).isEqualTo(PreconditionErrorPolicy.MARK_AS_EXECUTED);
            assertThat(result.executedSuccessfully()).isTrue();
        }
    }

    @Test
    public void fails_with_invalid_cypher_query() throws Exception {
        thrown.expect(PreconditionExecutionException.class);
        if(!graphDatabaseRule.isBolt()) {
            thrown.expectMessage("Error executing precondition:\n" +
                "\tMake sure your query <toto> yields exactly one column named or aliased 'result'.\n" +
                "\tActual cause: Error executing query toto\n" +
                " with params {}");
        }
        else{
            thrown.expectMessage("\n" + "Error executing precondition:\n"
                + "\tMake sure your query <toto> yields exactly one column named or aliased 'result'.\n"
                + "\tActual cause: Invalid input 't': expected <init> (line 1, column 1 (offset: 0))\n" + "\"toto\"\n ^");
        }
        ConnectionWrapper connection = graphDatabaseRule.connection();
        try (StatementWrapper ignored = connection.createStatement()) {
            executor
                .executePrecondition(connection, simplePrecondition(PreconditionErrorPolicy.MARK_AS_EXECUTED, "toto"));
        }
    }

    @Test
    public void fails_with_badly_named_precondition_result_column() throws Exception {
        thrown.expect(PreconditionExecutionException.class);
        thrown.expectMessage("Make sure your query <RETURN true> yields exactly one column named or aliased 'result'.");

        ConnectionWrapper connection = graphDatabaseRule.connection();
        try (StatementWrapper ignored = connection.createStatement()) {
            executor.executePrecondition(connection,
                simplePrecondition(PreconditionErrorPolicy.MARK_AS_EXECUTED, "RETURN true"));
        }
    }

    @Test
    public void fails_with_unknown_query_type() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Unsupported query type <org.liquigraph.core.io.PreconditionExecutorTest$1>");

        Precondition precondition = new Precondition();
        precondition.setQuery(new PreconditionQuery() {
        });
        executor.executePrecondition(graphDatabaseRule.connection(), precondition);
    }

    private Precondition simplePrecondition(PreconditionErrorPolicy fail, String query) {
        SimpleQuery simpleQuery = simplePreconditionQuery(query);
        Precondition precondition = precondition(fail);
        precondition.setQuery(simpleQuery);
        return precondition;
    }

    private Precondition andPrecondition(PreconditionErrorPolicy errorPolicy, String firstQuery, String secondQuery) {
        Precondition precondition = precondition(errorPolicy);
        precondition.setQuery(andPreconditionQuery(firstQuery, secondQuery));
        return precondition;
    }

    private Precondition orPrecondition(PreconditionErrorPolicy errorPolicy, String firstQuery, String secondQuery) {
        Precondition precondition = precondition(errorPolicy);
        precondition.setQuery(orPreconditionQuery(firstQuery, secondQuery));
        return precondition;
    }

    private AndQuery andPreconditionQuery(String firstQuery, String secondQuery) {
        AndQuery andQuery = new AndQuery();
        andQuery.setPreconditionQueries(simpleQueries(firstQuery, secondQuery));
        return andQuery;
    }

    private OrQuery orPreconditionQuery(String firstQuery, String secondQuery) {
        OrQuery orQuery = new OrQuery();
        orQuery.setPreconditionQueries(simpleQueries(firstQuery, secondQuery));
        return orQuery;
    }

    private List<PreconditionQuery> simpleQueries(String firstQuery, String secondQuery) {
        return Lists.<PreconditionQuery>newArrayList(simplePreconditionQuery(firstQuery),
            simplePreconditionQuery(secondQuery));
    }

    private Precondition precondition(PreconditionErrorPolicy policy) {
        Precondition precondition = new Precondition();
        precondition.setPolicy(policy);
        return precondition;
    }

    private SimpleQuery simplePreconditionQuery(String query) {
        SimpleQuery simpleQuery = new SimpleQuery();
        simpleQuery.setQuery(query);
        return simpleQuery;
    }
}
