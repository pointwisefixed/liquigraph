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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.liquigraph.connector.connection.ConnectionWrapper;
import org.liquigraph.connector.connection.ResultSetWrapper;
import org.liquigraph.connector.connection.StatementWrapper;
import org.liquigraph.connector.io.ChangelogGraphWriter;
import org.liquigraph.connector.io.PreconditionExecutor;
import org.liquigraph.core.EmbeddedGraphDatabaseRule;
import org.liquigraph.model.Changeset;
import org.liquigraph.model.Precondition;
import org.liquigraph.model.PreconditionErrorPolicy;
import org.liquigraph.model.SimpleQuery;
import org.liquigraph.model.exception.PreconditionNotMetException;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Node;

import java.sql.SQLException;
import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.liquigraph.model.Checksums.checksum;

public class ChangelogGraphWriterTest {

    @Rule public EmbeddedGraphDatabaseRule jdbcGraph = new EmbeddedGraphDatabaseRule("neotest", false);
    @Rule public ExpectedException thrown = ExpectedException.none();
    private ChangelogGraphWriter writer;

    @Before
    public void prepare() throws Exception {
        writer = new ChangelogGraphWriter(jdbcGraph.connection(), new PreconditionExecutor());
    }

    @Test
    public void persists_changesets_in_graph() throws Exception {
        writer.write(newArrayList(changeset("identifier", "fbiville", "CREATE (n: SomeNode {text:'yeah'})")));

        assertThatQueryIsExecutedAndHistoryPersisted(jdbcGraph.connection());
    }

    @Test
    public void persists_changesets_in_graph_when_preconditions_are_met() throws Exception {
        Precondition precondition = precondition(PreconditionErrorPolicy.FAIL, "RETURN true AS result");
        Changeset changeset = changeset("identifier", "fbiville", "CREATE (n: SomeNode {text:'yeah'})", precondition);

        writer.write(newArrayList(changeset));

        assertThatQueryIsExecutedAndHistoryPersisted(jdbcGraph.connection());
    }

    @Test
    public void fails_when_precondition_is_not_met_and_configured_to_halt_execution() {
        thrown.expect(PreconditionNotMetException.class);
        thrown.expectMessage(
            "Changeset <identifier>: precondition query <RETURN false AS result> failed with policy <FAIL>. Aborting.");

        Precondition precondition = precondition(PreconditionErrorPolicy.FAIL, "RETURN false AS result");
        Changeset changeset = changeset("identifier", "fbiville", "CREATE (n: SomeNode {text:'yeah'})", precondition);

        writer.write(newArrayList(changeset));
    }

    @Test
    public void skips_changeset_execution_when_precondition_is_not_met_and_configured_to_skip() throws Exception {
        Precondition precondition = precondition(PreconditionErrorPolicy.CONTINUE, "RETURN false AS result");
        Changeset changeset = changeset("identifier", "fbiville", "CREATE (n: SomeNode {text:'yeah'})", precondition);

        writer.write(newArrayList(changeset));

        try (StatementWrapper statement = jdbcGraph.connection().createStatement();
            ResultSetWrapper resultSet = statement.executeQuery(
                "MATCH (changelog:__LiquigraphChangelog)<-[execution:EXECUTED_WITHIN_CHANGELOG]-(changeset:__LiquigraphChangeset) "
                    +
                    "OPTIONAL MATCH (node :SomeNode) " +
                    "RETURN execution.order AS order, changeset, node")) {

            assertThat(resultSet.next()).as("No more result in result set").isFalse();
        }
    }

    @Test
    public void persists_changeset_but_does_execute_it_when_precondition_is_not_met_and_configured_to_mark_as_executed()
        throws Exception {
        Precondition precondition = precondition(PreconditionErrorPolicy.MARK_AS_EXECUTED, "RETURN false AS result");
        Changeset changeset = changeset("identifier", "fbiville", "CREATE (n: SomeNode {text:'yeah'})", precondition);

        writer.write(singletonList(changeset));

        try (StatementWrapper statement = jdbcGraph.connection().createStatement();
            ResultSetWrapper resultSet = statement.executeQuery("OPTIONAL MATCH  (node: SomeNode) " +
                "WITH node " +
                "MATCH  (changelog:__LiquigraphChangelog)<-[ewc:EXECUTED_WITHIN_CHANGELOG]-(changeset:__LiquigraphChangeset), "
                +
                "       (changeset)<-[:EXECUTED_WITHIN_CHANGESET]-(query:__LiquigraphQuery) " +
                "RETURN ewc.time AS time, changeset, COLLECT(query.query) AS queries, node")) {

            assertThat(resultSet.next()).as("Result set should contain 1 row").isTrue();
            assertThatChangesetIsStored(resultSet);
            assertThatQueryIsNotExecuted(resultSet);
            assertThat(resultSet.next()).as("No more result in result set").isFalse();
        }
    }

    @Test
    public void executes_changeset_with_multiple_queries() throws Exception {
        Changeset changeset =
            changeset("id", "fbiville", asList("CREATE (n:Human) RETURN n", "MATCH (n:Human) SET n.age = 42 RETURN n"));

        writer.write(singletonList(changeset));

        try (StatementWrapper transaction = jdbcGraph.connection().createStatement();
            ResultSetWrapper resultSet = transaction.executeQuery("MATCH (n:Human) RETURN n.age AS age")) {

            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getLong("age")).isEqualTo(42);
            assertThat(resultSet.next()).as("No more result in result set").isFalse();
        }

        try (StatementWrapper transaction = jdbcGraph.connection().createStatement();
            ResultSetWrapper resultSet = transaction
                .executeQuery("MATCH (queries:__LiquigraphQuery) RETURN COLLECT(queries.query) AS query")) {

            assertThat(resultSet.next()).isTrue();
            assertThat((Collection<String>) resultSet.getObject("query"))
                .containsExactly("CREATE (n:Human) RETURN n", "MATCH (n:Human) SET n.age = 42 RETURN n");
            assertThat(resultSet.next()).as("No more result in result set").isFalse();
        }
    }

    private Precondition precondition(PreconditionErrorPolicy policy, String query) {
        Precondition precondition = new Precondition();
        precondition.setPolicy(policy);
        SimpleQuery simpleQuery = new SimpleQuery();
        simpleQuery.setQuery(query);
        precondition.setQuery(simpleQuery);
        return precondition;
    }

    private Changeset changeset(String identifier, String author, String query, Precondition precondition) {
        Changeset changeset = changeset(identifier, author, query);
        changeset.setPrecondition(precondition);
        return changeset;
    }

    private Changeset changeset(String identifier, String author, String query) {
        Collection<String> queries = singletonList(query);
        return changeset(identifier, author, queries);
    }

    private Changeset changeset(String identifier, String author, Collection<String> queries) {
        Changeset changeset = new Changeset();
        changeset.setId(identifier);
        changeset.setAuthor(author);
        changeset.setQueries(queries);
        return changeset;
    }

    private static void assertThatQueryIsExecutedAndHistoryPersisted(ConnectionWrapper connection) throws Exception {
        try (StatementWrapper statement = connection.createStatement();
            ResultSetWrapper resultSet = statement.executeQuery("MATCH  (node: SomeNode), " +
                "       (changelog:__LiquigraphChangelog)<-[ewc:EXECUTED_WITHIN_CHANGELOG]-(changeset:__LiquigraphChangeset), "
                +
                "       (changeset)<-[:EXECUTED_WITHIN_CHANGESET]-(query:__LiquigraphQuery) " +
                "RETURN ewc.time AS time, changeset, COLLECT(query.query) AS queries, node")) {

            assertThat(resultSet.next()).as("Result set should contain 1 row").isTrue();
            assertThatChangesetIsStored(resultSet);
            assertThatQueryIsExecuted(resultSet);
            assertThat(resultSet.next()).as("No more result in result set").isFalse();
        }
    }

    private static void assertThatChangesetIsStored(ResultSetWrapper resultSet) throws Exception {
        assertThat(resultSet.getLong("time")).isGreaterThan(0);

        assertThat((Collection<String>) resultSet.getObject("queries"))
            .containsExactly("CREATE (n: SomeNode {text:'yeah'})");

        Node changeset = (Node) resultSet.getObject("changeset");
        assertThat(changeset.getProperty("id")).isEqualTo("identifier");
        assertThat(changeset.getProperty("author")).isEqualTo("fbiville");
        assertThat(changeset.getProperty("checksum"))
            .isEqualTo(checksum(singletonList("CREATE (n: SomeNode {text:'yeah'})")));
    }

    private static void assertThatQueryIsExecuted(ResultSetWrapper resultSet) throws Exception {
        Node node = (Node) resultSet.getObject("node");
        assertThat(node.getLabels()).containsExactly(DynamicLabel.label("SomeNode"));
        assertThat(node.getProperty("text")).isEqualTo("yeah");
    }

    private static void assertThatQueryIsNotExecuted(ResultSetWrapper resultSet) throws Exception {
        assertThat(resultSet.getObject("node")).isNull();
    }
}
