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
package org.liquigraph.core.api;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.liquigraph.connector.configuration.ConfigurationBuilder;
import org.liquigraph.connector.connection.ConnectionWrapper;
import org.liquigraph.connector.connection.PreparedStatementWrapper;
import org.liquigraph.connector.connection.ResultSetWrapper;
import org.liquigraph.connector.connection.StatementWrapper;
import org.liquigraph.core.EmbeddedGraphDatabaseRule;
import org.liquigraph.core.io.FixedConnectionConnector;

import static org.assertj.core.api.Assertions.assertThat;

public class LiquigraphTest {

    @Rule public EmbeddedGraphDatabaseRule graphDb = new EmbeddedGraphDatabaseRule("neo");
    private Liquigraph liquigraph;

    @Before
    public void prepare() {
        liquigraph = new Liquigraph(new FixedConnectionConnector(graphDb.connection()));
    }

    @Test
    public void runs_migrations_against_embedded_graph_with_failed_precondition_whose_changeset_is_marked_as_executed()
        throws Exception {
        liquigraph.runMigrations(
            new ConfigurationBuilder().withRunMode().withMasterChangelogLocation("changelog/changelog-with-1-node.xml")
                .withUri(graphDb.uri()).build());

        try (ConnectionWrapper connection = graphDb.connection()) {
            try (StatementWrapper statement = connection.createStatement();
                ResultSetWrapper resultSet = statement
                    .executeQuery("MATCH (human:Human {name: 'fbiville'}) RETURN human")) {

                assertThat(resultSet.next()).isTrue();
                assertThat(resultSet.next()).isFalse();
                connection.commit();
            }

            try (PreparedStatementWrapper statement = connection
                .prepareStatement("MATCH (changeset:__LiquigraphChangeset {id: {1}}) RETURN changeset")) {

                statement.setObject(1, "insert-fbiville");
                ResultSetWrapper resultSet = statement.executeQuery();
                assertThat(resultSet.next()).isTrue();
                assertThat(resultSet.next()).isFalse();

                statement.setObject(1, "insert-fbiville-again");
                resultSet = statement.executeQuery();
                assertThat(resultSet.next()).isTrue();
                assertThat(resultSet.next()).isFalse();
                connection.commit();
            }
        }
    }

    @Test
    public void runs_migrations_with_schema_changes() throws Exception {
        liquigraph.runMigrations(
            new ConfigurationBuilder().withRunMode().withMasterChangelogLocation("schema/schema-changelog.xml")
                .withUri(graphDb.uri()).build());

        try (ConnectionWrapper connection = graphDb.connection();
            StatementWrapper statement = connection.createStatement();
            ResultSetWrapper resultSet = statement.executeQuery("MATCH (foo:Foo {bar: 123}) RETURN foo")) {

            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.next()).isFalse();
            connection.commit();
        }
    }

    @Test
    public void runs_migrations_with_schema_changes_and_preconditions() throws Exception {
        liquigraph.runMigrations(new ConfigurationBuilder().withRunMode()
            .withMasterChangelogLocation("schema/schema-preconditions-changelog.xml").withUri(graphDb.uri()).build());

        try (ConnectionWrapper connection = graphDb.connection();
            StatementWrapper statement = connection.createStatement();
            ResultSetWrapper resultSet = statement.executeQuery("MATCH (foo:Foo {bar: 123}) RETURN foo")) {

            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.next()).isFalse();
            connection.commit();
        }
    }

}
