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

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.liquigraph.connector.BoltConnector;
import org.liquigraph.connector.GraphJdbcConnector;
import org.liquigraph.connector.LiquigraphConnector;
import org.liquigraph.connector.configuration.ConfigurationBuilder;
import org.liquigraph.connector.connection.ConnectionWrapper;
import org.liquigraph.connector.io.lock.LockableConnection;
import org.neo4j.jdbc.Neo4jConnection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.MapEntry.entry;

public class GraphJdbcConnectorTest {

    @Rule public ExpectedException thrown = ExpectedException.none();

    private LiquigraphConnector connector = new GraphJdbcConnector();
    private boolean isBolt = true;

    @Test
    @Ignore("requires starting local Neo4j instance")
    public void instantiates_a_local_graph_database() throws Exception {
        String uri = "jdbc:neo4j:mem";
        if(isBolt){
            connector = new BoltConnector();
            uri = "bolt://localhost:7887";
        }
        try (ConnectionWrapper connection = connector.getConnection(
            new ConfigurationBuilder().withRunMode().withMasterChangelogLocation("changelog/changelog.xml")
                .withUri(uri).build())) {
            assertThat(connection).isInstanceOf(LockableConnection.class);
        }

    }

    @Test
    @Ignore("requires starting local Neo4j instance")
    public void instantiates_a_remote_graph_database() throws Exception {
        try (ConnectionWrapper connection = connector.getConnection(
            new ConfigurationBuilder().withRunMode().withMasterChangelogLocation("changelog.xml")
                .withUri("jdbc:neo4j://localhost:7474").withUsername("neo4j").withPassword("toto").build())) {

            assertThat(((Neo4jConnection) connection).getProperties())
                .contains(entry("user", "neo4j"), entry("password", "toto"));
        }
    }

}
