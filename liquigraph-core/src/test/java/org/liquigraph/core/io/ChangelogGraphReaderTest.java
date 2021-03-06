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

import org.junit.Rule;
import org.junit.Test;
import org.liquigraph.connector.connection.ConnectionWrapper;
import org.liquigraph.connector.connection.StatementWrapper;
import org.liquigraph.connector.io.ChangelogGraphReader;
import org.liquigraph.core.EmbeddedGraphDatabaseRule;
import org.liquigraph.model.Changeset;

import java.util.Collection;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.liquigraph.model.Checksums.checksum;

public class ChangelogGraphReaderTest {

    @Rule public EmbeddedGraphDatabaseRule graph = new EmbeddedGraphDatabaseRule("neotest");
    private ChangelogGraphReader reader = new ChangelogGraphReader();

    @Test
    public void reads_changelog_from_graph_database() throws Exception {
        try (ConnectionWrapper connection = graph.connection()) {
            String query = "MATCH n RETURN n";
            given_inserted_data(format("CREATE (:__LiquigraphChangelog)<-[:EXECUTED_WITHIN_CHANGELOG {time:1}]-" +
                    "(:__LiquigraphChangeset {" +
                    "   author:'fbiville'," +
                    "   id:'test'," +
                    "   checksum:'%s'" +
                    "})<-[:EXECUTED_WITHIN_CHANGESET]-(:__LiquigraphQuery {query: '%s'})", checksum(singletonList(query)),
                query), connection);

            Collection<Changeset> changesets = reader.read(graph.connection());

            assertThat(changesets).hasSize(1);
            Changeset changeset = changesets.iterator().next();
            assertThat(changeset.getId()).isEqualTo("test");
            assertThat(changeset.getChecksum()).isEqualTo(checksum(singletonList(query)));
            assertThat(changeset.getAuthor()).isEqualTo("fbiville");
            assertThat(changeset.getQueries()).containsExactly(query);
            connection.commit();
        }
    }

    @Test
    public void reads_changeset_with_multiple_queries() throws Exception {
        try (ConnectionWrapper connection = graph.connection()) {
            given_inserted_data(format("CREATE     (:__LiquigraphChangelog)<-[:EXECUTED_WITHIN_CHANGELOG {time:1}]-" +
                    "           (changeset:__LiquigraphChangeset {" +
                    "               author:'fbiville'," +
                    "               id:'test'," +
                    "               checksum:'%s'" +
                    "           }), " +
                    "           (changeset)<-[:EXECUTED_WITHIN_CHANGESET {order: 1} ]-(:__LiquigraphQuery {query: '%s'}), "
                    +
                    "           (changeset)<-[:EXECUTED_WITHIN_CHANGESET {order: 0} ]-(:__LiquigraphQuery {query: '%s'}), "
                    +
                    "           (changeset)<-[:EXECUTED_WITHIN_CHANGESET {order: 2}]-(:__LiquigraphQuery {query: '%s'})",
                checksum(asList("MATCH m RETURN m", "MATCH n RETURN n", "Match o Return o")), "MATCH n RETURN n",
                "MATCH m RETURN m", "MATCH o RETURN o"), connection);

            Collection<Changeset> changesets = reader.read(graph.connection());

            assertThat(changesets).hasSize(1);
            Changeset changeset = changesets.iterator().next();
            assertThat(changeset.getId()).isEqualTo("test");
            assertThat(changeset.getChecksum())
                .isEqualTo(checksum(asList("MATCH m RETURN m", "MATCH n RETURN n", "Match o Return o")));
            assertThat(changeset.getAuthor()).isEqualTo("fbiville");
            assertThat(changeset.getQueries())
                .containsExactly("MATCH m RETURN m", "MATCH n RETURN n", "MATCH o RETURN o");
            connection.commit();
        }
    }

    private void given_inserted_data(String query, ConnectionWrapper connection) throws Exception {
        try (StatementWrapper stmt = connection.createStatement()) {
            stmt.execute(query);
        }
    }
}
