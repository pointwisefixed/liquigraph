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
package org.liquigraph.connector.io;

import org.liquigraph.connector.connection.ConnectionWrapper;
import org.liquigraph.connector.connection.NodeAdaptor;
import org.liquigraph.connector.connection.ResultSetWrapper;
import org.liquigraph.connector.connection.StatementWrapper;
import org.liquigraph.model.Changeset;
import org.neo4j.driver.internal.value.ListValue;
import org.neo4j.driver.v1.Value;
import org.neo4j.graphdb.Node;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

import static com.google.common.base.Throwables.propagate;
import static com.google.common.collect.Lists.newLinkedList;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableCollection;

public class ChangelogGraphReader {

    private static final String MATCH_CHANGESETS =
        "MATCH (changelog:__LiquigraphChangelog)<-[exec:EXECUTED_WITHIN_CHANGELOG]-(changeset:__LiquigraphChangeset) " +
            "WITH changeset, exec " +
            "MATCH (changeset)<-[execChangeSet:EXECUTED_WITHIN_CHANGESET]-(query:__LiquigraphQuery) " +
            "WITH changeset, query, exec " +
            "ORDER BY exec.time ASC, execChangeSet.order ASC " +
            "WITH changeset, COLLECT(query.query) AS queries, exec " +
            "RETURN {" +
            "   id: changeset.id, " +
            "   author:changeset.author, " +
            "   checksum:changeset.checksum, " +
            "   query:queries" +
            "} AS changeset";

    public final Collection<Changeset> read(ConnectionWrapper connection) {
        Collection<Changeset> changesets = newLinkedList();
        try (StatementWrapper statement = connection.createStatement();
            ResultSetWrapper result = statement.executeQuery(MATCH_CHANGESETS)) {
            while (result.next()) {
                changesets.add(mapRow(result.getObject("changeset")));
            }
            connection.commit();
        } catch (Exception e) {
            throw propagate(e);
        }
        return changesets;
    }

    @SuppressWarnings("unchecked")
    private Changeset mapRow(Object line) throws SQLException {
        if (line instanceof NodeAdaptor) {
            return changeset((NodeAdaptor) line);
        }
        if (line instanceof Map) {
            return changeset((Map<String, Object>) line);
        }
        throw new IllegalArgumentException(format("Unsupported row.\n\t" + "Cannot parse: %s", line));
    }

    private Changeset changeset(NodeAdaptor node) {
        Changeset changeset = new Changeset();
        changeset.setAuthor(String.valueOf(node.getPropertyAsString("author")));
        changeset.setId(String.valueOf(node.getPropertyAsString("id")));
        changeset.setQueries(adaptQueries(node.get("query")));
        changeset.setChecksum(String.valueOf(node.getPropertyAsString("checksum")));
        return changeset;
    }

    private Changeset changeset(Map<String, Object> node) {
        Changeset changeset = new Changeset();
        changeset.setAuthor(String.valueOf(node.get("author")));
        changeset.setId(String.valueOf(node.get("id")));
        changeset.setQueries(adaptQueries(node.get("query")));
        changeset.setChecksum(String.valueOf(node.get("checksum")));
        return changeset;
    }

    private Collection<String> adaptQueries(Object rawQuery) {
        if (rawQuery instanceof ListValue) {
            return ((ListValue) rawQuery).asList(Value::asString);
        }
        return unmodifiableCollection((Collection<String>) rawQuery);
    }
}
