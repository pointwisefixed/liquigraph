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
import org.liquigraph.connector.connection.PreparedStatementWrapper;
import org.liquigraph.connector.connection.StatementWrapper;
import org.liquigraph.connector.transaction.IncludedTransaction;
import org.liquigraph.model.Changeset;
import org.liquigraph.model.Precondition;
import org.liquigraph.model.exception.PreconditionNotMetException;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Throwables.propagate;
import static com.google.common.collect.Iterables.get;
import static java.text.MessageFormat.format;
import static org.liquigraph.connector.io.PreconditionResult.NO_PRECONDITION;

public class ChangelogGraphWriter implements ChangelogWriter {

    private static final String CHANGESET_UPSERT = "MERGE (changelog:__LiquigraphChangelog) " +
        "MERGE (changelog)<-[ewc:EXECUTED_WITHIN_CHANGELOG]-(changeset:__LiquigraphChangeset {id: {1}}) " +
        "ON MATCH SET  changeset.checksum = {2} " +
        "ON CREATE SET changeset.checksum = {2}, " +
        "              changeset.author = {3}, " +
        "              ewc.time = timestamp() " +
        "WITH changeset " +
        // deletes previous stored queries, if any
        "OPTIONAL MATCH (changeset)<-[eq:EXECUTED_WITHIN_CHANGESET]-(queries:__LiquigraphQuery) " +
        "DELETE eq, queries " +
        "WITH changeset " +
        "RETURN changeset ";

    private static final String QUERY_UPSERT =
        // stores the possibly updated queries
        "MATCH (changeset:__LiquigraphChangeset {id: {1}, author: {2}}) " +
            "WITH changeset " +
            "CREATE (changeset)<-[:EXECUTED_WITHIN_CHANGESET {order:{3}}]-(:__LiquigraphQuery {query: {4}})";

    private final ConnectionWrapper connection;
    private final PreconditionExecutor preconditionExecutor;

    public ChangelogGraphWriter(ConnectionWrapper connection, PreconditionExecutor preconditionExecutor) {
        this.connection = connection;
        this.preconditionExecutor = preconditionExecutor;
    }

    /**
     * Runs the set of migrations against the configured database and inserts them
     * in the persisted migration graph.
     *
     * Please note that these two operations are performed in two separate transactions,
     * as user-defined migrations may operate on indices and those need be run apart
     * from data changes.
     */
    @Override
    public void write(Collection<Changeset> changelog) {
        for (Changeset changeset : changelog) {
            StatementExecution statementExecution = executeStatement(changeset);
            if (statementExecution == StatementExecution.IGNORE_FAILURE) {
                continue;
            }
            insertChangeset(connection, changeset);
        }
    }

    private StatementExecution executeStatement(Changeset changeset) {
        try (StatementWrapper statement = connection.createStatement()) {
            Precondition precondition = changeset.getPrecondition();
            PreconditionResult preconditionResult = executePrecondition(precondition);

            if (preconditionResult.executedSuccessfully()) {
                for (String query : changeset.getQueries()) {
                    statement.execute(query);
                }
            } else {
                switch (preconditionResult.errorPolicy()) {
                    /*
                     * ignore MARK_AS_EXECUTED:
                     * the changeset should just be inserted in the history graph
                     * without actually being executed
                     */
                    case CONTINUE:
                        return StatementExecution.IGNORE_FAILURE;
                    case FAIL:
                        throw new PreconditionNotMetException(
                            format("Changeset <%s>: precondition query %s failed with policy <%s>. Aborting.",
                                changeset.getId(), precondition.getQuery(), precondition.getPolicy()));
                }
            }
            connection.commit(new IncludedTransaction());
        } catch (Exception e) {
            throw propagate(e);
        }
        return StatementExecution.SUCCESS;
    }

    private PreconditionResult executePrecondition(Precondition precondition) {
        if (precondition == null) {
            return NO_PRECONDITION;
        }
        try (StatementWrapper ignored = connection.createStatement()) {
            return preconditionExecutor.executePrecondition(connection, precondition);
        } catch (Exception e) {
            throw propagate(e);
        }
    }

    private void insertChangeset(ConnectionWrapper connection, Changeset changeset) {
        try (PreparedStatementWrapper changesetStmt = connection.prepareStatement(CHANGESET_UPSERT);
            PreparedStatementWrapper queryStmt = connection.prepareStatement(QUERY_UPSERT)) {

            insertChangesetNode(changeset, changesetStmt);
            insertQueryNodes(changeset, queryStmt);

            connection.commit();
        } catch (Exception e) {
            throw propagate(e);
        }
    }

    private void insertChangesetNode(Changeset changeset, PreparedStatementWrapper changesetStmt) throws Exception {
        populateChangesetStatement(changeset, changesetStmt);
        changesetStmt.execute();
    }

    private void insertQueryNodes(Changeset changeset, PreparedStatementWrapper queryStmt) throws Exception {
        queryStmt.setString(1, changeset.getId());
        queryStmt.setString(2, changeset.getAuthor());
        Collection<String> queries = changeset.getQueries();
        for (int i = 0; i < queries.size(); i++) {
            queryStmt.setInt(3, i);
            queryStmt.setString(4, get(queries, i));
            queryStmt.execute();
        }
    }

    private void populateChangesetStatement(Changeset changeset, PreparedStatementWrapper changesetStmt) throws Exception {
        for (Integer key : changesetParameters(changeset).keySet()) {
            changesetStmt.setObject(key, changesetParameters(changeset).get(key));
        }
    }

    private Map<Integer, Object> changesetParameters(Changeset changeset) {
        String checksum = changeset.getChecksum();
        Map<Integer, Object> parameters = new HashMap<>();
        parameters.put(1, changeset.getId());
        parameters.put(2, checksum);
        parameters.put(3, changeset.getAuthor());
        return parameters;
    }

    private enum StatementExecution {
        SUCCESS, IGNORE_FAILURE;
    }
}
