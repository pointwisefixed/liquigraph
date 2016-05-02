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
package org.liquigraph.connector.io.lock;

import org.liquigraph.connector.connection.*;
import org.liquigraph.connector.connectionctio.ClobWrapper;
import org.liquigraph.model.exception.LiquigraphLockException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.Exception;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executor;

/**
 * This JDBC connection decorator writes a (:__LiquigraphLock)
 * Neo4j node in order to prevent concurrent executions.
 *
 * Closing this connection will delete the created "Lock" node.
 *
 * A shutdown hook is executed to remove the lock node if and
 * only if the connection has not been properly closed.
 */
public final class LockableConnection implements ConnectionWrapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(LockableConnection.class);
    private final ConnectionWrapper delegate;
    private final UUID uuid;
    private final Thread task;

    private LockableConnection(ConnectionWrapper delegate) {
        this.delegate = delegate;
        this.uuid = UUID.randomUUID();
        this.task = new Thread(new ShutdownTask(this));
    }

    public static LockableConnection acquire(ConnectionWrapper delegate) {
        LockableConnection connection = new LockableConnection(delegate);
        try {
            connection.acquireLock();
            return connection;
        } catch (RuntimeException e) {
            try {
                connection.close();
            } catch (Exception exception) {
                e.addSuppressed(exception);
            }
            throw e;
        }
    }

    /**
     * Removes lock node and removes the related cleanup
     * task shutdown hook before closing the underlying
     * connection.
     *
     * @see ShutdownTask
     */
    @Override
    public void close() throws Exception {
        removeShutdownHook();
        releaseLock();
        delegate.close();
    }

    public StatementWrapper createStatement() throws Exception {
        return delegate.createStatement();
    }

    public void commit() throws Exception {
        delegate.commit();
    }


    public void commit(TransactionWrapper wrapper) throws Exception {
        delegate.commit(wrapper);
    }

    public NClobWrapper createNClob() throws Exception {
        return delegate.createNClob();
    }

    public Map<String, Class<?>> getTypeMap() throws Exception {
        return delegate.getTypeMap();
    }

    public void setHoldability(int holdability) throws Exception {
        delegate.setHoldability(holdability);
    }

    public void releaseSavepoint(SavepointWrapper savepoint) throws Exception {
        delegate.releaseSavepoint(savepoint);
    }

    public ArrayWrapper createArrayOf(String typeName, Object[] elements) throws Exception {
        return delegate.createArrayOf(typeName, elements);
    }

    public PreparedStatementWrapper prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws Exception {
        return delegate.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    public StructWrapper createStruct(String typeName, Object[] attributes) throws Exception {
        return delegate.createStruct(typeName, attributes);
    }

    public void setAutoCommit(boolean autoCommit) throws Exception {
        delegate.setAutoCommit(autoCommit);
    }

    public void setReadOnly(boolean readOnly) throws Exception {
        delegate.setReadOnly(readOnly);
    }

    @Override
    public Properties getClientInfo() throws Exception {
        return delegate.getClientInfo();
    }

    public void setTransactionIsolation(int level) throws Exception {
        delegate.setTransactionIsolation(level);
    }

    public void setTypeMap(Map<String, Class<?>> map) throws Exception {
        delegate.setTypeMap(map);
    }

    public void setClientInfo(String name, String value) throws Exception {
        delegate.setClientInfo(name, value);
    }

    public PreparedStatementWrapper prepareStatement(String sql) throws Exception {
        return delegate.prepareStatement(sql);
    }

    public boolean isClosed() throws Exception {
        return delegate.isClosed();
    }

    public boolean isReadOnly() throws Exception {
        return delegate.isReadOnly();
    }

    public SavepointWrapper setSavepoint(String name) throws Exception {
        return delegate.setSavepoint(name);
    }

    public int getNetworkTimeout() throws Exception {
        return delegate.getNetworkTimeout();
    }

    public String getSchema() throws Exception {
        return delegate.getSchema();
    }

    public String getCatalog() throws Exception {
        return delegate.getCatalog();
    }

    public void setCatalog(String catalog) throws Exception {
        delegate.setCatalog(catalog);
    }

    public boolean isWrapperFor(Class<?> iface) throws Exception {
        return delegate.isWrapperFor(iface);
    }

    public StatementWrapper createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws Exception {
        return delegate.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    public void abort(Executor executor) throws Exception {
        delegate.abort(executor);
    }

    public DatabaseMetaDataWrapper getMetaData() throws Exception {
        return delegate.getMetaData();
    }

    public XmlWrapper createXML() throws Exception {
        return delegate.createXML();
    }

    public void rollback(SavepointWrapper wrapper) throws Exception {
        delegate.rollback(wrapper);
    }

    public CallableStatementWrapper prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws Exception {
        return delegate.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    public void setNetworkTimeout(Executor executor, int milliseconds) throws Exception {
        delegate.setNetworkTimeout(executor, milliseconds);
    }

    public WarningWrapper getWarnings() throws Exception {
        return delegate.getWarnings();
    }

    public boolean getAutoCommit() throws Exception {
        return delegate.getAutoCommit();
    }

    public ClobWrapper createClob() throws Exception {
        return delegate.createClob();
    }

    public <T> T unwrap(Class<T> iface) throws Exception {
        return delegate.unwrap(iface);
    }

    public void setClientInfo(Properties properties) throws Exception {
        delegate.setClientInfo(properties);
    }

    public int getHoldability() throws Exception {
        return delegate.getHoldability();
    }

    public PreparedStatementWrapper prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws Exception {
        return delegate.prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    public String nativeSQL(String sql) throws Exception {
        return delegate.nativeSQL(sql);
    }

    public StatementWrapper createStatement(int resultSetType, int resultSetConcurrency) throws Exception {
        return delegate.createStatement(resultSetType, resultSetConcurrency);
    }

    public PreparedStatementWrapper prepareStatement(String sql, String[] columnNames) throws Exception {
        return delegate.prepareStatement(sql, columnNames);
    }

    public boolean isValid(int timeout) throws Exception {
        return delegate.isValid(timeout);
    }

    public CallableStatementWrapper prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws Exception {
        return delegate.prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    public PreparedStatementWrapper prepareStatement(String sql, int autoGeneratedKeys) throws Exception {
        return delegate.prepareStatement(sql, autoGeneratedKeys);
    }

    public BlobWrapper createBlob() throws Exception {
        return delegate.createBlob();
    }

    public PreparedStatementWrapper prepareStatement(String sql, int[] columnIndexes) throws Exception {
        return delegate.prepareStatement(sql, columnIndexes);
    }

    public int getTransactionIsolation() throws Exception {
        return delegate.getTransactionIsolation();
    }

    public CallableStatementWrapper prepareCall(String sql) throws Exception {
        return delegate.prepareCall(sql);
    }

    public void clearWarnings() throws Exception {
        delegate.clearWarnings();
    }

    public void setSchema(String schema) throws Exception {
        delegate.setSchema(schema);
    }

    final void releaseLock() {
        try (PreparedStatementWrapper statement = prepareStatement(
            "MATCH (lock:__LiquigraphLock {uuid:{1}}) DELETE lock")) {

            statement.setString(1, uuid.toString());
            statement.execute();
            commit();
        } catch (Exception e) {
            LOGGER.error(
                "Cannot remove __LiquigraphLock during cleanup.",
                e
            );
        }
    }

    private  void acquireLock() {
        addShutdownHook();
        ensureLockUnicity();
        tryWriteLock();
    }

    private  void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(task);
    }

    private  void removeShutdownHook() {
        Runtime.getRuntime().removeShutdownHook(task);
    }

    private void ensureLockUnicity() {
        try (StatementWrapper statement = delegate.createStatement()) {
            statement.execute("CREATE CONSTRAINT ON (lock:__LiquigraphLock) ASSERT lock.name IS UNIQUE");
            commit();
        }
        catch (Exception e) {
            throw new LiquigraphLockException(
                "Could not ensure __LiquigraphLock unicity\n\t" +
                    "Please make sure your instance is in a clean state\n\t" +
                    "No more than 1 lock should be there simultaneously!",
                e
            );
        }
    }

    private void tryWriteLock() {
        try (PreparedStatementWrapper statement = delegate.prepareStatement(
            "CREATE (:__LiquigraphLock {name:'John', uuid:{1}})")) {

            statement.setString(1, uuid.toString());
            statement.execute();
            commit();
        }
        catch (Exception e) {
            throw new LiquigraphLockException(
                "Cannot create __LiquigraphLock lock\n\t" +
                "Likely another Liquigraph execution is going on or has crashed.",
                e
            );
        }
    }
}
