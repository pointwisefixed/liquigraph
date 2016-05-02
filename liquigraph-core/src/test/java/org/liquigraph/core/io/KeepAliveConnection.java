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

import org.liquigraph.connector.connection.*;
import org.liquigraph.connector.connectionctio.ClobWrapper;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * Prevents Liquigraph from closing the delegate
 * before tests can re-use it.
 *
 * Related to https://github.com/neo4j-contrib/neo4j-jdbc/issues/55.
 */
public class KeepAliveConnection implements ConnectionWrapper {

    private final ConnectionWrapper delegate;

    public KeepAliveConnection(ConnectionWrapper delegate) {
        this.delegate = delegate;
    }


    @Override
    public void close() throws Exception {
    }

    @Override
    public StatementWrapper createStatement() throws Exception {
        return delegate.createStatement();
    }

    @Override
    public void commit(TransactionWrapper wrapper) throws Exception {
        delegate.commit();
    }

    @Override
    public PreparedStatementWrapper prepareStatement(String sql) throws Exception {
        return delegate.prepareStatement(sql);
    }

    @Override
    public CallableStatementWrapper prepareCall(String sql) throws Exception {
        return delegate.prepareCall(sql);
    }

    @Override
    public String nativeSQL(String sql) throws Exception {
        return delegate.nativeSQL(sql);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws Exception {
        delegate.setAutoCommit(autoCommit);
    }

    @Override
    public boolean getAutoCommit() throws Exception {
        return delegate.getAutoCommit();
    }

    @Override
    public void commit() throws Exception {
        delegate.commit();
    }

    @Override
    public boolean isClosed() throws Exception {
        return delegate.isClosed();
    }

    @Override
    public DatabaseMetaDataWrapper getMetaData() throws Exception {
        return delegate.getMetaData();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws Exception {
        delegate.setReadOnly(readOnly);
    }

    @Override
    public boolean isReadOnly() throws Exception {
        return delegate.isReadOnly();
    }

    @Override
    public void setCatalog(String catalog) throws Exception {
        delegate.setCatalog(catalog);
    }

    @Override
    public String getCatalog() throws Exception {
        return delegate.getCatalog();
    }

    @Override
    public void setTransactionIsolation(int level) throws Exception {
        delegate.setTransactionIsolation(level);
    }

    @Override
    public int getTransactionIsolation() throws Exception {
        return delegate.getTransactionIsolation();
    }

    @Override
    public WarningWrapper getWarnings() throws Exception {
        return delegate.getWarnings();
    }

    @Override
    public void clearWarnings() throws Exception {
        delegate.clearWarnings();
    }

    @Override
    public StatementWrapper createStatement(int resultSetType, int resultSetConcurrency) throws Exception {
        return delegate.createStatement(resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatementWrapper prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
        throws Exception {
        return delegate.prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public CallableStatementWrapper prepareCall(String sql, int resultSetType, int resultSetConcurrency)
        throws Exception {
        return delegate.prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws Exception {
        return delegate.getTypeMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws Exception {
        delegate.setTypeMap(map);
    }

    @Override
    public void setHoldability(int holdability) throws Exception {
        delegate.setHoldability(holdability);
    }

    @Override
    public int getHoldability() throws Exception {
        return delegate.getHoldability();
    }

    @Override
    public SavepointWrapper setSavepoint(String name) throws Exception {
        return delegate.setSavepoint(name);
    }

    @Override
    public void rollback(SavepointWrapper savepoint) throws Exception {
        delegate.rollback(savepoint);
    }

    @Override
    public void releaseSavepoint(SavepointWrapper savepoint) throws Exception {
        delegate.releaseSavepoint(savepoint);
    }

    @Override
    public StatementWrapper createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
        throws Exception {
        return delegate.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatementWrapper prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
        int resultSetHoldability) throws Exception {
        return delegate.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public CallableStatementWrapper prepareCall(String sql, int resultSetType, int resultSetConcurrency,
        int resultSetHoldability) throws Exception {
        return delegate.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatementWrapper prepareStatement(String sql, int autoGeneratedKeys) throws Exception {
        return delegate.prepareStatement(sql, autoGeneratedKeys);
    }

    @Override
    public PreparedStatementWrapper prepareStatement(String sql, int[] columnIndexes) throws Exception {
        return delegate.prepareStatement(sql, columnIndexes);
    }

    @Override
    public PreparedStatementWrapper prepareStatement(String sql, String[] columnNames) throws Exception {
        return delegate.prepareStatement(sql, columnNames);
    }

    @Override
    public ClobWrapper createClob() throws Exception {
        return delegate.createClob();
    }

    @Override
    public BlobWrapper createBlob() throws Exception {
        return delegate.createBlob();
    }

    @Override
    public NClobWrapper createNClob() throws Exception {
        return delegate.createNClob();
    }

    @Override
    public XmlWrapper createXML() throws Exception {
        return delegate.createXML();
    }

    @Override
    public boolean isValid(int timeout) throws Exception {
        return delegate.isValid(timeout);
    }

    @Override
    public void setClientInfo(String name, String value) throws Exception {
        delegate.setClientInfo(name, value);
    }

    @Override
    public void setClientInfo(Properties properties) throws Exception {
        delegate.setClientInfo(properties);
    }

    @Override
    public Properties getClientInfo() throws Exception {
        return delegate.getClientInfo();
    }

    @Override
    public ArrayWrapper createArrayOf(String typeName, Object[] elements) throws Exception {
        return delegate.createArrayOf(typeName, elements);
    }

    @Override
    public StructWrapper createStruct(String typeName, Object[] attributes) throws Exception {
        return delegate.createStruct(typeName, attributes);
    }

    @Override
    public void setSchema(String schema) throws Exception {
        delegate.setSchema(schema);
    }

    @Override
    public String getSchema() throws Exception {
        return delegate.getSchema();
    }

    @Override
    public void abort(Executor executor) throws Exception {
        delegate.abort(executor);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws Exception {
        delegate.setNetworkTimeout(executor, milliseconds);
    }

    @Override
    public int getNetworkTimeout() throws Exception {
        return delegate.getNetworkTimeout();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws Exception {
        return delegate.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws Exception {
        return delegate.isWrapperFor(iface);
    }
}
