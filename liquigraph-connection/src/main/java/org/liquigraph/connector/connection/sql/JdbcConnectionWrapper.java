package org.liquigraph.connector.connection.sql;

import org.liquigraph.connector.connection.*;
import org.liquigraph.connector.connectionctio.ClobWrapper;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * Created by wal-mart on 4/28/16.
 *
 * @author: wal-mart
 * @author: grosal3
 */
public class JdbcConnectionWrapper implements ConnectionWrapper {

    private final Connection connection;

    public JdbcConnectionWrapper(Connection conn) {
        this.connection = conn;
    }

    @Override
    public StatementWrapper createStatement() throws Exception {
        Statement st = connection.createStatement();
        return new StatementWrapper(st);
    }

    @Override
    public void commit(TransactionWrapper wrapper) throws Exception {
        connection.commit();
    }

    @Override
    public void commit() throws Exception {
        connection.commit();
    }

    @Override
    public PreparedStatementWrapper prepareStatement(String changesetUpsert) throws Exception {
        return new PreparedStatementWrapper(connection.prepareStatement(changesetUpsert));
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }

    @Override
    public NClobWrapper createNClob() throws Exception {
        return new NClobWrapper(connection.createNClob());
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws Exception {
        return connection.getTypeMap();
    }

    @Override
    public void setHoldability(int holdability) throws Exception {
        connection.setHoldability(holdability);
    }

    @Override
    public void releaseSavepoint(SavepointWrapper savepoint) throws Exception {
        connection.releaseSavepoint(savepoint.getSavePointWrapper());
    }

    @Override
    public ArrayWrapper createArrayOf(String typeName, Object[] elements) throws Exception {
        return new ArrayWrapper(connection.createArrayOf(typeName, elements));
    }

    @Override
    public void rollback(SavepointWrapper savepoint) throws Exception {
        connection.rollback(savepoint.getSavePointWrapper());
    }

    @Override
    public StructWrapper createStruct(String typeName, Object[] attributes) throws Exception {
        return new StructWrapper(connection.createStruct(typeName, attributes));
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws Exception {
        connection.setAutoCommit(autoCommit);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws Exception {
        connection.setReadOnly(readOnly);
    }

    @Override
    public Properties getClientInfo() throws Exception {
        return connection.getClientInfo();
    }

    @Override
    public void setTransactionIsolation(int level) throws Exception {
        connection.setTransactionIsolation(level);
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws Exception {
        connection.setTypeMap(map);
    }

    @Override
    public void setClientInfo(String name, String value) throws Exception {
        connection.setClientInfo(name, value);
    }

    @Override
    public boolean isClosed() throws Exception {
        return connection.isClosed();
    }

    @Override
    public boolean isReadOnly() throws Exception {
        return connection.isReadOnly();
    }

    @Override
    public SavepointWrapper setSavepoint(String name) throws Exception {
        return new SavepointWrapper(connection.setSavepoint(name));
    }

    @Override
    public int getNetworkTimeout() throws Exception {
        return connection.getNetworkTimeout();
    }

    @Override
    public String getSchema() throws Exception {
        return connection.getSchema();
    }

    @Override
    public String getCatalog() throws Exception {
        return connection.getCatalog();
    }

    @Override
    public void setCatalog(String catalog) throws Exception {
        connection.setCatalog(catalog);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws Exception {
        return connection.isWrapperFor(iface);
    }

    @Override
    public StatementWrapper createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
        throws Exception {

        Statement st = connection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
        return new StatementWrapper(st);

    }

    @Override
    public void abort(Executor executor) throws Exception {
        connection.abort(executor);
    }

    @Override
    public DatabaseMetaDataWrapper getMetaData() throws Exception {
        return new DatabaseMetaDataWrapper(connection.getMetaData());
    }

    @Override
    public XmlWrapper createXML() throws Exception {
        return new XmlWrapper(connection.createSQLXML());
    }

    @Override
    public void setSchema(String schema) throws Exception {
        connection.setSchema(schema);
    }

    @Override
    public void clearWarnings() throws Exception {
        connection.clearWarnings();
    }

    @Override
    public CallableStatementWrapper prepareCall(String sql) throws Exception {
        return new CallableStatementWrapper(connection.prepareCall(sql));
    }

    @Override
    public int getTransactionIsolation() throws Exception {
        return connection.getTransactionIsolation();
    }

    @Override
    public PreparedStatementWrapper prepareStatement(String sql, int[] columnIndexes) throws Exception {
        return new PreparedStatementWrapper(connection.prepareStatement(sql, columnIndexes));
    }

    @Override
    public PreparedStatementWrapper prepareStatement(String sql, String[] columnNames) throws Exception {
        return new PreparedStatementWrapper(connection.prepareStatement(sql, columnNames));
    }

    @Override
    public BlobWrapper createBlob() throws Exception {
        return new BlobWrapper(connection.createBlob());
    }

    @Override
    public boolean isValid(int timeout) throws Exception {
        return connection.isValid(timeout);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws Exception {
        connection.setNetworkTimeout(executor, milliseconds);
    }

    @Override
    public WarningWrapper getWarnings() throws Exception {
        return new WarningWrapper(connection.getWarnings());
    }

    @Override
    public boolean getAutoCommit() throws Exception {
        return connection.getAutoCommit();
    }

    @Override
    public ClobWrapper createClob() throws Exception {
        return new ClobWrapper(connection.createClob());
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws Exception {
        return connection.unwrap(iface);
    }

    @Override
    public int getHoldability() throws Exception {
        return connection.getHoldability();
    }

    @Override
    public String nativeSQL(String sql) throws Exception {
        return connection.nativeSQL(sql);
    }

    @Override
    public StatementWrapper createStatement(int resultSetType, int resultSetConcurrency) throws Exception {
        return new StatementWrapper(connection.createStatement(resultSetType, resultSetConcurrency));
    }

    @Override
    public PreparedStatementWrapper prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
        throws Exception {
        return new PreparedStatementWrapper(connection.prepareStatement(sql, resultSetType, resultSetConcurrency));
    }

    @Override
    public void setClientInfo(Properties properties) throws Exception {
        connection.setClientInfo(properties);
    }

    @Override
    public CallableStatementWrapper prepareCall(String sql, int resultSetType, int resultSetConcurrency,
        int resultSetHoldability) throws Exception {
        return new CallableStatementWrapper(connection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability));
    }

    @Override
    public PreparedStatementWrapper prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
        int resultSetHoldability) throws Exception {
        return new PreparedStatementWrapper(
            connection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability));
    }

    @Override
    public CallableStatementWrapper prepareCall(String sql, int resultSetType, int resultSetConcurrency)
        throws Exception {
        return new CallableStatementWrapper(connection.prepareCall(sql, resultSetType, resultSetConcurrency));
    }

    @Override
    public PreparedStatementWrapper prepareStatement(String sql, int autoGeneratedKeys) throws Exception {
        return new PreparedStatementWrapper(connection.prepareStatement(sql, autoGeneratedKeys));
    }
}
