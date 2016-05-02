package org.liquigraph.connector.connection.bolt;

import org.liquigraph.connector.connection.*;
import org.liquigraph.connector.connectionctio.ClobWrapper;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * Created by wal-mart on 4/28/16.
 *
 * @author: wal-mart
 * @author: grosal3
 */
public class BoltConnectionWrapper implements ConnectionWrapper {

    private final Session session;

    public BoltConnectionWrapper(Session session) {
        this.session = session;
    }

    @Override
    public StatementWrapper createStatement() throws Exception {
        return new StatementWrapper(session);
    }

    @Override
    public void commit(TransactionWrapper wrapper) throws Exception {
        wrapper.commit();
    }

    @Override
    public void commit() throws Exception {
        //noop
    }

    @Override
    public PreparedStatementWrapper prepareStatement(String changesetUpsert) throws Exception {
        Statement st = new Statement(changesetUpsert);
        return new PreparedStatementWrapper(session, st);
    }

    @Override
    public void close() throws Exception {
        session.close();
    }

    @Override
    public NClobWrapper createNClob() throws Exception {
        return new NClobWrapper();
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws Exception {
        return new HashMap<>();
    }

    @Override
    public void setHoldability(int holdability) throws Exception {
        //noop
    }

    @Override
    public void releaseSavepoint(SavepointWrapper savepoint) throws Exception {
        //noop
    }

    @Override
    public ArrayWrapper createArrayOf(String typeName, Object[] elements) throws Exception {
        return new ArrayWrapper();
    }

    @Override
    public void rollback(SavepointWrapper savepoint) throws Exception {
        //noop
    }

    @Override
    public StructWrapper createStruct(String typeName, Object[] attributes) throws Exception {
        return null;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws Exception {
        //noop
    }

    @Override
    public void setReadOnly(boolean readOnly) throws Exception {
        //noop
    }

    @Override
    public Properties getClientInfo() throws Exception {
        return new Properties();
    }

    @Override
    public void setTransactionIsolation(int level) throws Exception {
        //noop

    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws Exception {
        //noop

    }

    @Override
    public void setClientInfo(String name, String value) throws Exception {
        //noop

    }

    @Override
    public boolean isClosed() throws Exception {
        return !session.isOpen();
    }

    @Override
    public boolean isReadOnly() throws Exception {
        return false;
    }

    @Override
    public SavepointWrapper setSavepoint(String name) throws Exception {
        return null;
    }

    @Override
    public int getNetworkTimeout() throws Exception {
        return -1;
    }

    @Override
    public String getSchema() throws Exception {
        return null;
    }

    @Override
    public String getCatalog() throws Exception {
        return null;
    }

    @Override
    public void setCatalog(String catalog) throws Exception {

    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws Exception {
        return false;
    }

    @Override
    public StatementWrapper createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
        throws Exception {
        return new StatementWrapper(session);

    }

    @Override
    public void abort(Executor executor) throws Exception {

    }

    @Override
    public DatabaseMetaDataWrapper getMetaData() throws Exception {
        return null;
    }

    @Override
    public XmlWrapper createXML() throws Exception {
        return null;
    }

    @Override
    public void setSchema(String schema) throws Exception {

    }

    @Override
    public void clearWarnings() throws Exception {

    }

    @Override
    public CallableStatementWrapper prepareCall(String sql) throws Exception {
        return new CallableStatementWrapper();
    }

    @Override
    public int getTransactionIsolation() throws Exception {
        return 0;
    }

    @Override
    public PreparedStatementWrapper prepareStatement(String sql, int[] columnIndexes) throws Exception {
        return new PreparedStatementWrapper(session, new Statement(sql));
    }

    @Override
    public PreparedStatementWrapper prepareStatement(String sql, String[] columnNames) throws Exception {
        return new PreparedStatementWrapper(session, new Statement(sql));
    }

    @Override
    public BlobWrapper createBlob() throws Exception {
        return new BlobWrapper();
    }

    @Override
    public boolean isValid(int timeout) throws Exception {
        return true;
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws Exception {
        //noop

    }

    @Override
    public WarningWrapper getWarnings() throws Exception {
        return new WarningWrapper();
    }

    @Override
    public boolean getAutoCommit() throws Exception {
        return false;
    }

    @Override
    public ClobWrapper createClob() throws Exception {
        return new ClobWrapper();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws Exception {
        return null;
    }

    @Override
    public int getHoldability() throws Exception {
        return 0;
    }

    @Override
    public String nativeSQL(String sql) throws Exception {
        return null;
    }

    @Override
    public StatementWrapper createStatement(int resultSetType, int resultSetConcurrency) throws Exception {
        return new StatementWrapper(session);
    }

    @Override
    public PreparedStatementWrapper prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
        throws Exception {
        return new PreparedStatementWrapper(session, new Statement(sql));
    }

    @Override
    public void setClientInfo(Properties properties) throws Exception {

    }

    @Override
    public CallableStatementWrapper prepareCall(String sql, int resultSetType, int resultSetConcurrency,
        int resultSetHoldability) throws Exception {
        return new CallableStatementWrapper();
    }

    @Override
    public PreparedStatementWrapper prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
        int resultSetHoldability) throws Exception {
        return new PreparedStatementWrapper(session, new Statement(sql));
    }

    @Override
    public CallableStatementWrapper prepareCall(String sql, int resultSetType, int resultSetConcurrency)
        throws Exception {
        return new CallableStatementWrapper();
    }

    @Override
    public PreparedStatementWrapper prepareStatement(String sql, int autoGeneratedKeys) throws Exception {
        return new PreparedStatementWrapper(session, new Statement(sql));
    }
}
