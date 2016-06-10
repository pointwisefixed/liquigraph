package org.liquigraph.connector.connection;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.graphdb.Node;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

/**
 * Created by wal-mart on 4/28/16.
 *
 * @author: wal-mart
 * @author: grosal3
 */
public class ResultSetWrapper implements AutoCloseable {

    private ResultSet resultSet;
    private StatementResult result;
    private Record currentRecord;

    public ResultSetWrapper(ResultSet resultSet) {
        this.resultSet = resultSet;
    }

    public ResultSetWrapper(StatementResult result) {
        this.result = result;
    }

    public boolean next() throws Exception {
        if (resultSet != null) {
            return resultSet.next();
        } else {
            if (result.hasNext()) {
                currentRecord = result.next();
                return true;
            } else {
                return false;
            }
        }

    }

    public <T> Collection<T> getAsList(String columnName) throws Exception {
        if (resultSet != null) {
            Collection<T> rs = (Collection<T>) resultSet.getObject(columnName);
            return rs;
        } else {
            return (Collection<T>) currentRecord.get(columnName).asList();
        }
    }

    public boolean getBoolean(String columnName) throws Exception {
        if (resultSet != null) {
            return resultSet.getBoolean(columnName);
        } else {
            return currentRecord.get(columnName).asBoolean();
        }
    }

    public Object getObject(String columnName) throws Exception {
        if (resultSet != null) {
            Object result = resultSet.getObject(columnName);
            if (result instanceof org.neo4j.graphdb.Node) {
                return new NodeAdaptor((Node) result);
            } else if (result instanceof Map) {
                return result;
            }
            return null;
        } else {
            Value val = currentRecord.get(columnName);
            return val == Values.NULL ? null : new NodeAdaptor(val);
        }
    }

    @Override
    public void close() throws Exception {
        if (resultSet != null) {
            resultSet.close();
        }
    }

    public Long getLong(String column) throws SQLException {
        if (resultSet != null) {
            return resultSet.getLong(column);
        } else {
            return currentRecord.get(column).asLong();
        }
    }
}
