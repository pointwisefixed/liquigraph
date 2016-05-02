package org.liquigraph.connector.connection;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;

import java.sql.ResultSet;
import java.sql.SQLException;

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

    public boolean getBoolean(String columnName) throws Exception {
        if (resultSet != null) {
            return resultSet.getBoolean(columnName);
        } else {
            return currentRecord.get(columnName).asBoolean();
        }
    }

    public Object getObject(String columnName) throws Exception {
        if (resultSet != null) {
            return resultSet.getObject(columnName);
        } else {
            return currentRecord.get(columnName).asObject();
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
        }
        return null;
    }
}
