package org.liquigraph.connector.connection;

import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.Transaction;

import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by wal-mart on 4/28/16.
 *
 * @author: wal-mart
 * @author: grosal3
 */
public class PreparedStatementWrapper implements AutoCloseable {

    private Statement statement;
    private Session session;
    private PreparedStatement preparedStatement;

    public PreparedStatementWrapper(Session session, Statement statement) {

        this.session = session;
        this.statement = statement;
    }

    public PreparedStatementWrapper(PreparedStatement statement) {
        this.preparedStatement = statement;
    }


    public void setString(int i, String s) throws Exception {
        if (preparedStatement != null) {
            preparedStatement.setString(i, s);
        } else {
            Map<String, Object> currentValues = new HashMap<>();
            currentValues.putAll(statement.parameters().asMap());
            currentValues.put(String.valueOf(i), s);
            statement = new Statement(statement.text(), currentValues);
        }
    }

    public ResultSetWrapper executeQuery() throws Exception {
        try {
            if (preparedStatement != null) {
                return new ResultSetWrapper(preparedStatement.executeQuery());
            } else {
                Transaction tx = session.beginTransaction();
                ResultSetWrapper rw = new ResultSetWrapper(tx.run(statement));
                tx.success();
                tx.close();
                return rw;
            }
        } finally {
            if (session != null && session.isOpen()) {
                session.close();
            }
        }
    }

    public void execute() throws Exception {
        try {
            if (preparedStatement != null) {
                preparedStatement.executeQuery();
            } else {
                Transaction tx = session.beginTransaction();
                tx.run(statement);
                tx.success();
                tx.close();
            }
        } finally {
            if (session != null && session.isOpen()) {
                session.close();
            }
        }
    }

    public void setInt(int i, int o) throws Exception {
        if (preparedStatement != null) {
            preparedStatement.setInt(i, o);
        } else {
            Map<String, Object> currentValues = new HashMap<>();
            currentValues.putAll(statement.parameters().asMap());
            currentValues.put(String.valueOf(i), o);
            statement = new Statement(statement.text(), currentValues);
        }
    }

    public void setObject(int i, Object o) throws Exception {
        if (preparedStatement != null) {
            preparedStatement.setObject(i, o);
        } else {
            Map<String, Object> currentValues = new HashMap<>();
            currentValues.putAll(statement.parameters().asMap());
            currentValues.put(String.valueOf(i), o);
            statement = new Statement(statement.text(), currentValues);
        }
    }

    @Override
    public void close() throws Exception {
        if (preparedStatement != null) {
            preparedStatement.close();
        } else {
            if (session != null && session.isOpen() )
                session.close();
        }
    }
}
