package org.liquigraph.connector.connection;

import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;

import java.sql.Statement;

public class StatementWrapper implements AutoCloseable {

    private Statement statement;
    private Session session;

    public StatementWrapper(Statement st) {
        this.statement = st;
    }

    public StatementWrapper(Session session) {
        this.session = session;
    }

    public void execute(String query) throws Exception {
        try {
            if (statement != null) {
                statement.execute(query);
            } else if (session != null) {
                Transaction tx = session.beginTransaction();
                tx.run(query);
                tx.success();;
                tx.close();
            }
        } finally {
            if (session != null && session.isOpen()) {
                session.close();
            }
        }
    }

    public ResultSetWrapper executeQuery(String query) throws Exception {
        try {
            if (statement != null) {
                return new ResultSetWrapper(statement.executeQuery(query));
            } else {
                ResultSetWrapper rw = null;
                Transaction tx = session.beginTransaction();
                rw = new ResultSetWrapper(tx.run(query));
                tx.success();
                tx.close();
                return rw;
            }
        } finally {
            if (session != null && session.isOpen())
                session.close();
        }

    }

    @Override
    public void close() throws Exception {
        if (statement != null)
            statement.close();
        else if (session != null && session.isOpen()) {
            session.close();
        }
    }
}
