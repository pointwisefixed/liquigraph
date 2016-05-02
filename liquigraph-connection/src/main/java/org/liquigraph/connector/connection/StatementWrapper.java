package org.liquigraph.connector.connection;

import org.neo4j.driver.v1.Session;

import java.sql.Statement;

/**
 * Created by wal-mart on 4/28/16.
 *
 * @author: wal-mart
 * @author: grosal3
 */
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
        if (statement != null) {
            statement.execute(query);
        } else if (session != null) {
            session.run(query);
        }
    }

    public ResultSetWrapper executeQuery(String query) throws Exception {
        if(statement != null){
            return new ResultSetWrapper(statement.executeQuery(query));
        }
        else{
            return new ResultSetWrapper(session.run(query));
        }
    }

    @Override
    public void close() throws Exception {
        if (statement != null)
            statement.close();
    }
}
