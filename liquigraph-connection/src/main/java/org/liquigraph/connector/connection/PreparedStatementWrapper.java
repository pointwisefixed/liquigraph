package org.liquigraph.connector.connection;

import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.Values;

import java.sql.PreparedStatement;

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
            statement = statement.withUpdatedParameters(Values.value(s));
        }
    }

    public ResultSetWrapper executeQuery() throws Exception {
        if (preparedStatement != null) {
            return new ResultSetWrapper(preparedStatement.executeQuery());
        } else {
            return new ResultSetWrapper(session.run(statement));
        }
    }

    public void execute() throws Exception {
        if (preparedStatement != null) {
            preparedStatement.executeQuery();
        } else {
            session.run(statement);
        }
    }

    public void setInt(int i, int i1) throws Exception {
        if (preparedStatement != null) {
            preparedStatement.setInt(i, i1);
        } else {
            statement = statement.withUpdatedParameters(Values.value(i1));
        }
    }

    public void setObject(int i, Object o) throws Exception {
        if (preparedStatement != null) {
            preparedStatement.setObject(i, o);
        } else {
            statement = statement.withUpdatedParameters(Values.value(o));
        }
    }

    @Override
    public void close() throws Exception {
        if (preparedStatement != null) {
            preparedStatement.close();
        }
    }
}
