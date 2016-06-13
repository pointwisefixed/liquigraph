package org.liquigraph.connector.driver;

import org.liquigraph.connector.connection.ConnectionWrapper;
import org.liquigraph.connector.connection.bolt.BoltConnectionWrapper;
import org.liquigraph.connector.connection.sql.JdbcConnectionWrapper;
import org.neo4j.driver.v1.Driver;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DriverManagerWrapper {
    public static ConnectionWrapper getJdbcConnection(String uri) throws SQLException {
        return new JdbcConnectionWrapper(DriverManager.getConnection(uri));
    }

    public static ConnectionWrapper getJdbcConnection(String uri, Properties props) throws SQLException {
        return new JdbcConnectionWrapper(DriverManager.getConnection(uri, props));
    }

    public static ConnectionWrapper getBoltConnection(Driver driver) {
        return new BoltConnectionWrapper(driver);
    }

}
