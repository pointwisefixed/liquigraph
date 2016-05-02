package org.liquigraph.connector;

import org.liquigraph.connector.configuration.Configuration;
import org.liquigraph.connector.connection.ConnectionWrapper;
import org.liquigraph.connector.driver.DriverManagerWrapper;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;

/**
 * Created by wal-mart on 4/28/16.
 *
 * @author: wal-mart
 * @author: grosal3
 */
public class BoltConnector implements LiquigraphConnector {
    @Override
    public ConnectionWrapper getConnection(Configuration configuration) {

        //TODO check if there is not username or password
        Driver driver = GraphDatabase.driver(configuration.uri(),
            AuthTokens.basic(configuration.username().get(), configuration.password().get()));
        return DriverManagerWrapper.getBoltConnection(driver);
    }
}
