package org.liquigraph.connector;

import org.liquigraph.connector.configuration.Configuration;
import org.liquigraph.connector.connection.ConnectionWrapper;
import org.liquigraph.connector.driver.DriverManagerWrapper;
import org.liquigraph.connector.io.lock.LockableConnection;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;

public class BoltConnector implements LiquigraphConnector {
    @Override
    public ConnectionWrapper getConnection(Configuration configuration) {

        Driver driver = decide(configuration);
        return LockableConnection.acquire(DriverManagerWrapper.getBoltConnection(driver));
    }

    private Driver decide(Configuration configuration) {
        if (!configuration.username().isPresent() && !configuration.password().isPresent()) {
            return GraphDatabase.driver(configuration.uri());
        }
        return GraphDatabase.driver(configuration.uri(),
            AuthTokens.basic(configuration.username().get(), configuration.password().get()));
    }

}
