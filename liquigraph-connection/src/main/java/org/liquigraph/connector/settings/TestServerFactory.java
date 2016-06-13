package org.liquigraph.connector.settings;

import org.liquigraph.connector.connection.ConnectionWrapper;
import org.liquigraph.connector.driver.DriverManagerWrapper;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.harness.ServerControls;
import org.neo4j.harness.TestServerBuilders;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.configuration.Settings;

import java.sql.SQLException;
import java.util.Properties;

public class TestServerFactory {

    public static ConnectionWrapper getTestServer(boolean bolt, GraphDatabaseService db, String uri, String dbName)
        throws Exception {
        ConnectionWrapper connectionWrapper = null;
        if (!bolt) {
            try {
                Class.forName("org.neo4j.jdbc.Driver");
                Properties props = properties(db, dbName);
                connectionWrapper = DriverManagerWrapper.getJdbcConnection(uri, props);
            } catch (NoClassDefFoundError | ClassNotFoundException ex) {
                throw ex;
            } catch (SQLException e) {
                return null;
            }
        } else {
            //dbms.security.auth_enabled
            connectionWrapper = getBolt(uri);
        }
        connectionWrapper.setAutoCommit(false);
        return connectionWrapper;
    }

    private static ConnectionWrapper getBolt(String uri) {
        return DriverManagerWrapper.getBoltConnection(
            GraphDatabase.driver(uri, Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig()));
    }

    private static Properties properties(GraphDatabaseService db, String dbName) {
        Properties props = new Properties();
        props.put(dbName, db);
        return props;
    }

    public static GraphDatabaseService getDb(String uri, boolean bolt) {
        GraphDatabaseService db = null;
        if (!bolt) {
            db = TestServerBuilders.newInProcessBuilder().newServer().graph();
        } else {
            ServerControls controls = TestServerBuilders.newInProcessBuilder()
                .withConfig(getBoltConnectorAddressSetting(), uri.replace("bolt://", ""))
                .withConfig(getEnabledSetting(), "true").withConfig(getBoltConnectorTypeSetting(), "BOLT")
                .withConfig(getAuthEnabled(), "false").newServer();
            db = controls.graph();
        }
        return db;
    }

    private static Setting<HostnamePort> getBoltConnectorAddressSetting() {
        return Settings.setting("dbms.connector.0.address", Settings.HOSTNAME_PORT, "localhost:7887");
    }

    private static Setting<Boolean> getEnabledSetting() {
        return Settings.setting("dbms.connector.0.enabled", Settings.BOOLEAN, "false");
    }


    private static Setting<Boolean> getAuthEnabled() {
        return Settings.setting("dbms.security.auth_enabled", Settings.BOOLEAN, "false");
    }

    public static Setting getBoltConnectorTypeSetting() {

        try {
            Setting seting = Settings.setting("type", Settings
                    .options(Class.forName("org.neo4j.graphdb.factory.GraphDatabaseSettings$Connector$ConnectorType")),
                "BOLT");
            return seting;
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
