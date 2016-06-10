/**
 * Copyright 2014-2016 the original author or authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.liquigraph.core;

import org.junit.rules.ExternalResource;
import org.liquigraph.connector.connection.ConnectionWrapper;
import org.liquigraph.connector.settings.TestServerFactory;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.UUID;

import static com.google.common.base.Throwables.propagate;

public class EmbeddedGraphDatabaseRule extends ExternalResource {

    private final String dbName;
    private final String uri;

    public boolean isBolt() {
        return bolt;
    }

    private final boolean bolt;
    private ConnectionWrapper connection;
    private GraphDatabaseService db;

    public EmbeddedGraphDatabaseRule(String name) {
        dbName = name + "-" + UUID.randomUUID().toString();
        this.bolt = !hasJdbcDriver();
        if (!bolt) {
            uri = "jdbc:neo4j:instance:" + dbName;
        } else {
            uri = "bolt://localhost:7887";
        }
    }

    private static boolean hasJdbcDriver() {
        try {
            boolean hasJdbcDriver = Class.forName("org.neo4j.jdbc.Driver") != null;
            boolean has2xKernelVersion = Class.forName("org.neo4j.kernel.Version") != null;
            return has2xKernelVersion && hasJdbcDriver;
        } catch (NoClassDefFoundError notFound) {
            return false;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    public ConnectionWrapper connection() {
        return connection;
    }

    public String uri() {
        return uri;
    }

    protected void before() {
        try {
            db = TestServerFactory.getDb(uri, bolt);
            connection = TestServerFactory.getTestServer(bolt, db, uri, dbName);
        } catch (Exception e) {
            throw propagate(e);
        }
    }

    protected void after() {
        try {
            if (!connection.isClosed()) {
                connection.close();
            }
            db.shutdown();
        } catch (Exception e) {
            throw propagate(e);
        }
    }

}
