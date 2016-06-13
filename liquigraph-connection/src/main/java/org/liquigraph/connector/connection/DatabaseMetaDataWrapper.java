package org.liquigraph.connector.connection;

import java.sql.DatabaseMetaData;

public class DatabaseMetaDataWrapper {
    private DatabaseMetaData metaData;

    public DatabaseMetaDataWrapper() {
    }

    public DatabaseMetaDataWrapper(DatabaseMetaData metaData) {
        this.metaData = metaData;
    }

}
