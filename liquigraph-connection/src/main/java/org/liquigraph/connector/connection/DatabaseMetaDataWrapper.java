package org.liquigraph.connector.connection;

import java.sql.DatabaseMetaData;

/**
 * Created by wal-mart on 4/28/16.
 *
 * @author: wal-mart
 * @author: grosal3
 */
public class DatabaseMetaDataWrapper {
    private DatabaseMetaData metaData;

    public DatabaseMetaDataWrapper() {
    }

    public DatabaseMetaDataWrapper(DatabaseMetaData metaData) {
        this.metaData = metaData;
    }

    public DatabaseMetaData getMetaData() {
        return metaData;
    }
}
