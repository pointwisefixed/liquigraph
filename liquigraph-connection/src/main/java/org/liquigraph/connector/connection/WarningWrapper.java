package org.liquigraph.connector.connection;

import java.sql.SQLWarning;

/**
 * Created by wal-mart on 4/28/16.
 *
 * @author: wal-mart
 * @author: grosal3
 */
public class WarningWrapper {
    private SQLWarning warning;

    public WarningWrapper(){}

    public WarningWrapper(SQLWarning warnings) {
        this.warning = warnings;
    }

    public SQLWarning getWarning() {
        return warning;
    }
}
