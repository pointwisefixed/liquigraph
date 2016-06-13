package org.liquigraph.connector.connection;

import java.sql.SQLWarning;

public class WarningWrapper {
    private SQLWarning warning;

    public WarningWrapper() {
    }

    public WarningWrapper(SQLWarning warnings) {
        this.warning = warnings;
    }
}
