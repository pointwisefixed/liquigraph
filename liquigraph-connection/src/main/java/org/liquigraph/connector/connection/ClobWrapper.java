package org.liquigraph.connector.connection;

import java.sql.Clob;

public class ClobWrapper {
    private Clob clob;

    public ClobWrapper() {
    }

    public ClobWrapper(Clob clob) {
        this.clob = clob;
    }
}
