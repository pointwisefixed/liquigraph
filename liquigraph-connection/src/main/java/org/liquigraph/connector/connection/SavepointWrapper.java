package org.liquigraph.connector.connection;

import java.sql.Savepoint;

public class SavepointWrapper {

    private Savepoint sp;

    public SavepointWrapper(Savepoint sp) {
        this.sp = sp;
    }

    public Savepoint getSavePointWrapper() {

        return sp;
    }
}
