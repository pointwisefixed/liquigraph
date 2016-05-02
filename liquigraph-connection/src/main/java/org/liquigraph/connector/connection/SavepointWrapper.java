package org.liquigraph.connector.connection;

import java.sql.Savepoint;

/**
 * Created by wal-mart on 4/28/16.
 *
 * @author: wal-mart
 * @author: grosal3
 */
public class SavepointWrapper {

    private Savepoint sp;

    public SavepointWrapper() {
    }

    public SavepointWrapper(Savepoint sp) {
        this.sp = sp;
    }

    public Savepoint getSavePointWrapper() {

        return sp;
    }
}
