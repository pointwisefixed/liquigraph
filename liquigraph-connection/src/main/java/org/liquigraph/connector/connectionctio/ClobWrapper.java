package org.liquigraph.connector.connectionctio;

import java.sql.Clob;

/**
 * Created by wal-mart on 4/28/16.
 *
 * @author: wal-mart
 * @author: grosal3
 */
public class ClobWrapper {
    private Clob clob;

    public ClobWrapper(){}

    public ClobWrapper(Clob clob) {
        this.clob = clob;
    }

    public Clob getClob() {
        return clob;
    }
}
