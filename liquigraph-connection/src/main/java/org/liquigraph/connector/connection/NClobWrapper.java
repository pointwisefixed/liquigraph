package org.liquigraph.connector.connection;

import java.sql.NClob;

public class NClobWrapper {

    private NClob nClob;

    public NClobWrapper() {
    }

    public NClobWrapper(NClob nClob) {
        this.nClob = nClob;
    }
}
