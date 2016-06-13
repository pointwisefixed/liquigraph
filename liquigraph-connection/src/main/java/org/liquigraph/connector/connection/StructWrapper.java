package org.liquigraph.connector.connection;

import java.sql.Struct;

public class StructWrapper {

    private Struct struct;

    public StructWrapper(Struct struct) {
        this.struct = struct;
    }
}
