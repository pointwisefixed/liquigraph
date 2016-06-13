package org.liquigraph.connector.connection;

import java.sql.Array;

public class ArrayWrapper {

    private Array array;

    public ArrayWrapper(Array arrayOf) {
        this.array = arrayOf;
    }

    public ArrayWrapper() {

    }
}
