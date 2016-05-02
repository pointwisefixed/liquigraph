package org.liquigraph.connector.connection;

import java.sql.Array;

/**
 * Created by wal-mart on 4/28/16.
 *
 * @author: wal-mart
 * @author: grosal3
 */
public class ArrayWrapper {

    private Array array;

    public ArrayWrapper(Array arrayOf) {
        this.array = arrayOf;
    }

    public ArrayWrapper() {

    }


    public Array getArray() {
        return array;
    }
}
