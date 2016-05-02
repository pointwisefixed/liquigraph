package org.liquigraph.connector.connection;

import java.sql.Struct;

/**
 * Created by wal-mart on 4/28/16.
 *
 * @author: wal-mart
 * @author: grosal3
 */
public class StructWrapper {

    private Struct struct;

    public StructWrapper(Struct struct){
        this.struct = struct;
    }

    public StructWrapper(){}



    public Struct getStruct() {
        return struct;
    }
}
