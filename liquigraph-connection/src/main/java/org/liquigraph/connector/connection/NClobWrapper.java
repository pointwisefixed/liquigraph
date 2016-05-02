package org.liquigraph.connector.connection;

import java.sql.NClob;

/**
 * Created by wal-mart on 4/28/16.
 *
 * @author: wal-mart
 * @author: grosal3
 */
public class NClobWrapper {

    private  NClob nClob;

    public NClobWrapper(){}

    public NClobWrapper(NClob nClob){
        this.nClob = nClob;
    }

    public NClob getnClob() {
        return nClob;
    }
}
