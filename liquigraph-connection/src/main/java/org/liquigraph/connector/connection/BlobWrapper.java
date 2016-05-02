package org.liquigraph.connector.connection;

import java.sql.Blob;

/**
 * Created by wal-mart on 4/28/16.
 *
 * @author: wal-mart
 * @author: grosal3
 */
public class BlobWrapper {

    private  Blob blob;

    public BlobWrapper() {
    }

    public BlobWrapper(Blob blob) {
        this.blob = blob;
    }

    public Blob getBlob() {
        return blob;
    }
}
