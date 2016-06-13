package org.liquigraph.connector.connection;

import java.sql.Blob;

public class BlobWrapper {

    private Blob blob;

    public BlobWrapper() {
    }

    public BlobWrapper(Blob blob) {
        this.blob = blob;
    }
}
