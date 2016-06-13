package org.liquigraph.connector.transaction;

import org.liquigraph.connector.connection.TransactionWrapper;

public class IncludedTransaction implements TransactionWrapper {
    @Override
    public void commit() {
        //noop
    }
}
