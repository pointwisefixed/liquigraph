package org.liquigraph.connector.transaction;

import org.liquigraph.connector.connection.TransactionWrapper;

/**
 * Created by wal-mart on 4/28/16.
 *
 * @author: wal-mart
 * @author: grosal3
 */
public class IncludedTransaction implements TransactionWrapper {
    @Override
    public void commit() {
        //noop
    }
}
