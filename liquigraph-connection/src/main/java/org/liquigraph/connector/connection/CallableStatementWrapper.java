package org.liquigraph.connector.connection;

import java.sql.CallableStatement;

/**
 * Created by wal-mart on 4/28/16.
 *
 * @author: wal-mart
 * @author: grosal3
 */
public class CallableStatementWrapper {
    private CallableStatement callableStatement;

    public CallableStatementWrapper(){}

    public CallableStatementWrapper(CallableStatement callableStatement) {
        this.callableStatement = callableStatement;
    }

    public CallableStatement getCallableStatement() {
        return callableStatement;
    }
}
