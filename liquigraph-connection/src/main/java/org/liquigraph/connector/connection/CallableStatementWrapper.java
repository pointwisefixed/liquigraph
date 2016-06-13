package org.liquigraph.connector.connection;

import java.sql.CallableStatement;

public class CallableStatementWrapper {
    private CallableStatement callableStatement;

    public CallableStatementWrapper() {
    }

    public CallableStatementWrapper(CallableStatement callableStatement) {
        this.callableStatement = callableStatement;
    }
}
