package org.liquigraph.connector.connection;

import java.sql.SQLXML;

public class XmlWrapper {

    private SQLXML xml;

    public XmlWrapper(SQLXML xml) {
        this.xml = xml;
    }

    public SQLXML getXml() {
        return xml;
    }
}
