package org.liquigraph.connector.connection;

import java.sql.SQLXML;

/**
 * Created by wal-mart on 4/28/16.
 *
 * @author: wal-mart
 * @author: grosal3
 */
public class XmlWrapper {

    private SQLXML xml;

    public XmlWrapper(){}

    public XmlWrapper(SQLXML xml) {
        this.xml = xml;
    }

    public SQLXML getXml() {
        return xml;
    }
}
