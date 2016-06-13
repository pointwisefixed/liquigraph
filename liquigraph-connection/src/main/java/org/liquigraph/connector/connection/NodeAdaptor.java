package org.liquigraph.connector.connection;

import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class NodeAdaptor {

    private Node graphDbNode;
    private Value boltNode;
    private Map mapNode;

    public NodeAdaptor(Node graphNode) {
        this.graphDbNode = graphNode;
    }

    public NodeAdaptor(Value nd) {
        this.boltNode = nd;
    }


    public long id() {
        return graphDbNode != null ? graphDbNode.getId() : boltNode.asNode().id();
    }

    public Value get(String key) {
        return graphDbNode != null ? Values.value(graphDbNode.getProperty(key)) : boltNode.get(key);
    }

    public long getId() {
        return graphDbNode != null ? graphDbNode.getId() : boltNode.asNode().id();
    }

    public Iterable<Label> getLabels() {
        if (graphDbNode != null)
            return graphDbNode.getLabels();
        List<Label> lbls = new ArrayList<>();
        Iterable<String> labels = boltNode.asNode().labels();
        for (String lb : labels) {
            lbls.add(DynamicLabel.label(lb));
        }
        return lbls;
    }

    public String getPropertyAsString(String checksum) {
        return graphDbNode != null ?
            graphDbNode.getProperty(checksum).toString() :
            boltNode.get(checksum).asString().replaceAll("\"", "");
    }
}
