package org.liquigraph.connector.connection;

import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.util.Function;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by wal-mart on 5/4/16.
 *
 * @author: wal-mart
 * @author: grosal3
 */
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

    public Iterable<String> keys() {
        return graphDbNode != null ? graphDbNode.getPropertyKeys() : boltNode.keys();
    }

    public boolean containsKey(String key) {
        return graphDbNode != null ? graphDbNode.hasProperty(key) : boltNode.containsKey(key);
    }

    public Value get(String key) {
        return graphDbNode != null ? Values.value(graphDbNode.getProperty(key)) : boltNode.get(key);
    }

    public int size() {
        return graphDbNode != null ? graphDbNode.getAllProperties().size() : boltNode.size();
    }

    public Iterable<Value> values() {
        return graphDbNode != null ?
            Values.value(graphDbNode.getAllProperties()).asList(value -> value) :
            boltNode.values();
    }


    public <T> Iterable<T> values(Function<Value, T> mapFunction) {
        return graphDbNode != null ?
            Values.value(graphDbNode.getAllProperties()).asList(mapFunction) :
            boltNode.values(mapFunction);

    }


    public Map<String, Object> asMap() {
        return graphDbNode != null ? graphDbNode.getAllProperties() : boltNode.asMap();
    }


    public <T> Map<String, T> asMap(Function<Value, T> mapFunction) {

        return boltNode != null ? boltNode.asMap(mapFunction) : null;

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


    public Object getProperty(String key) {
        return graphDbNode != null ? graphDbNode.getProperty(key) : boltNode.get(key);
    }


    public Object getProperty(String key, Object defaultValue) {
        Object rslt = graphDbNode != null ? graphDbNode.getProperty(key, defaultValue) : boltNode.get(key);
        return rslt == null ? defaultValue : rslt;
    }



    public String getPropertyAsString(String checksum) {
        return graphDbNode != null ?
            graphDbNode.getProperty(checksum).toString() :
            boltNode.get(checksum).asString().replaceAll("\"", "");
    }
}
