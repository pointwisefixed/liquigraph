/**
 * Copyright 2014-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.liquigraph.connector.io.xml;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.Iterator;
import java.util.NoSuchElementException;

final class NodeListIterator implements Iterator<Node> {

    private final NodeList list;
    private int i;

    NodeListIterator(NodeList list) {
        this.list = list;
    }

    @Override
    public boolean hasNext() {
        return i < list.getLength();
    }

    @Override
    public Node next() {
        Node node = list.item(i++);
        if (node == null) {
            throw new NoSuchElementException();
        }
        return node;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
