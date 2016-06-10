/**
 * Copyright 2014-2016 the original author or authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.liquigraph.model.validators;


import org.neo4j.jdbc.Driver;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.LinkedList;

import static java.lang.String.format;

public class MandatoryOptionValidator {

    public Collection<String> validate(ClassLoader classLoader, String masterChangelog, String uri) {
        Collection<String> errors = new LinkedList<>();
        errors.addAll(validateMasterChangelog(masterChangelog, classLoader));
        errors.addAll(validateGraphInstance(uri));
        return errors;
    }

    private static Collection<String> validateMasterChangelog(String masterChangelog, ClassLoader classLoader) {
        Collection<String> errors = new LinkedList<>();
        if (masterChangelog == null) {
            errors.add("'masterChangelog' should not be null");
        } else {
            try (InputStream stream = classLoader.getResourceAsStream(masterChangelog)) {
                if (stream == null) {
                    errors.add(format("'masterChangelog' points to a non-existing location: %s", masterChangelog));
                }
            } catch (IOException e) {
                errors.add(format("'masterChangelog' read error. Cause: %s", e.getMessage()));
            }
        }
        return errors;
    }

    private static Collection<String> validateGraphInstance(String uri) {
        Collection<String> errors = new LinkedList<>();
        if (uri == null) {
            errors.add("'uri' should not be null");
        } else if (!uri.startsWith(Driver.CON_PREFIX) && !uri.startsWith("bolt://")) {
            errors.add(format("Invalid JDBC URI. Supported configurations:%n" +
                "\t - jdbc:neo4j://<host>:<port>/%n" +
                "\t - jdbc:neo4j:file:/path/to/db%n" +
                "\t - jdbc:neo4j:mem or jdbc:neo4j:mem:name.%n"
                + "\t - or for Bolt Connections:%n"
                + "\t - bolt://<host>:<port>%n" +
                "Given: %s", uri));
        }
        return errors;
    }

}
