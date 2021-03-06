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
package org.liquigraph.core.configuration;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.liquigraph.connector.configuration.ConfigurationBuilder;

import java.nio.file.Path;

import static java.lang.String.format;

public class ConfigurationBuilderTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public TemporaryFolder outputCypherFolder = new TemporaryFolder();

    @Test
    public void fails_on_null_changelog_location() {
        thrown.expect(RuntimeException.class);
        thrown.expectMessage("'masterChangelog' should not be null");

        new ConfigurationBuilder()
            .withUri("http://localhost:7474/db/data")
            .build();
    }

    @Test
    public void fails_on_non_existing_changelog() {
        thrown.expect(RuntimeException.class);
        thrown.expectMessage("'masterChangelog' points to a non-existing location: changelog/non-existing.xml");

        new ConfigurationBuilder()
            .withUri("http://localhost:7474/db/data")
            .withMasterChangelogLocation("changelog/non-existing.xml")
            .build();
    }

    @Test
    public void fails_on_null_uri() {
        thrown.expect(RuntimeException.class);
        thrown.expectMessage("'uri' should not be null");

        new ConfigurationBuilder()
            .withMasterChangelogLocation("changelog/changelog.xml")
            .build();
    }

    @Test
    public void fails_on_unsupported_protocol() {
        thrown.expect(RuntimeException.class);
        thrown.expectMessage(
            "\t - Invalid JDBC URI. Supported configurations:\n" +
            "\t - jdbc:neo4j://<host>:<port>/\n" +
            "\t - jdbc:neo4j:file:/path/to/db\n" +
            "\t - jdbc:neo4j:mem or jdbc:neo4j:mem:name.\n"
                + "\t - or for Bolt Connections:\n"
                + "\t - bolt://<host>:<port>\n" +
            "Given: ssh://sorry@buddy"
        );

        new ConfigurationBuilder()
            .withMasterChangelogLocation("changelog/changelog.xml")
            .withUri("ssh://sorry@buddy")
            .build();
    }

    @Test
    public void add_ups_all_misconfiguration_errors() {
        thrown.expect(RuntimeException.class);
        thrown.expectMessage(" - 'masterChangelog' should not be null");
        thrown.expectMessage(" - 'uri' should not be null");

        new ConfigurationBuilder().build();
    }

    @Test
    public void path_should_point_to_a_directory() throws Exception {
        outputCypherFolder.create();
        Path path = outputCypherFolder.newFile("output.cypher").toPath();

        thrown.expect(RuntimeException.class);
        thrown.expectMessage(format("<%s> is not a directory", path.toString()));

        new ConfigurationBuilder()
                    .withMasterChangelogLocation("changelog/changelog.xml")
                    .withUri("file:///sorry@buddy")
                    .withDryRunMode(path)
                    .build();
    }

    @Test
    public void output_folder_must_be_writable() throws Exception {
        outputCypherFolder.create();
        Path path = outputCypherFolder.getRoot().toPath();
        path.toAbsolutePath().toFile().setWritable(false);

        thrown.expect(RuntimeException.class);
        thrown.expectMessage(format("<%s> must be writable", path.toString()));

        new ConfigurationBuilder()
                    .withMasterChangelogLocation("changelog/changelog.xml")
                    .withUri("http:///sorry@buddy")
                    .withDryRunMode(path)
                    .build();
    }
}
