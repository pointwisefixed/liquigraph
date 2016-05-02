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
package org.liquigraph.cli;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableList;
import org.liquigraph.core.api.Liquigraph;
import org.liquigraph.connector.configuration.ConfigurationBuilder;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;

import static com.google.common.base.Throwables.propagate;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

public class LiquigraphCli {

    @Parameter(
            names = {"--help", "-h"},
            description = "Get this help",
            help = true
    )
    private boolean help;

    @Parameter(
            names = {"--changelog", "-c"},
            description = "Master Liquigraph changelog location.\n" +
                    "\t Prefix with 'classpath:' if location is in classpath",
            required = true
    )
    private String changelog;

    @Parameter(
            names = {"--graph-db-uri", "-g"},
            description = "Graph JDBC URI:\n" +
                    "\t- jdbc:neo4j://host:port/\n" +
                    "\t- jdbc:neo4j:file:/path/to/db\n" +
                    "\t- jdbc:neo4j:mem\n" +
                    "\t- jdbc:neo4j:mem:name\n",
            required = true
    )
    private String graphDbUri;

    @Parameter(
            names = {"--username", "-u"},
            description = "Graph DB username (remote only)"
    )
    private String username;

    @Parameter(
            names = {"--password", "-p"},
            description = "Graph DB password (remote only)",
            password = true
    )
    private String password;

    @Parameter(
            names = {"--execution-contexts", "-x"},
            description = "Comma-separated list of Liquigraph execution contexts"
    )
    private String executionContexts = "";

    @Parameter(
            names = {"--dry-run-output-directory", "-d"},
            description = "Output directory path into which changeset queries will be written. " +
                    "Not setting this option will trigger RUN mode."
    )
    private String dryRunOutputDirectory;



    public static void main(String[] args) {
        LiquigraphCli cli = new LiquigraphCli();
        JCommander commander = new JCommander(cli, args);
        commander.setProgramName("liquigraph");

        if (cli.help) {
            commander.usage();
            return;
        }

        ConfigurationBuilder builder = new ConfigurationBuilder()
                .withMasterChangelogLocation(fileName(cli.changelog))
                .withUri(cli.graphDbUri)
                .withUsername(cli.username)
                .withPassword(cli.password)
                .withExecutionContexts(executionContexts(cli.executionContexts))
                .withClassLoader(classloader(parentFolder(cli.changelog), cli.dryRunOutputDirectory));

        String outputDirectory = cli.dryRunOutputDirectory;
        if (outputDirectory != null) {
            builder.withDryRunMode(Paths.get(outputDirectory));
        }
        else {
            builder.withRunMode();
        }

        new Liquigraph(false).runMigrations(
                builder.build()
        );
    }

    private static String fileName(String changelog) {
        return new File(changelog).getName();
    }

    private static String parentFolder(String changelog) {
        try {
            return new File(changelog).getCanonicalFile().getParent();
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private static Collection<String> executionContexts(String executionContexts) {
        if (executionContexts.isEmpty()) {
            return emptyList();
        }
        Collection<String> result = new ArrayList<>();
        for (String context : asList(executionContexts.split(","))) {
            result.add(context.trim());
        }
        return result;
    }

    private static ClassLoader classloader(String changelog, String dryRunOutputDirectory) {
        ImmutableList.Builder<URL> resources = ImmutableList.builder();
        try {
            resources.add(toUrl(changelog));
            if (dryRunOutputDirectory != null) {
                resources.add(toUrl(dryRunOutputDirectory));
            }
        } catch (MalformedURLException e) {
            throw propagate(e);
        }

        Collection<URL> urls = resources.build();
        return new URLClassLoader(urls.toArray(new URL[urls.size()]));
    }

    private static URL toUrl(String location) throws MalformedURLException {
        return new File(location).toURI().toURL();
    }
}
