package com.liquigraph.maven;

import com.liquigraph.core.api.Liquigraph;
import com.liquigraph.core.configuration.Configuration;
import com.liquigraph.core.configuration.ConfigurationBuilder;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

import java.util.ArrayList;
import java.util.Collection;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

public abstract class LiquigraphMojoBase extends AbstractMojo {

    /**
     * Current maven project
     */
    @Parameter(property = "project", required = true, readonly = true)
    MavenProject project;

    /**
     * Classpath location of the master changelog file
     */
    @Parameter(property = "changelog", required = true)
    String changelog;

    /**
     * Graph JDBC URI
     *
     *  - jdbc:neo4j://<host>:<port>/
     *  - jdbc:neo4j:file:/path/to/db
     *  - jdbc:neo4j:mem
     *  - jdbc:neo4j:mem:name
     */
    @Parameter(property = "uri", required = true)
    String uri;

    /**
     * Graph connection username.
     */
    @Parameter(property = "username")
    String username;

    /**
     * Graph connection password.
     */
    @Parameter(property = "password")
    String password;

    /**
     * Comma-separated execution context list.
     * If no context is specified, all Liquigraph changeset contexts will match.
     * If contexts are defined, all Liquigraph changesets whose at least 1 declared context will match.
     * Please note that Liquigraph changesets that define no context will always match.
     */
    @Parameter(property = "executionContexts", defaultValue = "")
    String executionContexts = "";

    @Override
    public final void execute() throws MojoExecutionException, MojoFailureException {
        String directory = project.getBuild().getDirectory();

        try {
            Configuration configuration = withExecutionMode(new ConfigurationBuilder()
                .withClassLoader(ProjectClassLoader.getClassLoader(project))
                .withExecutionContexts(executionContexts(executionContexts))
                .withMasterChangelogLocation(changelog)
                .withUsername(username)
                .withPassword(password)
                .withUri(uri))
                .build();

            getLog().info("Generating Cypher output file in directory: " + directory);
            new Liquigraph().runMigrations(configuration);

        } catch (Exception e) {
            throw new MojoExecutionException(e.getMessage(), e);
        }
    }

    protected abstract ConfigurationBuilder withExecutionMode(ConfigurationBuilder configurationBuilder);

    private Collection<String> executionContexts(String executionContexts) {
        if (executionContexts.isEmpty()) {
            return emptyList();
        }
        Collection<String> result = new ArrayList<>();
        for (String context : asList(executionContexts.split(","))) {
            result.add(context.trim());
        }
        return result;
    }

}