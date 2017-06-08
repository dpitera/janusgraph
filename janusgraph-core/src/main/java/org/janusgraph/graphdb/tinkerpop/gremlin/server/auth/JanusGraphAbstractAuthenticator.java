/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * Modifications Copyright 2017 JanusGraph Authors
 */

package org.janusgraph.graphdb.tinkerpop.gremlin.server.auth;

import org.apache.tinkerpop.gremlin.server.auth.Authenticator;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.groovy.plugin.dsl.credential.CredentialGraph;

import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.core.schema.SchemaAction;
import org.janusgraph.graphdb.database.management.ManagementSystem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.groovy.plugin.dsl.credential.CredentialGraphTokens.PROPERTY_PASSWORD;
import static org.apache.tinkerpop.gremlin.groovy.plugin.dsl.credential.CredentialGraphTokens.PROPERTY_USERNAME;

abstract public class JanusGraphAbstractAuthenticator implements Authenticator {
    private static final Logger logger = LoggerFactory.getLogger(JanusGraphAbstractAuthenticator.class);

    /**
     * The location of the configuration file that contains the credentials database.
     */
    public static final String CONFIG_CREDENTIAL_DB = "credentialDb";

    /**
     * Default username
     */
    public static final String CONFIG_DEFAULT_USER = "defaultUsername";

    /**
     * Default password for default username.
     */
    public static final String CONFIG_DEFAULT_PASSWORD = "defaultPassword";

    protected CredentialGraph credentialStore;

    @Override
    public boolean requireAuthentication() {
        return true;
    }

    @Override
    abstract public SaslNegotiator newSaslNegotiator(final InetAddress remoteAddress);

    @Override
    abstract public SaslNegotiator newSaslNegotiator();

    public CredentialGraph createCredentialGraph(JanusGraph graph) {
        return new CredentialGraph(graph);
    }

    public JanusGraph openGraph(String conf) {
        return JanusGraphFactory.open(conf);
    }

    public void setup(final Map<String,Object> config) {
        logger.info("Initializing authentication with the {}", this.getClass().getName());
        if (null == config) {
            throw new IllegalArgumentException(String.format(
                    "Could not configure a %s - provide a 'config' in the 'authentication' settings",
                    this.getClass().getName()));
        }

        if (!config.containsKey(CONFIG_CREDENTIAL_DB)) {
            throw new IllegalStateException(String.format(
                    "Credential configuration missing the %s key that points to a graph config file or graph name", CONFIG_CREDENTIAL_DB));
        }

        final JanusGraph graph = openGraph(config.get(CONFIG_CREDENTIAL_DB).toString());
        credentialStore = createCredentialGraph(graph);
        graph.tx().rollback();
        ManagementSystem mgmt = (ManagementSystem) graph.openManagement();
        if (!mgmt.containsGraphIndex("byUsername")) {
            final PropertyKey username = mgmt.makePropertyKey(PROPERTY_USERNAME).dataType(String.class).cardinality(Cardinality.SINGLE).make();
            JanusGraphIndex index = mgmt.buildIndex("byUsername", Vertex.class).addKey(username).unique().buildCompositeIndex();
            mgmt.commit();

            mgmt = (ManagementSystem) graph.openManagement();
            index = mgmt.getGraphIndex("byUsername");
            if (!index.getIndexStatus(username).equals(SchemaStatus.ENABLED)) {
                try {
                    mgmt = (ManagementSystem) graph.openManagement();
                    mgmt.updateIndex(mgmt.getGraphIndex("byUsername"), SchemaAction.REINDEX);
                    mgmt.awaitGraphIndexStatus(graph, "byUsername").status(SchemaStatus.ENABLED).call();
                } catch (InterruptedException rude) {
                    throw new RuntimeException("Timed out waiting for byUsername index to be created on credential graph", rude);
                }
            }
        }

        if (credentialStore.countUsers() == 0) {
            if (!config.containsKey(CONFIG_DEFAULT_USER) || !config.containsKey(CONFIG_DEFAULT_PASSWORD)) {
                throw new IllegalStateException(String.format("If there are no users in your credential graph both %s and %s must be defined", CONFIG_DEFAULT_USER, CONFIG_DEFAULT_PASSWORD));
            }
            credentialStore.createUser(config.get(CONFIG_DEFAULT_USER).toString(), config.get(CONFIG_DEFAULT_PASSWORD).toString());
        }

    }

}
