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

import org.apache.tinkerpop.gremlin.groovy.plugin.dsl.credential.CredentialGraph;
import org.apache.tinkerpop.gremlin.server.auth.Authenticator;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticationException;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.mindrot.BCrypt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.core.schema.SchemaAction;
import org.janusgraph.graphdb.database.management.ManagementSystem;

import org.mindrot.BCrypt;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.groovy.plugin.dsl.credential.CredentialGraphTokens.PROPERTY_PASSWORD;
import static org.apache.tinkerpop.gremlin.groovy.plugin.dsl.credential.CredentialGraphTokens.PROPERTY_USERNAME;

/**
 * A simple implementation of an {@link Authenticator} that uses a {@link Graph} instance as a credential store.
 * Management of the credential store can be handled through the {@link CredentialGraph} DSL.
 * A modification of the SimpleAuthenticator from TinkerPop to add some configuration and an index
 * on UserName
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class JanusGraphSimpleAuthenticator implements Authenticator {
    private static final Logger logger = LoggerFactory.getLogger(JanusGraphSimpleAuthenticator.class);
    private static final byte NUL = 0;
    private CredentialGraph credentialStore;

    /**
     * Default username
     */
    public static final String CONFIG_DEFAULT_USER = "defaultUsername";

    /**
     * Default password for default username.
     */
    public static final String CONFIG_DEFAULT_PASSWORD = "defaultPassword";

    /**
     * The location of the configuration file that contains the credentials database.
     */
    public static final String CONFIG_CREDENTIAL_DB = "credentialDb";


    private static final String AUTH_ERROR = "Username and/or password are incorrect";

    @Override
    public boolean requireAuthentication() {
        return true;
    }

    public JanusGraph openGraph(String conf) {
        return JanusGraphFactory.open(conf);
    }

    public CredentialGraph createCredentialGraph(JanusGraph graph) {
        return new CredentialGraph(graph);
    }

    //Setup is the only modification from the TinkerPop SimpleAuthenticator.
    @Override
    public void setup(final Map<String,Object> config) {
        logger.info("Initializing authentication with the {}", JanusGraphSimpleAuthenticator.class.getName());
        if (null == config) {
            throw new IllegalArgumentException(String.format(
                    "Could not configure a %s - provide a 'config' in the 'authentication' settings",
                    JanusGraphSimpleAuthenticator.class.getName()));
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

    @Override
    public SaslNegotiator newSaslNegotiator(final InetAddress remoteAddress) {
        return new PlainTextSaslAuthenticator();
    }

    @Override
    public SaslNegotiator newSaslNegotiator() {
        return new PlainTextSaslAuthenticator();
    }

    public AuthenticatedUser authenticate(final Map<String, String> credentials) throws AuthenticationException {
        final Vertex user;
        if (!credentials.containsKey(PROPERTY_USERNAME)) throw new IllegalArgumentException(String.format("Credentials must contain a %s", PROPERTY_USERNAME));
        if (!credentials.containsKey(PROPERTY_PASSWORD)) throw new IllegalArgumentException(String.format("Credentials must contain a %s", PROPERTY_PASSWORD));

        final String username = credentials.get(PROPERTY_USERNAME);
        final String password = credentials.get(PROPERTY_PASSWORD);
        try {
            user = credentialStore.findUser(username);
        } catch (IllegalStateException ex) {
            // typically thrown when there are multiple users with the same name in the credential store
            logger.warn(ex.getMessage());
            throw new AuthenticationException(AUTH_ERROR, ex);
        } catch (Exception ex) {
            throw new AuthenticationException(AUTH_ERROR, ex);
        }

        if (null == user)  throw new AuthenticationException(AUTH_ERROR);

        final String hash = user.value(PROPERTY_PASSWORD);
        if (!BCrypt.checkpw(password, hash))
            throw new AuthenticationException(AUTH_ERROR);

        return new AuthenticatedUser(username);
    }

    private class PlainTextSaslAuthenticator implements Authenticator.SaslNegotiator {
        private boolean complete = false;
        private String username;
        private String password;

        @Override
        public byte[] evaluateResponse(final byte[] clientResponse) throws AuthenticationException {
            decodeCredentials(clientResponse);
            complete = true;
            return null;
        }

        @Override
        public boolean isComplete() {
            return complete;
        }

        @Override
        public AuthenticatedUser getAuthenticatedUser() throws AuthenticationException {
            if (!complete) throw new AuthenticationException("SASL negotiation not complete");
            final Map<String,String> credentials = new HashMap<>();
            credentials.put(PROPERTY_USERNAME, username);
            credentials.put(PROPERTY_PASSWORD, password);
            return authenticate(credentials);
        }

        /**
         * SASL PLAIN mechanism specifies that credentials are encoded in a
         * sequence of UTF-8 bytes, delimited by 0 (US-ASCII NUL).
         * The form is : {code}authzId<NUL>authnId<NUL>password<NUL>{code}.
         *
         * @param bytes encoded credentials string sent by the client
         */
        private void decodeCredentials(byte[] bytes) throws AuthenticationException {
            byte[] user = null;
            byte[] pass = null;
            int end = bytes.length;
            for (int i = bytes.length - 1 ; i >= 0; i--) {
                if (bytes[i] == NUL) {
                    if (pass == null)
                        pass = Arrays.copyOfRange(bytes, i + 1, end);
                    else if (user == null)
                        user = Arrays.copyOfRange(bytes, i + 1, end);
                    end = i;
                }
            }

            if (null == user) throw new AuthenticationException("Authentication ID must not be null");
            if (null == pass) throw new AuthenticationException("Password must not be null");

            username = new String(user, StandardCharsets.UTF_8);
            password = new String(pass, StandardCharsets.UTF_8);
        }
    }
}
