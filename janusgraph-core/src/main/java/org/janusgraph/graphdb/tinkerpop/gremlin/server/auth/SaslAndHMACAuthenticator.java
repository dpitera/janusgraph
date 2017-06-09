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

import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.Map;

/**
 * A wrapper authenticator that instantiates A JansuGraphSimpleAuthenticator (for Sasl)
 * and an HMACAuthenticator (for http)
 * @author Keith Lohnes lohnesk@gmail.com
 */
public class SaslAndHMACAuthenticator extends JanusGraphAbstractAuthenticator {
    private static final String ILLEGAL_STATE_MESSAGE =
        "This exception is likely due to a misconfiguration. Try using the HMACAndSaslAuthenticationHandler as the " +
        "authenticationHandler in the server authentication configuration";

    private Map<String, String> authenticatorConfig;
    private JanusGraphSimpleAuthenticator simpleAuthenticator = new JanusGraphSimpleAuthenticator();
    private HMACAuthenticator hmacAuthenticator = new HMACAuthenticator();

    @Override
    public SaslNegotiator newSaslNegotiator(final InetAddress remoteAddress) {
        throw new RuntimeException("JanusGraphWsAndHttpAuthenticator does not use SASL!");
    }

    public SaslNegotiator newSaslNegotiator() {
        throw new RuntimeException("JanusGraphWsAndHttpAuthenticator does not use SASL!");
    }

    @Override
    public void setup(final Map<String,Object> config) {
        super.setup(config);
        simpleAuthenticator.credentialStore = this.credentialStore;
        hmacAuthenticator.credentialStore = this.credentialStore;
    }

    @Override
    public AuthenticatedUser authenticate(final Map<String, String> credentials) throws AuthenticationException {
        throw new IllegalStateException(ILLEGAL_STATE_MESSAGE);
    }

    public JanusGraphSimpleAuthenticator getSimpleAuthenticator() {
        return this.simpleAuthenticator;
    }

    public HMACAuthenticator getHMACAuthenticator() {
        return this.hmacAuthenticator;
    }


}
