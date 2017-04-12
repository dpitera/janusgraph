// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package org.janusgraph.graphdb.tinkerpop.gremlin.server.auth;

import org.apache.tinkerpop.gremlin.server.auth.Authenticator;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticationException;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.groovy.plugin.dsl.credential.CredentialGraph;
import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.ImmutableMap;

import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.core.schema.SchemaAction;
import org.janusgraph.graphdb.database.management.ManagementSystem;

import org.mindrot.BCrypt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Base64;
import java.util.Date;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import static org.apache.tinkerpop.gremlin.groovy.plugin.dsl.credential.CredentialGraphTokens.PROPERTY_PASSWORD;
import static org.apache.tinkerpop.gremlin.groovy.plugin.dsl.credential.CredentialGraphTokens.PROPERTY_USERNAME;

import static org.janusgraph.graphdb.tinkerpop.gremlin.server.handler.HttpHMACAuthenticationHandler.PROPERTY_GENERATE_TOKEN;
import static org.janusgraph.graphdb.tinkerpop.gremlin.server.handler.HttpHMACAuthenticationHandler.PROPERTY_TOKEN;
/**
 * A class for doing Basic Auth and Token auth using an HMAC intended to be used with
 * the HMACAuthenticationHandler
 *
 * @author Keith Lohnes lohnesk@gmail.com
 */
public class HMACAuthenticator implements Authenticator {
    private static final Logger logger = LoggerFactory.getLogger(HMACAuthenticator.class);
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

    /**
     * Hmac algorithm defaults to hmacsha256
     */
    public static final String CONFIG_HMAC_ALGO = "hmacAlgo";

    /**
     * How long an auth token should stay valid
     */
    public static final String CONFIG_TOKEN_TIMEOUT = "tokenTimeout";

    /**
     * Hmac secret config
     */
    public static final String CONFIG_HMAC_SECRET = "hmacSecret";

    private static final String AUTH_ERROR = "Username and/or password are incorrect";

    private CredentialGraph credentialGraph = null;

    private String hmacAlgo = "HmacSHA256";

    private String secret = "secret";

    private Long timeout = 3600000L;

    @Override
    public boolean requireAuthentication() {
        return true;
    }

    @Override
    public void setup(final Map<String,Object> config) {
        logger.info("Initializing authentication with the {}", HMACAuthenticator.class.getName());
        if (null == config) {
            throw new IllegalArgumentException(String.format(
                    "Could not configure a %s - provide a 'config' in the 'authentication' settings",
                    HMACAuthenticator.class.getName()));
        }

        if (!config.containsKey(CONFIG_CREDENTIAL_DB)) {
            throw new IllegalStateException(String.format(
                    "Credential configuration missing the %s key that points to a graph config file or graph name", CONFIG_CREDENTIAL_DB));
        }

        if (!config.containsKey(CONFIG_HMAC_SECRET)) {
            throw new IllegalStateException(String.format("Credential configuration missing the %s key", CONFIG_HMAC_SECRET));
        }

        secret = config.get(CONFIG_HMAC_SECRET).toString();

        if (config.containsKey(CONFIG_HMAC_ALGO)) {
            hmacAlgo = config.get(CONFIG_HMAC_ALGO).toString();
        }

        if (config.containsKey(CONFIG_TOKEN_TIMEOUT)) {
            timeout = ((Number) config.get(CONFIG_TOKEN_TIMEOUT)).longValue();
        }

        final JanusGraph graph = openGraph(config.get(CONFIG_CREDENTIAL_DB).toString());
        credentialGraph = createCredentialGraph(graph);

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

        if (credentialGraph.countUsers() == 0) {
            if (!config.containsKey(CONFIG_DEFAULT_USER) || !config.containsKey(CONFIG_DEFAULT_PASSWORD)) {
                throw new IllegalStateException(String.format("If there are no users in your credential graph both %s and %s must be defined", CONFIG_DEFAULT_USER, CONFIG_DEFAULT_PASSWORD));
            }
            credentialGraph.createUser(config.get(CONFIG_DEFAULT_USER).toString(), config.get(CONFIG_DEFAULT_PASSWORD).toString());
        }

    }

    public JanusGraph openGraph(String conf) {
        return JanusGraphFactory.open(conf);
    }

    public CredentialGraph createCredentialGraph(JanusGraph graph) {
        return new CredentialGraph(graph);
    }

    @Override
    public SaslNegotiator newSaslNegotiator(final InetAddress remoteAddress) {
        throw new RuntimeException("HMACAuthenticator does not use SASL!");
    }

    public SaslNegotiator newSaslNegotiator() {
        throw new RuntimeException("HMACAuthenticator does not use SASL!");
    }

    @Override
    public AuthenticatedUser authenticate(final Map<String, String> credentials) throws AuthenticationException {
        if (credentials.get(PROPERTY_GENERATE_TOKEN) != null) {
            final AuthenticatedUser user = authenticateUser(credentials);
            if (user == null) {
                throw new AuthenticationException(AUTH_ERROR);
            }
            credentials.put(PROPERTY_TOKEN, getToken(credentials));
            return user;
        } else if (credentials.get(PROPERTY_TOKEN) != null) {
            if (validateToken(credentials)) {
                return new AuthenticatedUser(credentials.get(PROPERTY_USERNAME));
            } else {
                throw new AuthenticationException("Invalid token");
            }
        } else {
            return authenticateUser(credentials);
        }
    }

    private AuthenticatedUser authenticateUser(final Map<String, String> credentials) throws AuthenticationException {
        Vertex v = credentialGraph.findUser(credentials.get(PROPERTY_USERNAME));
        if (null == v || !BCrypt.checkpw(credentials.get(PROPERTY_PASSWORD), v.value(PROPERTY_PASSWORD))) {
            throw new AuthenticationException(AUTH_ERROR);
        }
        return new AuthenticatedUser(credentials.get(PROPERTY_USERNAME));
    }

    private boolean validateToken(Map<String, String> credentials) {
        final String token = credentials.get(PROPERTY_TOKEN);
        final Map<String, String> tokenMap = parseToken(token);
        final String username = tokenMap.get(PROPERTY_USERNAME);
        final String time = tokenMap.get("time");
        final String password = credentialGraph.findUser(username).value(PROPERTY_PASSWORD);
        final String salt = getBcryptSaltFromStoredPassword(password);
        final String expected = generateToken(username, salt, time);
        final Long timeLong = Long.parseLong(time);
        final Long currentTime = new Date().getTime();
        final String base64Token = new String(Base64.getUrlEncoder().encode(token.getBytes()));

        if (timeLong + timeout < currentTime) {
            return false;
        } else {
            //Don't short circuit comparison to prevent timing attacks
            boolean isValid = true;
            for (int i = 0; i < token.length(); i++) {
                if (base64Token.charAt(i) != expected.charAt(i)) {
                    isValid = false;
                }
            }
            return isValid;
        }
    }

    private Map<String, String> parseToken(final String token) {
        final String[] parts = token.split(":");
        return ImmutableMap.of(PROPERTY_USERNAME, parts[0], "time", parts[1], "hmac", parts[2]);
    }

    private String generateToken(final String username, final String salt, final String time) {
        try {
            String secretAndSalt = secret + ":" + salt;
            String tokenPrefix = username + ":" + time.toString() + ":";
            SecretKeySpec keySpec = new SecretKeySpec(secretAndSalt.getBytes(), hmacAlgo);
            Mac hmac = Mac.getInstance(hmacAlgo);
            hmac.init(keySpec);
            hmac.update(username.getBytes());
            hmac.update(time.toString().getBytes());
            Base64.Encoder encoder = Base64.getUrlEncoder();
            byte[] hmacbytes = encoder.encode(hmac.doFinal());
            byte[] tokenbytes = tokenPrefix.getBytes();
            byte[] token = ByteBuffer.wrap(new byte[tokenbytes.length + hmacbytes.length]).put(tokenbytes).put(hmacbytes).array();
            return new String(encoder.encode(token));
        } catch (Exception ex){
            throw new RuntimeException(ex);
        }
    }

    private String getToken(final Map<String, String> credentials) {
        final String username = credentials.get(PROPERTY_USERNAME);
        final Vertex user = credentialGraph.findUser(username);
        final String password = user.value(PROPERTY_PASSWORD);
        final String salt = getBcryptSaltFromStoredPassword(password);
        final String time = Long.toString(new Date().getTime());
        return generateToken(username, salt, time);
    }

    //In BCrypt, the salt is the 22 chars after the 3rd $
    private String getBcryptSaltFromStoredPassword(String password) {
        Integer saltStart = StringUtils.ordinalIndexOf(password, "$", 3);
        return password.substring(saltStart + 1, saltStart + 23);
    }

}
