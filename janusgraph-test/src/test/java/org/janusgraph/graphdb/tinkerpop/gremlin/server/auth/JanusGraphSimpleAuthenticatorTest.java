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

import static org.apache.tinkerpop.gremlin.groovy.plugin.dsl.credential.CredentialGraphTokens.PROPERTY_PASSWORD;
import static org.apache.tinkerpop.gremlin.groovy.plugin.dsl.credential.CredentialGraphTokens.PROPERTY_USERNAME;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.notNull;
import static org.easymock.EasyMock.replay;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.groovy.plugin.dsl.credential.CredentialGraph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import org.easymock.EasyMockSupport;
import org.easymock.EasyMock;

import org.mindrot.BCrypt;

import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.PropertyKeyMaker;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.graphdb.tinkerpop.gremlin.server.auth.JanusGraphSimpleAuthenticator;
import org.janusgraph.graphdb.database.management.ManagementSystem;

import org.junit.Test;


import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

public class JanusGraphSimpleAuthenticatorTest extends EasyMockSupport {

    @Test(expected = IllegalArgumentException.class)
    public void testSetupNullConfig() {
        JanusGraphSimpleAuthenticator authenticator = new JanusGraphSimpleAuthenticator();
        authenticator.setup(null);
    }

    @Test(expected = IllegalStateException.class)
    public void testSetupNoCredDb() {
        JanusGraphSimpleAuthenticator authenticator = new JanusGraphSimpleAuthenticator();
        authenticator.setup(new HashMap<String, Object>());
    }

    @Test(expected = IllegalStateException.class)
    public void testSetupEmptyNoUserDefault() {
        JanusGraphSimpleAuthenticator authenticator = createMockBuilder(JanusGraphSimpleAuthenticator.class)
            .addMockedMethod("openGraph")
            .addMockedMethod("createCredentialGraph")
            .createMock();
        JanusGraph graph = createMock(JanusGraph.class);
        CredentialGraph credentialGraph = createMock(CredentialGraph.class);
        Map<String, Object> configMap = new HashMap<String, Object>();
        configMap.put(JanusGraphSimpleAuthenticator.CONFIG_CREDENTIAL_DB, "configCredDb");
        configMap.put(JanusGraphSimpleAuthenticator.CONFIG_DEFAULT_PASSWORD, "pass");

        expect(authenticator.openGraph(isA(String.class))).andReturn(graph);
        expect(authenticator.createCredentialGraph(isA(JanusGraph.class))).andReturn(credentialGraph);
        expect(credentialGraph.countUsers()).andReturn(0l);
        authenticator.setup(configMap);
    }

    @Test(expected=IllegalStateException.class)
    public void testSetupEmptyCredGraphNoPassDefault() {
        JanusGraphSimpleAuthenticator authenticator = createMockBuilder(JanusGraphSimpleAuthenticator.class)
            .addMockedMethod("openGraph")
            .addMockedMethod("createCredentialGraph")
            .createMock();
        JanusGraph graph = createMock(JanusGraph.class);
        CredentialGraph credentialGraph = createMock(CredentialGraph.class);
        Map<String, Object> configMap = new HashMap<String, Object>();
        configMap.put(JanusGraphSimpleAuthenticator.CONFIG_CREDENTIAL_DB, "configCredDb");
        configMap.put(JanusGraphSimpleAuthenticator.CONFIG_DEFAULT_USER, "user");

        expect(authenticator.openGraph(isA(String.class))).andReturn(graph);
        expect(authenticator.createCredentialGraph(isA(JanusGraph.class))).andReturn(credentialGraph);
        expect(credentialGraph.countUsers()).andReturn(0l);
        authenticator.setup(configMap);
    }

    @Test
    public void testSetupEmptyCredGraphNoUserIndex() {
        JanusGraphSimpleAuthenticator authenticator = createMockBuilder(JanusGraphSimpleAuthenticator.class)
            .addMockedMethod("openGraph")
            .addMockedMethod("createCredentialGraph")
            .createMock();

        Map<String, Object> configMap = new HashMap<String, Object>();
        configMap.put(JanusGraphSimpleAuthenticator.CONFIG_CREDENTIAL_DB, "configCredDb");
        configMap.put(JanusGraphSimpleAuthenticator.CONFIG_DEFAULT_PASSWORD, "pass");
        configMap.put(JanusGraphSimpleAuthenticator.CONFIG_DEFAULT_USER, "user");

        JanusGraph graph = createMock(JanusGraph.class);
        CredentialGraph credentialGraph = createMock(CredentialGraph.class);
        ManagementSystem mgmt = createMock(ManagementSystem.class);
        Transaction tx = createMock(Transaction.class);
        PropertyKey pk = createMock(PropertyKey.class);
        PropertyKeyMaker pkm = createMock(PropertyKeyMaker.class);
        JanusGraphManagement.IndexBuilder indexBuilder = createMock(JanusGraphManagement.IndexBuilder.class);
        JanusGraphIndex index = createMock(JanusGraphIndex.class);
        PropertyKey[] pks = {pk};

        expect(authenticator.openGraph(isA(String.class))).andReturn(graph);
        expect(authenticator.createCredentialGraph(isA(JanusGraph.class))).andReturn(credentialGraph);
        expect(credentialGraph.countUsers()).andReturn(0l);
        expect(credentialGraph.createUser(eq("user"), eq("pass"))).andReturn(null);
        expect(graph.openManagement()).andReturn(mgmt);
        expect(graph.tx()).andReturn(tx);
        tx.rollback();
        EasyMock.expectLastCall();
        expect(mgmt.containsGraphIndex(eq("byUsername"))).andReturn(false);
        expect(mgmt.makePropertyKey(PROPERTY_USERNAME)).andReturn(pkm);
        expect(pkm.dataType(eq(String.class))).andReturn(pkm);
        expect(pkm.cardinality(Cardinality.SINGLE)).andReturn(pkm);
        expect(pkm.make()).andReturn(pk);
        expect(mgmt.buildIndex(eq("byUsername"), eq(Vertex.class))).andReturn(indexBuilder);
        expect(mgmt.getGraphIndex(eq("byUsername"))).andReturn(index);
        expect(indexBuilder.addKey(eq(pk))).andReturn(indexBuilder);
        expect(indexBuilder.unique()).andReturn(indexBuilder);
        expect(indexBuilder.buildCompositeIndex()).andReturn(index);
        expect(index.getFieldKeys()).andReturn(pks);
        expect(index.getIndexStatus(eq(pk))).andReturn(SchemaStatus.ENABLED);

        expect(graph.openManagement()).andReturn(mgmt);
        expect(graph.tx()).andReturn(tx);
        tx.rollback();
        EasyMock.expectLastCall();

        mgmt.commit();
        EasyMock.expectLastCall();

        mgmt.rollback();
        EasyMock.expectLastCall();

        replayAll();

        authenticator.setup(configMap);
    }

    @Test
    public void testPassEmptyCredGraphUserIndex() {
        JanusGraphSimpleAuthenticator authenticator = createMockBuilder(JanusGraphSimpleAuthenticator.class)
            .addMockedMethod("openGraph")
            .addMockedMethod("createCredentialGraph")
            .createMock();

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(JanusGraphSimpleAuthenticator.CONFIG_CREDENTIAL_DB, "configCredDb");
        configMap.put(JanusGraphSimpleAuthenticator.CONFIG_DEFAULT_PASSWORD, "pass");
        configMap.put(JanusGraphSimpleAuthenticator.CONFIG_DEFAULT_USER, "user");

        JanusGraph graph = createMock(JanusGraph.class);
        CredentialGraph credentialGraph = createMock(CredentialGraph.class);
        ManagementSystem mgmt = createMock(ManagementSystem.class);
        Transaction tx = createMock(Transaction.class);
        PropertyKey pk = createMock(PropertyKey.class);
        PropertyKeyMaker pkm = createMock(PropertyKeyMaker.class);
        JanusGraphManagement.IndexBuilder indexBuilder = createMock(JanusGraphManagement.IndexBuilder.class);

        expect(authenticator.openGraph(isA(String.class))).andReturn(graph);
        expect(authenticator.createCredentialGraph(isA(JanusGraph.class))).andReturn(credentialGraph);
        expect(credentialGraph.countUsers()).andReturn(0l);
        expect(mgmt.containsGraphIndex(eq("byUsername"))).andReturn(true);
        expect(credentialGraph.createUser(eq("user"), eq("pass"))).andReturn(null);
        expect(graph.openManagement()).andReturn(mgmt);
        expect(graph.tx()).andReturn(tx);
        tx.rollback();
        EasyMock.expectLastCall();
        replayAll();

        authenticator.setup(configMap);
    }
}
