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

import static org.junit.Assert.assertNotNull;

import org.apache.tinkerpop.gremlin.server.auth.AuthenticationException;

import org.easymock.EasyMockSupport;
import org.easymock.EasyMock;
import org.junit.Test;

import java.net.InetAddress;

public class SaslAndHMACAuthenticatorTest extends EasyMockSupport {

    @Test(expected = RuntimeException.class)
    public void testNewSaslNegotiator() {
        new SaslAndHMACAuthenticator().newSaslNegotiator();
    }

    @Test(expected = RuntimeException.class)
    public void testNewSaslNegotiatorInet() {
        InetAddress inet = createMock(InetAddress.class);
        new SaslAndHMACAuthenticator().newSaslNegotiator(inet);
    }

    @Test(expected = IllegalStateException.class)
    public void testAuthenticate() throws AuthenticationException {
        new SaslAndHMACAuthenticator().authenticate(null);
    }

    @Test
    public void testGetSimpleAuthenticator() {
        assertNotNull(new SaslAndHMACAuthenticator().getSimpleAuthenticator());
    }

    @Test
    public void testGetHMACAuthenticator() {
        assertNotNull(new SaslAndHMACAuthenticator().getHMACAuthenticator());
    }


}
