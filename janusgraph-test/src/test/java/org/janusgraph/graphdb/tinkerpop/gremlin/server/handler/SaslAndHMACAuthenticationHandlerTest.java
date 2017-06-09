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
package org.janusgraph.graphdb.tinkerpop.gremlin.server.handler;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.notNull;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.expectLastCall;
import static org.apache.tinkerpop.gremlin.server.channel.HttpChannelizer.PIPELINE_AUTHENTICATOR;
import static io.netty.handler.codec.http.HttpHeaders.Names.UPGRADE;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;

import org.easymock.EasyMockSupport;
import org.easymock.EasyMock;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpHeaders;

import org.janusgraph.graphdb.tinkerpop.gremlin.server.auth.JanusGraphAbstractAuthenticator;
import org.janusgraph.graphdb.tinkerpop.gremlin.server.auth.HMACAuthenticator;
import org.janusgraph.graphdb.tinkerpop.gremlin.server.auth.JanusGraphSimpleAuthenticator;
import org.janusgraph.graphdb.tinkerpop.gremlin.server.auth.SaslAndHMACAuthenticator;
import org.janusgraph.graphdb.tinkerpop.gremlin.server.handler.SaslAndHMACAuthenticationHandler;

import org.junit.Test;

public class SaslAndHMACAuthenticationHandlerTest extends EasyMockSupport {

    @Test
    public void testHttpChannelReadWhenAuthenticatorHasNotBeenAdded() throws Exception {
        HMACAuthenticator hmacAuth = createMock(HMACAuthenticator.class);
        SaslAndHMACAuthenticator authenticator = createMock(SaslAndHMACAuthenticator.class);
        ChannelHandlerContext ctx = createMock(ChannelHandlerContext.class);
        ChannelPipeline pipeline = createMock(ChannelPipeline.class);
        HttpMessage msg = createMock(HttpMessage.class);
        HttpHeaders headers = createMock(HttpHeaders.class);

        expect(authenticator.getHMACAuthenticator()).andReturn(hmacAuth);
        expect(authenticator.getSimpleAuthenticator()).andReturn(createMock(JanusGraphSimpleAuthenticator.class));
        expect(ctx.pipeline()).andReturn(pipeline).times(2);
        expect(pipeline.get("hmac_authenticator")).andReturn(null);
        expect(pipeline.addAfter(eq(PIPELINE_AUTHENTICATOR), eq("hmac_authenticator"), isA(ChannelHandler.class))).andReturn(null);
        expect(msg.headers()).andReturn(headers).times(2);
        expect(headers.get(isA(String.class))).andReturn(null).times(2);
        expect(ctx.fireChannelRead(eq(msg))).andReturn(ctx);
        replayAll();

        SaslAndHMACAuthenticationHandler handler = new SaslAndHMACAuthenticationHandler(authenticator);
        handler.channelRead(ctx, msg);
    }

    @Test
    public void testHttpChannelReadWhenAuthenticatorHasBeenAdded() throws Exception {
        SaslAndHMACAuthenticator authenticator = createMock(SaslAndHMACAuthenticator.class);
        HMACAuthenticator hmacAuth = createMock(HMACAuthenticator.class);
        ChannelHandlerContext ctx = createMock(ChannelHandlerContext.class);
        ChannelHandler mockHandler = createMock(ChannelHandler.class);
        ChannelPipeline pipeline = createMock(ChannelPipeline.class);
        HttpMessage msg = createMock(HttpMessage.class);
        HttpHeaders headers = createMock(HttpHeaders.class);

        expect(authenticator.getHMACAuthenticator()).andReturn(hmacAuth);
        expect(authenticator.getSimpleAuthenticator()).andReturn(createMock(JanusGraphSimpleAuthenticator.class));
        expect(ctx.pipeline()).andReturn(pipeline);
        expect(pipeline.get("hmac_authenticator")).andReturn(mockHandler);
        expect(msg.headers()).andReturn(headers).times(2);
        expect(headers.get(isA(String.class))).andReturn(null).times(2);
        expect(ctx.fireChannelRead(eq(msg))).andReturn(ctx);
        replayAll();

        SaslAndHMACAuthenticationHandler handler = new SaslAndHMACAuthenticationHandler(authenticator);
        handler.channelRead(ctx, msg);
    }

}
