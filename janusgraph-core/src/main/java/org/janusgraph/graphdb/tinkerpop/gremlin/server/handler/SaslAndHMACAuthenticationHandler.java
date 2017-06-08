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

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpMessage;
import org.apache.tinkerpop.gremlin.server.auth.Authenticator;
import org.apache.tinkerpop.gremlin.server.handler.AbstractAuthenticationHandler;
import org.apache.tinkerpop.gremlin.server.handler.SaslAuthenticationHandler;
import org.apache.tinkerpop.gremlin.server.handler.WebSocketHandlerUtil;
import org.janusgraph.graphdb.tinkerpop.gremlin.server.auth.SaslAndHMACAuthenticator;
import org.janusgraph.graphdb.tinkerpop.gremlin.server.auth.JanusGraphSimpleAuthenticator;
import org.janusgraph.graphdb.tinkerpop.gremlin.server.auth.HMACAuthenticator;

import static org.apache.tinkerpop.gremlin.server.channel.HttpChannelizer.PIPELINE_AUTHENTICATOR;

/**
 * A class for doing Basic Auth and Token auth using an HMAC as well as
 * Sasl authentication
 * @author Keith Lohnes lohnesk@gmail.com
 */
@ChannelHandler.Sharable
public class SaslAndHMACAuthenticationHandler extends AbstractAuthenticationHandler {

    private final String HMAC_AUTH = "hmac_authenticator";
    private final String SASL_AUTH = "sasl_authenticator";

    public SaslAndHMACAuthenticationHandler(final Authenticator authenticator) {
        super(authenticator);
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object obj) {
        if (obj instanceof HttpMessage
            && null == ctx.pipeline().get(HMAC_AUTH)
            && !WebSocketHandlerUtil.isWebSocket((HttpMessage)obj)) {
            final HMACAuthenticator hmacAuthenticator = ((SaslAndHMACAuthenticator)authenticator).getHMACAuthenticator();
            final HttpHMACAuthenticationHandler authHandler = new HttpHMACAuthenticationHandler(hmacAuthenticator);
            ctx.pipeline().addAfter(PIPELINE_AUTHENTICATOR, HMAC_AUTH, authHandler);
        } else if (null == ctx.pipeline().get(SASL_AUTH)) {
            final JanusGraphSimpleAuthenticator simpleAuthenticator = ((SaslAndHMACAuthenticator)authenticator).getSimpleAuthenticator();
            final SaslAuthenticationHandler authHandler = new SaslAuthenticationHandler(simpleAuthenticator);
            ctx.pipeline().addAfter(PIPELINE_AUTHENTICATOR, SASL_AUTH, authHandler);
        }
        ctx.fireChannelRead(obj);
    }

}
