package com.ctrip.xpipe.redis.proxy.integrate;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpointManager;
import com.ctrip.xpipe.redis.core.proxy.endpoint.EndpointHealthChecker;
import com.ctrip.xpipe.redis.core.proxy.handler.NettyClientSslHandlerFactory;
import com.ctrip.xpipe.redis.core.proxy.handler.NettyServerSslHandlerFactory;
import com.ctrip.xpipe.redis.proxy.AbstractRedisProxyServerTest;
import com.ctrip.xpipe.redis.proxy.DefaultProxyServer;
import com.ctrip.xpipe.redis.proxy.TestProxyConfig;
import com.ctrip.xpipe.redis.proxy.tunnel.DefaultTunnelManager;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.nio.NioEventLoopGroup;

/**
 * @author chen.zhu
 * <p>
 * May 23, 2018
 */
public class AbstractProxyIntegrationTest extends AbstractRedisProxyServerTest {

    protected void prepare(DefaultProxyServer server) {
        TestProxyConfig config = new TestProxyConfig();
        sameStuff(server, config);
    }

    protected void prepareTLS(DefaultProxyServer server) {
        TestProxyConfig config = new TestProxyConfig();
        config.setSslEnabled(true);
        sameStuff(server, config);
    }

    private void sameStuff(DefaultProxyServer server, TestProxyConfig config) {
        server.setConfig(config);
        server.setServerSslHandlerFactory(new NettyServerSslHandlerFactory(config));
        DefaultProxyEndpointManager endpointManager = new DefaultProxyEndpointManager(()-> 60);
        endpointManager.setHealthChecker(new EndpointHealthChecker() {
            @Override
            public boolean checkConnectivity(Endpoint endpoint) {
                return true;
            }
        });
        server.setTunnelManager(new DefaultTunnelManager()
                .setConfig(config)
                .setFactory(new NettyClientSslHandlerFactory(config))
                .setEndpointManager(endpointManager)
                .setBackendEventLoopGroup(new NioEventLoopGroup(5, XpipeThreadFactory.create("backend")))
        );
    }

    protected String generateSequncialString(int length) {
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < length; i++) {
            sb.append(i).append(' ');
            if(i != 0 && (i % 10) == 0) {
                sb.append("\r\n");
            }
        }
        return sb.toString();
    }

    protected void write(ChannelFuture future, String sendout) {
        future.channel().writeAndFlush(UnpooledByteBufAllocator.DEFAULT.buffer().writeBytes(sendout.getBytes()));
    }
}
