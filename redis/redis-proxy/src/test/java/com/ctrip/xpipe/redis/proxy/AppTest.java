package com.ctrip.xpipe.redis.proxy;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpointManager;
import com.ctrip.xpipe.redis.core.proxy.endpoint.EndpointHealthChecker;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpoint;
import com.ctrip.xpipe.redis.core.proxy.handler.NettyClientSslHandlerFactory;
import com.ctrip.xpipe.redis.core.proxy.handler.NettyServerSslHandlerFactory;
import com.ctrip.xpipe.redis.proxy.config.ProxyConfig;
import com.ctrip.xpipe.redis.proxy.tunnel.DefaultTunnelManager;
import com.ctrip.xpipe.spring.AbstractProfile;
import org.junit.Before;
import org.junit.Test;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @author chen.zhu
 * <p>
 * May 14, 2018
 */
@SpringBootApplication
public class AppTest extends AbstractRedisProxyServerTest {

    @Before
    public void beforeAppTest(){
        System.setProperty(AbstractProfile.PROFILE_KEY, AbstractProfile.PROFILE_NAME_TEST);
    }

    @Test
    public void start8992() throws Exception {
        System.setProperty("server.port", "7080");
        DefaultProxyServer server = new DefaultProxyServer(8992);
        prepare(server);
        server.start();
        Thread.sleep(1000 * 60 * 60);
    }

    @Test
    public void start8993() throws Exception {
        System.setProperty("server.port", "7081");
        DefaultProxyServer server = new DefaultProxyServer(8993);
        prepare(server);
        server.start();
        Thread.sleep(1000 * 60 * 60);
    }

    @Test
    public void startListenServer() throws Exception {
        startListenServer(8009).sync().channel().closeFuture().sync();
    }

    private void prepare(DefaultProxyServer server) {
        ProxyConfig config = new TestProxyConfig();
        server.setConfig(config);
        server.setServerSslHandlerFactory(new NettyServerSslHandlerFactory(config));
        DefaultProxyEndpointManager endpointManager = new DefaultProxyEndpointManager(()-> 60);
        endpointManager.setNextJumpAlgorithm(new LocalNextJumpAlgorithm());
        endpointManager.setHealthChecker(new EndpointHealthChecker() {
            @Override
            public boolean checkConnectivity(Endpoint endpoint) {
                ProxyEndpoint proxyEndpoint = (ProxyEndpoint) endpoint;
                if(!proxyEndpoint.isSslEnabled() && proxyEndpoint.getHost().equals("127.0.0.1")) {
                    return true;
                } else {
                    return false;
                }
            }
        });
        server.setTunnelManager(new DefaultTunnelManager()
                .setConfig(config)
                .setFactory(new NettyClientSslHandlerFactory(config))
                .setEndpointManager(endpointManager)
        );
    }
}
