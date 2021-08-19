package com.ctrip.xpipe.redis.integratedtest.metaserver;

import com.ctrip.xpipe.redis.integratedtest.console.AbstractXPipeClusterTest;
import com.ctrip.xpipe.redis.integratedtest.metaserver.proxy.LocalProxyConfig;
import com.ctrip.xpipe.redis.integratedtest.metaserver.proxy.LocalResourceManager;
import com.ctrip.xpipe.redis.proxy.DefaultProxyServer;
import com.ctrip.xpipe.redis.proxy.ProxyServer;
import com.ctrip.xpipe.redis.proxy.monitor.DefaultTunnelMonitorManager;
import com.ctrip.xpipe.redis.proxy.monitor.TunnelMonitorManager;
import com.ctrip.xpipe.redis.proxy.monitor.stats.impl.DefaultPingStatsManager;
import com.ctrip.xpipe.redis.proxy.resource.ResourceManager;
import com.ctrip.xpipe.redis.proxy.tunnel.DefaultTunnelManager;
import com.ctrip.xpipe.redis.proxy.tunnel.TunnelManager;
import com.ctrip.xpipe.zk.ZkTestServer;


public class AbstractMetaServerIntegrated extends AbstractXPipeClusterTest {


    protected ZkTestServer startZk(int zkPort) {
        try {
            logger.info(remarkableMessage("[startZK]{}"), zkPort);
            ZkTestServer zkTestServer = new ZkTestServer(zkPort);
            zkTestServer.initialize();
            zkTestServer.start();
            add(zkTestServer);
            return zkTestServer;
        } catch (Exception e) {
            logger.error("[startZk]", e);
            throw new IllegalStateException("[startZk]" + zkPort, e);
        }
    }

    protected ProxyServer startProxyServer( int tcp_port, int tls_port) throws Exception {
        LocalProxyConfig proxyConfig = new LocalProxyConfig();
        proxyConfig.setFrontendTcpPort(tcp_port).setFrontendTlsPort(tls_port);
        ResourceManager resourceManager = new LocalResourceManager(proxyConfig);
        TunnelMonitorManager tunnelMonitorManager = new DefaultTunnelMonitorManager(resourceManager);
        TunnelManager tunnelManager = new DefaultTunnelManager()
                .setConfig(proxyConfig)
                .setProxyResourceManager(resourceManager)
                .setTunnelMonitorManager(tunnelMonitorManager);
        DefaultProxyServer server = new DefaultProxyServer().setConfig(proxyConfig);
        server.setTunnelManager(tunnelManager);
        server.setResourceManager(resourceManager);
        server.setPingStatsManager(new DefaultPingStatsManager());
        server.start();
// subProcessCmds.add(server);
        return server;
    }
}
