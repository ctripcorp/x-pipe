package com.ctrip.xpipe.redis.checker.impl;

import com.ctrip.xpipe.api.cluster.ClusterServer;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.CheckerConsoleService;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.model.ProxyTunnelInfo;
import com.ctrip.xpipe.redis.checker.model.TunnelStatsInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.web.client.RestOperations;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.mockito.Matchers.anyString;

/**
 * @author lishanglin
 * date 2021/3/17
 */
@RunWith(MockitoJUnitRunner.class)
public class CheckerProxyManagerTest extends AbstractCheckerTest {

    @Mock
    private ClusterServer clusterServer;

    @Mock
    private CheckerConfig checkerConfig;

    @Mock
    private CheckerConsoleService checkerConsoleService;

    @Mock
    private RestOperations restTemplate;

    private CheckerProxyManager manager;

    private String backupDcId = "back-dc";

    private String clusterId = "cluster1";

    private String shardId = "shard1";

    private String peerDcId = "peer-dc";

    @Before
    public void setupCheckerProxyManagerTest() {
        this.manager = new CheckerProxyManager(clusterServer, checkerConfig, checkerConsoleService);
        this.manager.setRestOperations(restTemplate);

        Mockito.when(clusterServer.amILeader()).thenReturn(true);
        Mockito.when(checkerConsoleService.getProxyTunnelInfos(anyString()))
                .thenReturn(Collections.singletonList(mockTunnelInfo()));
    }

    @Test
    public void testRefreshProxyTunnels() {
        manager.refreshProxyTunnels();
        ProxyTunnelInfo proxyTunnelInfo = manager.getProxyTunnelInfo(backupDcId, clusterId, shardId, peerDcId);
        Assert.assertEquals(mockTunnelInfo(), proxyTunnelInfo);
    }

    @Test
    public void testCloseProxyTunnel() {
        manager.refreshProxyTunnels();
        ProxyTunnelInfo proxyTunnelInfo = manager.getProxyTunnelInfo(backupDcId, clusterId, shardId, peerDcId);
        manager.closeProxyTunnel(proxyTunnelInfo);
        Mockito.verify(restTemplate, Mockito.times(1)).delete("http://10.0.0.1:8080/api/tunnel/local/port/1001");
        Mockito.verify(restTemplate, Mockito.times(1)).delete("http://10.0.0.2:8080/api/tunnel/local/port/2001");
    }

    private ProxyTunnelInfo mockTunnelInfo() {
        ProxyTunnelInfo proxyTunnelInfo = new ProxyTunnelInfo();
        List<TunnelStatsInfo> tunnels = new ArrayList<>();
        tunnels.add(new TunnelStatsInfo().setBackend(new HostPort("", 1001))
                .setFrontend(new HostPort("", 443)).setProxyHost(new HostPort("10.0.0.1", 8080)));
        tunnels.add(new TunnelStatsInfo().setBackend(new HostPort("", 2001))
                .setFrontend(new HostPort("", 80)).setProxyHost(new HostPort("10.0.0.2", 8080)));

        proxyTunnelInfo.setBackupDcId(backupDcId).setClusterId(clusterId).setShardId(shardId).setTunnelStatsInfos(tunnels);
        return proxyTunnelInfo;
    }

}
