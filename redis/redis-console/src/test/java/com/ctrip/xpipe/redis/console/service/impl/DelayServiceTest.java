package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HEALTH_STATE;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HealthStateService;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.console.impl.ConsoleServiceManager;
import com.ctrip.xpipe.redis.console.model.consoleportal.UnhealthyInfoModel;
import com.ctrip.xpipe.redis.console.service.CrossMasterDelayService;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.*;
import java.util.concurrent.TimeUnit;

@RunWith(MockitoJUnitRunner.class)
public class DelayServiceTest {

    @InjectMocks
    DefaultDelayService delayService;

    @Mock
    private MetaCache metaCache;

    @Mock
    private XpipeMeta xpipeMeta;

    @Mock
    private ConsoleServiceManager consoleServiceManager;

    @Mock
    private CrossMasterDelayService crossMasterDelayService;

    @Mock
    private HealthStateService healthStateService;

    @Mock
    private ConsoleConfig consoleConfig;

    private final HashMap<String, DcMeta> dcs = new HashMap<String, DcMeta>() {{
        put("jq", new DcMeta().setId("jq"));
        put("oy", new DcMeta().setId("oy"));
        put("rb", new DcMeta().setId("rb"));
    }};

    @Before
    public void DefaultDelayServiceTestSetUp() {
        Mockito.when(crossMasterDelayService.getCurrentDcUnhealthyMasters()).thenReturn(new UnhealthyInfoModel());
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(xpipeMeta);
        Mockito.when(xpipeMeta.getDcs()).thenReturn(dcs);

        Mockito.when(consoleServiceManager.getUnhealthyInstanceByIdc(Mockito.anyString()))
                .thenAnswer((invocation) -> {
                    String dcName = invocation.getArgument(0, String.class);
                    UnhealthyInfoModel unhealthyInfoModel = new UnhealthyInfoModel();
                    for (String dc: dcs.keySet()) {
                        unhealthyInfoModel.addUnhealthyInstance(dcName + "cluster", dc, "shard1", new HostPort("127.0.0.1", 1000), true);
                        unhealthyInfoModel.addUnhealthyInstance(dcName + "cluster", dc, "shard1", new HostPort("127.0.0.1", 2000), false);
                        unhealthyInfoModel.addUnhealthyInstance(dcName + "cluster", dc, "shard2", new HostPort("127.0.0.1", 3000), false);
                        unhealthyInfoModel.addUnhealthyInstance(dcName + "cluster", dc, "shard2", new HostPort("127.0.0.1", 4000), false);
                    }

                    return unhealthyInfoModel;
                });
        delayService.setFoundationService(FoundationService.DEFAULT);
    }

    @Test
    public void getAllUnhealthyInstanceTest() {
        dcs.remove(FoundationService.DEFAULT.getDataCenter());
        UnhealthyInfoModel unhealthyInfo = delayService.getAllUnhealthyInstance();
        Assert.assertEquals(dcs.size(), unhealthyInfo.getUnhealthyCluster());
        Assert.assertEquals(dcs.size() * dcs.size() * 2, unhealthyInfo.getUnhealthyShard());
        Assert.assertEquals(dcs.size() * dcs.size() * 4, unhealthyInfo.getUnhealthyRedis());
    }

    @Test
    public void getDcActiveClusterUnhealthyInstanceTest() {
        prepareDcMeta();
        Map<HostPort, HEALTH_STATE> allHealthStatus = new HashMap<>();
        allHealthStatus.put(new HostPort("127.0.0.1", 1000), HEALTH_STATE.DOWN);
        allHealthStatus.put(new HostPort("127.0.0.1", 2000), HEALTH_STATE.SICK);
        allHealthStatus.put(new HostPort("127.0.0.1", 3000), HEALTH_STATE.HEALTHY);
        Mockito.when(healthStateService.getAllCachedState()).thenReturn(allHealthStatus);

        String dc = "jq";
        UnhealthyInfoModel unhealthyInfo = delayService.getDcActiveClusterUnhealthyInstance(dc);
        Assert.assertEquals(1, unhealthyInfo.getUnhealthyCluster());
        Assert.assertEquals(2, unhealthyInfo.getUnhealthyRedis());
        System.out.println(unhealthyInfo.getUnhealthyDcShardByCluster("cluster1"));
    }

    @Test
    public void testGetClusterActiveDc() {
        String dcId = "jq";
        HostPort hostPort = new HostPort("127.0.0.1", 1000);
        Pair<String, String> clusterShard = new Pair<>("cluster1", "shard1");
        Mockito.when(metaCache.findClusterShard(hostPort)).thenReturn(clusterShard);
        Mockito.when(metaCache.getClusterType("cluster1")).thenReturn(ClusterType.ONE_WAY);
        Mockito.when(metaCache.getAzGroupType(hostPort)).thenReturn(ClusterType.ONE_WAY);
        Mockito.when(metaCache.getActiveDc(hostPort)).thenReturn(dcId);
        String clusterActiveDc = delayService.getClusterActiveDc(hostPort);
        Assert.assertEquals(clusterActiveDc, dcId);
    }

    @Test
    public void testGetDcClusterDelay() {
        String dcId = "oy";
        String clusterId = "cluster1";
        List<RedisMeta> allInstancesOfDc = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            allInstancesOfDc.add(new RedisMeta().setIp("127.0.0.1").setPort(i));
        }
        Set<String> clusterTypes = new HashSet<>();
        clusterTypes.add("BI_DIRECTION");
        Mockito.when(metaCache.getAllInstanceOfDc(clusterId, dcId)).thenReturn(allInstancesOfDc);
        Mockito.when(metaCache.getClusterType(clusterId)).thenReturn(ClusterType.ONE_WAY);
        Mockito.when(consoleConfig.getOwnClusterType()).thenReturn(clusterTypes);
        Map<HostPort, Long> delay = delayService.getDelay(dcId, clusterId);
        for (Map.Entry<HostPort, Long> entry : delay.entrySet()) {
            Assert.assertEquals(-1L, (long) entry.getValue());
        }
        clusterTypes.add("ONE_WAY");
        delay = delayService.getDelay(dcId, clusterId);
        for (Map.Entry<HostPort, Long> entry : delay.entrySet()) {
            Assert.assertEquals(-1L, (long) entry.getValue());
        }
        Pair<String, String> clusterShard = new Pair<>("cluster1", "shard1");
        for (int i = 0; i < 8; i++) {
            HostPort hostPort = new HostPort("127.0.0.1", i);
            Mockito.when(metaCache.findClusterShard(hostPort)).thenReturn(clusterShard);
            Mockito.when(metaCache.getAzGroupType(hostPort)).thenReturn(ClusterType.ONE_WAY);
            Mockito.when(metaCache.getActiveDc(hostPort)).thenReturn(dcId);
        }
        Mockito.when(metaCache.getClusterType("cluster1")).thenReturn(ClusterType.ONE_WAY);
        Map<HostPort, Long> delayResult = new HashMap<>();
        for (int i = 0; i < 8; i++) {
            HostPort hostPort = new HostPort("127.0.0.1", i);
            delayResult.put(hostPort, 100000L);
        }
        List<HostPort> hostPorts = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            hostPorts.add(new HostPort("127.0.0.1", i));
        }
        Mockito.when(consoleServiceManager.getDelay(hostPorts, dcId)).thenReturn(delayResult);
        delay = delayService.getDelay(dcId, clusterId);
        for (Map.Entry<HostPort, Long> entry : delay.entrySet()) {
            Assert.assertEquals((long)entry.getValue(), TimeUnit.NANOSECONDS.toMillis(delayResult.get(entry.getKey())));
        }
    }

    private void prepareDcMeta() {
        int portCnt = 0;
        List<HostPort> redisList = new ArrayList<>();
        for (DcMeta dcMeta : dcs.values()) {
            ClusterMeta clusterMeta = new ClusterMeta();
            clusterMeta.setType(ClusterType.ONE_WAY.toString());
            clusterMeta.setId("cluster");
            clusterMeta.setActiveDc(FoundationService.DEFAULT.getDataCenter());
            ShardMeta shardMeta = new ShardMeta();
            shardMeta.setId("shard");

            for (int i = 0; i < 2; i++) {
                portCnt++;
                RedisMeta redisMeta = new RedisMeta();
                redisMeta.setIp("127.0.0.1").setPort(portCnt * 1000);
                redisList.add(new HostPort("127.0.0.1", portCnt * 1000));
                shardMeta.addRedis(redisMeta);
            }

            clusterMeta.addShard(shardMeta);
            dcMeta.addCluster(clusterMeta);
        }
    }

}
