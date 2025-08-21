package com.ctrip.xpipe.redis.checker.healthcheck.session;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.CheckerConsoleService;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * @author yu
 * <p>
 * 2023/9/28
 */

@RunWith(MockitoJUnitRunner.class)
public class DefaultRedisSessionManagerTest {

    @InjectMocks
    private DefaultRedisSessionManager sessionManager;

    @Mock
    private CheckerConfig checkerConfig;

    @Mock
    private CheckerConsoleService checkerConsoleService;

    @Mock
    private MetaCache metaCache;


    private List<String> mockDcs = Arrays.asList("jq");

    private List<String> mockClusters = Arrays.asList("cluster1", "cluster2");

    private List<String> mockShards = Arrays.asList("shard1", "shard2");

    private List<String> keepercontainerIps = Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4");
    private List<String> redisIps = Arrays.asList("127.0.1.1", "127.0.1.2", "127.0.1.3", "127.0.1.4", "127.0.1.5", "127.0.1.6", "127.0.1.7", "127.0.1.8");

    private List<String> azIds = Arrays.asList("1", "2");



    @Before
    public void before() throws IOException, SAXException {
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(mockXpipeMeta());
    }


    @Test
    public void getInUseRedises() {
        Set<HostPort> useRedises = sessionManager.getInUseInstances();
        Assert.assertEquals(6, useRedises.size());
    }



    private XpipeMeta mockXpipeMeta() {
        XpipeMeta meta = new XpipeMeta();

        for (String dc: mockDcs) {
            meta.addDc(mockDcMeta(dc));
        }

        return meta;
    }

    private DcMeta mockDcMeta(String dc) {
        DcMeta dcMeta = new DcMeta();
        dcMeta.setId(dc);

        dcMeta.addAz(new AzMeta(azIds.get(0)));
        dcMeta.addAz(new AzMeta(azIds.get(1)));

        dcMeta.addKeeperContainer(mockKeepercontainerMeta(1, keepercontainerIps.get(0), Long.valueOf(azIds.get(0))));
        dcMeta.addKeeperContainer(mockKeepercontainerMeta(3, keepercontainerIps.get(2), Long.valueOf(azIds.get(1))));

        dcMeta.addCluster(mockClusterMeta(mockClusters.get(0)));

        return dcMeta;
    }

    private XpipeMeta MockAllCurrentDcMeta() {
        XpipeMeta meta = new XpipeMeta();
        String dc = mockDcs.get(0);
        DcMeta dcMeta = new DcMeta();
        dcMeta.setId(dc);

        dcMeta.addAz(new AzMeta(azIds.get(0)));
        dcMeta.addAz(new AzMeta(azIds.get(1)));

        dcMeta.addKeeperContainer(mockKeepercontainerMeta(1, keepercontainerIps.get(0), Long.valueOf(azIds.get(0))));
        dcMeta.addKeeperContainer(mockKeepercontainerMeta(2, keepercontainerIps.get(1), Long.valueOf(azIds.get(0))));
        dcMeta.addKeeperContainer(mockKeepercontainerMeta(3, keepercontainerIps.get(2), Long.valueOf(azIds.get(1))));
        dcMeta.addKeeperContainer(mockKeepercontainerMeta(4, keepercontainerIps.get(3), Long.valueOf(azIds.get(1))));

        dcMeta.addCluster(mockClusterMeta(mockClusters.get(0)));
        dcMeta.addCluster(mockClusterMeta(mockClusters.get(1)));

        meta.addDc(dcMeta);

        return meta;
    }

    private KeeperContainerMeta mockKeepercontainerMeta(long id, String ip, Long azId) {
        KeeperContainerMeta keeperContainerMeta = new KeeperContainerMeta();
        keeperContainerMeta.setId(id);
        keeperContainerMeta.setAzId(azId);
        keeperContainerMeta.setIp(ip);
        return keeperContainerMeta;
    }

    private ClusterMeta mockClusterMeta(String cluster) {
        ClusterMeta clusterMeta = new ClusterMeta();
        clusterMeta.setId(cluster);
        clusterMeta.setType(ClusterType.ONE_WAY.toString());
        if (cluster.equalsIgnoreCase(mockClusters.get(0))) {
            ShardMeta shard1 = new ShardMeta("shard1");
            shard1.addKeeper(new KeeperMeta().setIp(keepercontainerIps.get(0)).setPort(6380).setKeeperContainerId(1L));
            shard1.addKeeper(new KeeperMeta().setIp(keepercontainerIps.get(1)).setPort(6380).setKeeperContainerId(2L));
            shard1.addRedis(new RedisMeta().setIp(redisIps.get(0)).setPort(6379).setMaster("127.0.0.1"));
            shard1.addRedis(new RedisMeta().setIp(redisIps.get(1)).setPort(6379));
            clusterMeta.addShard(shard1);

            ShardMeta shard2 = new ShardMeta("shard2");
            shard2.addKeeper(new KeeperMeta().setIp(keepercontainerIps.get(2)).setPort(6380).setKeeperContainerId(3L));
            shard2.addKeeper(new KeeperMeta().setIp(keepercontainerIps.get(3)).setPort(6380).setKeeperContainerId(4L));
            shard2.addRedis(new RedisMeta().setIp(redisIps.get(2)).setPort(6379));
            shard2.addRedis(new RedisMeta().setIp(redisIps.get(3)).setPort(6379).setMaster("127.0.0.1"));
            clusterMeta.addShard(shard2);
        } else {
            ShardMeta shard3 = new ShardMeta("shard3");
            shard3.addKeeper(new KeeperMeta().setIp(keepercontainerIps.get(0)).setPort(6381).setKeeperContainerId(1L));
            shard3.addKeeper(new KeeperMeta().setIp(keepercontainerIps.get(1)).setPort(6381).setKeeperContainerId(2L));
            shard3.addRedis(new RedisMeta().setIp(redisIps.get(4)).setPort(6379).setMaster("127/.0.0.1"));
            shard3.addRedis(new RedisMeta().setIp(redisIps.get(5)).setPort(6379));
            clusterMeta.addShard(shard3);

            ShardMeta shard4 = new ShardMeta("shard4");
            shard4.addKeeper(new KeeperMeta().setIp(keepercontainerIps.get(2)).setPort(6381).setKeeperContainerId(3L));
            shard4.addKeeper(new KeeperMeta().setIp(keepercontainerIps.get(3)).setPort(6381).setKeeperContainerId(4L));
            shard4.addRedis(new RedisMeta().setIp(redisIps.get(6)).setPort(6379).setMaster("127.0.0.1"));
            shard4.addRedis(new RedisMeta().setIp(redisIps.get(7)).setPort(6379));
            clusterMeta.addShard(shard4);
        }

        return clusterMeta;
    }
}