package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.dao.ClusterDao;
import com.ctrip.xpipe.redis.console.health.delay.DefaultDelayMonitor;
import com.ctrip.xpipe.redis.console.health.delay.DelayService;
import com.ctrip.xpipe.redis.console.migration.status.ClusterStatus;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.model.consoleportal.ClusterListClusterModel;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.OrganizationService;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Mockito.when;


public class ClusterServiceImplTest2 {

    @Mock
    private DelayService delayService;

    @Mock
    private ClusterDao clusterDao;

    @Mock
    private MetaCache metaCache;

    @InjectMocks
    private ClusterServiceImpl clusterService = new ClusterServiceImpl();

    @Before
    public void beforeClusterServiceImplTest2() {
        MockitoAnnotations.initMocks(this);
        when(metaCache.getXpipeMeta()).thenReturn(buildXpipeMeta());
    }

    @Test
    public void testFindUnhealthyClusters() throws Exception {
        when(delayService.getDelay(any())).thenReturn(10L);
        when(delayService.getDelay(new HostPort("127.0.0.2", 6379))).thenReturn(DefaultDelayMonitor.SAMPLE_LOST_BUT_PONG);
        when(clusterDao.findClustersWithName(Lists.newArrayList("cluster1")))
                .thenReturn(Lists.newArrayList(new ClusterTbl().setClusterName("cluster1")));

        List<ClusterListClusterModel> clusterTbls = clusterService.findUnhealthyClusters();

        Assert.assertEquals(1, clusterTbls.size());

        Assert.assertEquals("cluster1", clusterTbls.get(0).getClusterName());

    }

    @Test
    public void testFindUnhealthyClusters2() throws Exception {
        when(delayService.getDelay(any())).thenReturn(10L);
        when(delayService.getDelay(new HostPort("127.0.0.2", 6379))).thenReturn(DefaultDelayMonitor.SAMPLE_LOST_BUT_PONG);
        when(delayService.getDelay(new HostPort("127.0.0.4", 6380))).thenReturn(DefaultDelayMonitor.SAMPLE_LOST_AND_NO_PONG);
        when(clusterDao.findClustersWithName(anyList())).then(new Answer<List<ClusterTbl>>() {
            @Override
            public List<ClusterTbl> answer(InvocationOnMock invocation) throws Throwable {
                List<String> clusterNames = (List<String>) invocation.getArguments()[0];
                return Lists.transform(clusterNames, new Function<String, ClusterTbl>() {
                    @Override
                    public ClusterTbl apply(String input) {
                        return new ClusterTbl().setClusterName(input);
                    }
                });
            }
        });


        List<ClusterListClusterModel> clusterTbls = clusterService.findUnhealthyClusters();

        Assert.assertEquals(2, clusterTbls.size());

        Assert.assertEquals("cluster1", clusterTbls.get(0).getClusterName());

        Assert.assertEquals("cluster2", clusterTbls.get(1).getClusterName());
    }

    private XpipeMeta buildXpipeMeta() {

        XpipeMeta xpipeMeta = new XpipeMeta();

        String activeDcName = "jq", backupDcName = "oy";
        DcMeta activeDc = new DcMeta(activeDcName);
        DcMeta backupDc = new DcMeta(backupDcName);

        String cluster1 = "cluster1";
        ClusterMeta clusterMeta = new ClusterMeta(cluster1);
        clusterMeta.setParent(activeDc);
        activeDc.addCluster(clusterMeta);
        clusterMeta.setActiveDc(activeDcName);
        clusterMeta.setBackupDcs(backupDcName);

        String cluster2 = "cluster2";
        ClusterMeta clusterMeta2 = new ClusterMeta(cluster2);
        clusterMeta2.setParent(activeDc);
        activeDc.addCluster(clusterMeta2);
        clusterMeta2.setActiveDc(activeDcName);
        clusterMeta2.setBackupDcs(backupDcName);

        String cluster12 = "cluster1";
        ClusterMeta clusterMeta12 = new ClusterMeta(cluster12);
        clusterMeta12.setParent(backupDc);
        backupDc.addCluster(clusterMeta12);
        clusterMeta12.setActiveDc(activeDcName);
        clusterMeta12.setBackupDcs(backupDcName);

        String cluster22 = "cluster2";
        ClusterMeta clusterMeta22 = new ClusterMeta(cluster22);
        clusterMeta22.setParent(backupDc);
        backupDc.addCluster(clusterMeta22);
        clusterMeta22.setActiveDc(activeDcName);
        clusterMeta22.setBackupDcs(backupDcName);

        ShardMeta shardMeta1 = new ShardMeta("shard1");
        shardMeta1.setParent(clusterMeta);
        clusterMeta.addShard(shardMeta1);
        shardMeta1.addRedis(new RedisMeta().setIp("127.0.0.1").setPort(6379));
        shardMeta1.addRedis(new RedisMeta().setIp("127.0.0.2").setPort(6379));

        ShardMeta shardMeta2 = new ShardMeta("shard2");
        shardMeta2.setParent(clusterMeta12);
        clusterMeta12.addShard(shardMeta2);
        shardMeta2.addRedis(new RedisMeta().setIp("127.0.0.1").setPort(6380));
        shardMeta2.addRedis(new RedisMeta().setIp("127.0.0.2").setPort(6380));

        ShardMeta shardMeta12 = new ShardMeta("shard1");
        shardMeta12.setParent(clusterMeta12);
        clusterMeta12.addShard(shardMeta12);
        shardMeta12.addRedis(new RedisMeta().setIp("127.0.0.3").setPort(6379));
        shardMeta12.addRedis(new RedisMeta().setIp("127.0.0.4").setPort(6379));

        ShardMeta shardMeta22 = new ShardMeta("shard2");
        shardMeta22.setParent(clusterMeta22);
        clusterMeta22.addShard(shardMeta22);
        shardMeta22.addRedis(new RedisMeta().setIp("127.0.0.3").setPort(6380));
        shardMeta22.addRedis(new RedisMeta().setIp("127.0.0.4").setPort(6380));

        xpipeMeta.addDc(activeDc).addDc(backupDc);
        return xpipeMeta;
    }
}
