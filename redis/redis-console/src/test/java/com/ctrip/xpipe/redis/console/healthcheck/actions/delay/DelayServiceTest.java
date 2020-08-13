package com.ctrip.xpipe.redis.console.healthcheck.actions.delay;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.console.impl.ConsoleServiceManager;
import com.ctrip.xpipe.redis.console.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.console.healthcheck.actions.delay.DefaultDelayService;
import com.ctrip.xpipe.redis.console.healthcheck.actions.delay.DelayAction;
import com.ctrip.xpipe.redis.console.healthcheck.actions.delay.DelayActionContext;
import com.ctrip.xpipe.redis.console.healthcheck.impl.DefaultRedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.impl.DefaultRedisInstanceInfo;
import com.ctrip.xpipe.redis.console.model.consoleportal.UnhealthyInfoModel;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.core.entity.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.*;

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
        Mockito.when(metaCache.getAllActiveRedisOfDc(Mockito.anyString(), Mockito.anyString()))
                .thenReturn(Arrays.asList(new HostPort("127.0.0.1", 1000),
                        new HostPort("127.0.0.1", 2000),
                        new HostPort("127.0.0.1", 3000),
                        new HostPort("127.0.0.1", 4000))
                );

        Mockito.when(consoleServiceManager.getUnhealthyInstanceByIdc(Mockito.anyString()))
                .thenAnswer((invocation) -> {
                    String dcName = invocation.getArgumentAt(0, String.class);
                    UnhealthyInfoModel unhealthyInfoModel = new UnhealthyInfoModel();
                    for (String dc: dcs.keySet()) {
                        unhealthyInfoModel.addUnhealthyInstance(dcName + "cluster", dc, "shard1", new HostPort("127.0.0.1", 1000));
                        unhealthyInfoModel.addUnhealthyInstance(dcName + "cluster", dc, "shard1", new HostPort("127.0.0.1", 2000));
                        unhealthyInfoModel.addUnhealthyInstance(dcName + "cluster", dc, "shard2", new HostPort("127.0.0.1", 3000));
                        unhealthyInfoModel.addUnhealthyInstance(dcName + "cluster", dc, "shard2", new HostPort("127.0.0.1", 4000));
                    }

                    return unhealthyInfoModel;
                });
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
        Map<HostPort, Long> redisDelay = new HashMap<HostPort, Long>() {{
            put(new HostPort("127.0.0.1", 1000), DelayAction.SAMPLE_LOST_BUT_PONG);
            put(new HostPort("127.0.0.1", 2000), DelayAction.SAMPLE_LOST_AND_NO_PONG);
            put(new HostPort("127.0.0.1", 3000), 500L);
        }};
        redisDelay.forEach((redis, delay) -> {
            RedisInstanceInfo redisInstanceInfo = new DefaultRedisInstanceInfo(null, null, null, redis, null, ClusterType.ONE_WAY);
            delayService.onAction(new DelayActionContext(new DefaultRedisHealthCheckInstance().setRedisInstanceInfo(redisInstanceInfo), delay));
        });

        UnhealthyInfoModel unhealthyInfo = delayService.getDcActiveClusterUnhealthyInstance(FoundationService.DEFAULT.getDataCenter());
        Assert.assertEquals(1, unhealthyInfo.getUnhealthyCluster());
        Assert.assertEquals(dcs.size(), unhealthyInfo.getUnhealthyShard());
        Assert.assertEquals(dcs.size() * 2 - 1, unhealthyInfo.getUnhealthyRedis());
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

        Mockito.when(metaCache.getAllActiveRedisOfDc(Mockito.anyString(), Mockito.anyString())).thenReturn(redisList);

    }

}
