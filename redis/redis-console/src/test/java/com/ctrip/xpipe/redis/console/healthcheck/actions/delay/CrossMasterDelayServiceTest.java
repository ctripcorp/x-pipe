package com.ctrip.xpipe.redis.console.healthcheck.actions.delay;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.console.impl.ConsoleServiceManager;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.impl.DefaultRedisInstanceInfo;
import com.ctrip.xpipe.redis.console.model.consoleportal.UnhealthyInfoModel;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;

@RunWith(MockitoJUnitRunner.class)
public class CrossMasterDelayServiceTest extends AbstractConsoleTest {

    @InjectMocks
    private CrossMasterDelayService service;

    @Mock
    private ConsoleServiceManager consoleServiceManager;

    @Mock
    private RedisHealthCheckInstance instance;

    @Mock
    private HealthCheckAction action;

    @Mock
    private MetaCache metaCache;

    private String clusterId = "cluster1", shardId = "shard1", dcId = FoundationService.DEFAULT.getDataCenter();

    private String remoteDcId = "remote-dc";

    @Before
    public void setupCrossMasterDelayServiceTest() {
        Mockito.when(consoleServiceManager.getCrossMasterDelay(remoteDcId, clusterId, shardId)).thenReturn(Collections.singletonMap(dcId, Pair.of(new HostPort(), 10L)));
        Mockito.when(instance.getRedisInstanceInfo()).thenReturn(new DefaultRedisInstanceInfo(remoteDcId, clusterId, shardId, new HostPort(), null, ClusterType.BI_DIRECTION));
        Mockito.when(action.getActionInstance()).thenReturn(instance);
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(getXpipeMeta());
    }

    @Test
    public void testOnAction() {
        service.onAction(new DelayActionContext(instance, 10 * 1000 * 1000L));
        Assert.assertEquals(Collections.singletonMap(remoteDcId, Pair.of(new HostPort(), 10L)), service.getPeerMasterDelayFromCurrentDc(clusterId, shardId));
        Assert.assertEquals(Collections.singletonMap(remoteDcId, Pair.of(new HostPort(), 10L)), service.getPeerMasterDelayFromSourceDc(dcId, clusterId, shardId));
    }

    @Test
    public void testGetMasterDelayFromRemoteDc() {
        Assert.assertEquals(Collections.singletonMap(dcId, Pair.of(new HostPort(), 10L)), service.getPeerMasterDelayFromSourceDc(remoteDcId, clusterId, shardId));
    }

    @Test
    public void testStopWatch() {
        testOnAction();
        service.stopWatch(action);
        Assert.assertEquals(Collections.EMPTY_MAP, service.getPeerMasterDelayFromCurrentDc(clusterId, shardId));

        Mockito.when(instance.getRedisInstanceInfo()).thenReturn(new DefaultRedisInstanceInfo(dcId, clusterId, shardId, new HostPort(), null, ClusterType.BI_DIRECTION));
        service.stopWatch(action);
        Assert.assertEquals(null, service.getPeerMasterDelayFromCurrentDc(clusterId, shardId));
    }

    @Test
    public void testCrossMasterReplicationHealthy() {
        UnhealthyInfoModel infoModel = service.getCurrentDcUnhealthyMasters();
        Assert.assertEquals(0, infoModel.getUnhealthyClusterNames().size());

        Mockito.when(instance.getRedisInstanceInfo()).thenReturn(new DefaultRedisInstanceInfo("oy", clusterId, shardId, new HostPort(), null, ClusterType.BI_DIRECTION));
        service.onAction(new DelayActionContext(instance, 10L));
        Mockito.when(instance.getRedisInstanceInfo()).thenReturn(new DefaultRedisInstanceInfo("rb", clusterId, shardId, new HostPort(), null, ClusterType.BI_DIRECTION));
        service.onAction(new DelayActionContext(instance, 10L));

        infoModel = service.getCurrentDcUnhealthyMasters();
        Assert.assertEquals(0, infoModel.getUnhealthyRedis());
        Assert.assertEquals(0, infoModel.getUnhealthyClusterNames().size());
    }

    @Test
    public void testCrossMasterReplicationUnhealthy() {
        HostPort expectedMaster = new HostPort("127.0.0.1", 6379);
        Mockito.when(instance.getRedisInstanceInfo()).thenReturn(new DefaultRedisInstanceInfo("oy", clusterId, shardId, new HostPort(), null, ClusterType.BI_DIRECTION));

        service.onAction(new DelayActionContext(instance, DelayAction.SAMPLE_LOST_BUT_PONG));
        UnhealthyInfoModel infoModel = service.getCurrentDcUnhealthyMasters();
        HostPort master = infoModel.getUnhealthyInstance().get(clusterId).values().iterator().next().iterator().next();
        Assert.assertEquals(1, infoModel.getUnhealthyClusterNames().size());
        Assert.assertEquals(expectedMaster, master);

        service.onAction(new DelayActionContext(instance, DelayAction.SAMPLE_LOST_AND_NO_PONG));
        infoModel = service.getCurrentDcUnhealthyMasters();
        master = infoModel.getUnhealthyInstance().get(clusterId).values().iterator().next().iterator().next();
        Assert.assertEquals(1, infoModel.getUnhealthyClusterNames().size());
        Assert.assertEquals(expectedMaster, master);
    }

    @Override
    protected String getXpipeMetaConfigFile() {
        return "crdt-replication-test.xml";
    }

}
