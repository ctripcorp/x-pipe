package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.delay.DelayAction;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.delay.DelayActionContext;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.console.impl.ConsoleServiceManager;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisInstanceInfo;
import com.ctrip.xpipe.redis.console.model.consoleportal.UnhealthyInfoModel;
import com.ctrip.xpipe.redis.console.service.CrossMasterDelayService;
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

import java.util.Collections;

@RunWith(MockitoJUnitRunner.class)
public class CrossMasterDelayServiceTest extends AbstractCheckerTest {

    @InjectMocks
    private DefaultCrossMasterDelayService service = new DefaultCrossMasterDelayService(FoundationService.DEFAULT.getDataCenter());

    @Mock
    private ConsoleServiceManager consoleServiceManager;

    @Mock
    private RedisHealthCheckInstance instance;

    @Mock
    private HealthCheckAction action;
    
    @Mock 
    private ConsoleConfig config; 

    @Mock
    private MetaCache metaCache;

    private String clusterId = "cluster1", shardId = "shard1", dcId = FoundationService.DEFAULT.getDataCenter();

    private String remoteDcId = "remote-dc";

    @Before
    public void setupCrossMasterDelayServiceTest() {
        service.setConsoleConfig(config);
        Mockito.when(consoleServiceManager.getCrossMasterDelay(remoteDcId, clusterId, shardId)).thenReturn(Collections.singletonMap(dcId, Pair.of(new HostPort(), 10L)));
        Mockito.when(instance.getCheckInfo()).thenReturn(new DefaultRedisInstanceInfo(remoteDcId, clusterId, shardId, new HostPort(), null, ClusterType.BI_DIRECTION));
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

        Mockito.when(instance.getCheckInfo()).thenReturn(new DefaultRedisInstanceInfo(dcId, clusterId, shardId, new HostPort(), null, ClusterType.BI_DIRECTION));
        service.stopWatch(action);
        Assert.assertEquals(null, service.getPeerMasterDelayFromCurrentDc(clusterId, shardId));
    }

    @Test
    public void testCrossMasterReplicationHealthy() {
        UnhealthyInfoModel infoModel = service.getCurrentDcUnhealthyMasters();
        Assert.assertEquals(0, infoModel.getUnhealthyClusterNames().size());

        Mockito.when(instance.getCheckInfo()).thenReturn(new DefaultRedisInstanceInfo("oy", clusterId, shardId, new HostPort(), null, ClusterType.BI_DIRECTION));
        service.onAction(new DelayActionContext(instance, 10L));
        Mockito.when(instance.getCheckInfo()).thenReturn(new DefaultRedisInstanceInfo("rb", clusterId, shardId, new HostPort(), null, ClusterType.BI_DIRECTION));
        service.onAction(new DelayActionContext(instance, 10L));

        infoModel = service.getCurrentDcUnhealthyMasters();
        Assert.assertEquals(0, infoModel.getUnhealthyRedis());
        Assert.assertEquals(0, infoModel.getUnhealthyClusterNames().size());
    }

    @Test
    public void testCrossMasterReplicationUnhealthy() {
        UnhealthyInfoModel.RedisHostPort expectedMaster = new UnhealthyInfoModel.RedisHostPort(new HostPort("127.0.0.1", 6379), true);
        Mockito.when(instance.getCheckInfo()).thenReturn(new DefaultRedisInstanceInfo("oy", clusterId, shardId, new HostPort(), null, ClusterType.BI_DIRECTION));

        service.onAction(new DelayActionContext(instance, DelayAction.SAMPLE_LOST_BUT_PONG));
        UnhealthyInfoModel infoModel = service.getCurrentDcUnhealthyMasters();
        UnhealthyInfoModel.RedisHostPort master = infoModel.getUnhealthyInstance().get(clusterId).values().iterator().next().iterator().next();
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
