package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.SentinelManager;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHello;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisInstanceInfo;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * @author chen.zhu
 * <p>
 * Feb 13, 2018
 */
public class SentinelCollector4KeeperTest {

    @InjectMocks
    private SentinelCollector4Keeper collector = new SentinelCollector4Keeper();

    @Mock
    private MetaCache metaCache;

    @Mock
    private SentinelManager sentinelManager;

    @Mock
    private AlertManager alertManager;

    private HostPort sentinel = new HostPort("127.0.0.1", 5002);
    private HostPort masterAddr = new HostPort("127.0.0.1", 6379);
    private HostPort redisAddr = new HostPort("127.0.0.1", 6380);
    private String monitorName = "xpipe-test";

    private RedisInstanceInfo info = new DefaultRedisInstanceInfo("dc", "cluster", "shard", redisAddr, "dc2", ClusterType.ONE_WAY);
    private SentinelHello hello = new SentinelHello(sentinel, masterAddr, monitorName);

    @Before
    public void beforeSentinelCollector4KeeperTest() {
        MockitoAnnotations.initMocks(this);
        when(alertManager.shouldAlert(any())).thenReturn(true);
    }

    @Test
    public void testSentinelActionGetAction() {
        boolean masterGood = true, monitorGood = true;
        SentinelCollector4Keeper.SentinelCollectorAction action = SentinelCollector4Keeper
                .SentinelCollectorAction.getAction(masterGood, monitorGood);
        Assert.assertTrue(action == SentinelCollector4Keeper.SentinelCollectorAction.MASTER_GOOD_MONITOR_GOOD);

        masterGood = false;
        action = SentinelCollector4Keeper
                .SentinelCollectorAction.getAction(masterGood, monitorGood);
        Assert.assertTrue(action == SentinelCollector4Keeper.SentinelCollectorAction.MASTER_BAD_MONITOR_GOOD);

        monitorGood = false;
        action = SentinelCollector4Keeper
                .SentinelCollectorAction.getAction(masterGood, monitorGood);
        Assert.assertTrue(action == SentinelCollector4Keeper.SentinelCollectorAction.MASTER_BAD_MONITOR_BAD);

        masterGood = true;
        action = SentinelCollector4Keeper
                .SentinelCollectorAction.getAction(masterGood, monitorGood);
        Assert.assertTrue(action == SentinelCollector4Keeper.SentinelCollectorAction.MASTER_GOOD_MONITOR_BAD);

    }

    @Test
    public void testDoAction1() {
        boolean masterGood = true, monitorGood = true;
        SentinelCollector4Keeper.SentinelCollectorAction action = SentinelCollector4Keeper
                .SentinelCollectorAction.getAction(masterGood, monitorGood);


        action.doAction(collector, hello, info);
        verify(alertManager, never()).alert(any(), any(), any(), any(), any());
    }

    @Test
    public void testDoAction2() {
        doNothing().when(alertManager).alert(any(), any(), any(), any(), any());
        boolean masterGood = true, monitorGood = false;
        SentinelCollector4Keeper.SentinelCollectorAction action = SentinelCollector4Keeper
                .SentinelCollectorAction.getAction(masterGood, monitorGood);

        when(sentinelManager.removeSentinelMonitor(any(),any())).thenReturn(new AbstractCommand<String>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setSuccess("OK");
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        action.doAction(collector, hello, info);

        verify(alertManager).alert(any(RedisInstanceInfo.class), any(), any());
    }

    @Test
    public void testDoAction3() {
        doNothing().when(alertManager).alert(any(), any(), any(), any(), any());
        boolean masterGood = false, monitorGood = false;
        SentinelCollector4Keeper.SentinelCollectorAction action = SentinelCollector4Keeper
                .SentinelCollectorAction.getAction(masterGood, monitorGood);

        action.doAction(collector, hello, info);

        verify(alertManager).alert(any(RedisInstanceInfo.class), any(), any());
    }
}