package com.ctrip.xpipe.redis.console.healthcheck.actions.sentinel;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.alert.AlertManager;
import com.ctrip.xpipe.redis.console.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.console.healthcheck.impl.DefaultRedisInstanceInfo;
import com.ctrip.xpipe.redis.console.redis.SentinelManager;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

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

    private RedisInstanceInfo info = new DefaultRedisInstanceInfo("dc", "cluster", "shard", redisAddr, "dc2");
    private SentinelHello hello = new SentinelHello(sentinel, masterAddr, monitorName);

    @Before
    public void beforeSentinelCollector4KeeperTest() {
        MockitoAnnotations.initMocks(this);
        doNothing().when(sentinelManager).removeSentinelMonitor(any(), any());
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
        doNothing().when(sentinelManager).removeSentinelMonitor(any(), any());
        doNothing().when(alertManager).alert(any(), any(), any(), any(), any());
        boolean masterGood = true, monitorGood = false;
        SentinelCollector4Keeper.SentinelCollectorAction action = SentinelCollector4Keeper
                .SentinelCollectorAction.getAction(masterGood, monitorGood);

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