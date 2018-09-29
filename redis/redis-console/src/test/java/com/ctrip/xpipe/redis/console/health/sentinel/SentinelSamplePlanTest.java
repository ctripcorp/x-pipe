package com.ctrip.xpipe.redis.console.health.sentinel;

import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author chen.zhu
 * <p>
 * Aug 27, 2018
 */
public class SentinelSamplePlanTest extends AbstractRedisTest {

    private SentinelSamplePlan plan;

    @Mock
    private ConsoleConfig consoleConfig;

    @Before
    public void beforeSentinelSamplePlanTest() {
        MockitoAnnotations.initMocks(this);
        plan = new SentinelSamplePlan("cluster", "shard", consoleConfig);
    }

    @Test
    public void testIsOnlyActiveDcAvailableCheck() {
        ShardMeta shardMeta = mock(ShardMeta.class);
        when(shardMeta.getActiveDc()).thenReturn("SHAOY");
        when(shardMeta.getBackupDcs()).thenReturn("FRA-AWS");
        RedisMeta redisMeta = new RedisMeta().setIp("localhost").setPort(6379).setParent(shardMeta);
        when(consoleConfig.getIgnoredHealthCheckDc()).thenReturn(Sets.newHashSet("FRA-AWS"));

        boolean result = plan.isOnlyActiveDcAvailableCheck("SHAOY", redisMeta);
        Assert.assertTrue(result);

        when(shardMeta.getBackupDcs()).thenReturn("FRA-AWS,SHAJQ");
        result = plan.isOnlyActiveDcAvailableCheck("SHAOY", redisMeta);
        Assert.assertFalse(result);
    }

    @Test
    public void testAddRedis() {
        ShardMeta shardMeta = mock(ShardMeta.class);
        when(shardMeta.getActiveDc()).thenReturn("SHAOY");
        when(shardMeta.getBackupDcs()).thenReturn("FRA-AWS");
        RedisMeta redisMeta = new RedisMeta().setIp("localhost").setPort(6379).setParent(shardMeta);
        when(consoleConfig.getIgnoredHealthCheckDc()).thenReturn(Sets.newHashSet("FRA-AWS"));

        plan.addRedis("SHAOY", redisMeta, new InstanceSentinelResult());

        Assert.assertEquals(1, plan.getHostPort2SampleResult().size());
    }

    @Test
    public void testAddRedisWithOneDcBanned() {
        ShardMeta shardMeta = mock(ShardMeta.class);
        when(shardMeta.getActiveDc()).thenReturn("SHAOY");
        when(shardMeta.getBackupDcs()).thenReturn("FRA-AWS,SHAJQ");
        RedisMeta redisMeta = new RedisMeta().setIp("localhost").setPort(6379).setParent(shardMeta);
        when(consoleConfig.getIgnoredHealthCheckDc()).thenReturn(Sets.newHashSet("FRA-AWS"));

        plan.addRedis("SHAOY", redisMeta, new InstanceSentinelResult());

        Assert.assertEquals(0, plan.getHostPort2SampleResult().size());
    }
}