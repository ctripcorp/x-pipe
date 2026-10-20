package com.ctrip.xpipe.redis.console.healthcheck.availablezone;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Collections;

import static com.ctrip.xpipe.redis.console.healthcheck.availablezone.RedisVacantAzChecker.REDIS_VACANT_AZ_EVENT_TYPE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RedisVacantAzCheckerTest {

    @InjectMocks
    private RedisVacantAzChecker checker;

    @Mock
    private MetaCache metaCache;

    @Mock
    private ConsoleConfig consoleConfig;

    @Mock
    private EventMonitor eventMonitor;

    @Captor
    private ArgumentCaptor<String> typeCaptor;

    @Captor
    private ArgumentCaptor<String> nameCaptor;

    @Captor
    private ArgumentCaptor<Long> countCaptor;

    @Before
    public void setUp() {
        ReflectionTestUtils.setField(checker, "consoleConfig", consoleConfig);
        checker.setEventMonitor(eventMonitor);
        when(consoleConfig.getRedisVacantAzCheckDcs()).thenReturn(Sets.newHashSet("SGP-ALI", "FRA-AWS"));
        when(metaCache.getXpipeMeta()).thenReturn(mockXpipeMeta());
    }

    @Test
    public void testReportOneEventPerProblematicDc() {
        checker.doAction();

        verify(eventMonitor, times(1)).logEvent(typeCaptor.capture(), nameCaptor.capture(), countCaptor.capture());
        assertEquals(REDIS_VACANT_AZ_EVENT_TYPE, typeCaptor.getValue());
        assertEquals("SGP-ALI", nameCaptor.getValue());
        assertEquals(Long.valueOf(2L), countCaptor.getValue());
    }

    @Test
    public void testSkipWhenDcsEmpty() {
        when(consoleConfig.getRedisVacantAzCheckDcs()).thenReturn(Collections.emptySet());

        checker.doAction();

        verify(eventMonitor, never()).logEvent(Mockito.anyString(), Mockito.anyString(), Mockito.anyLong());
    }

    @Test
    public void testSkipDcNotConfigured() {
        when(consoleConfig.getRedisVacantAzCheckDcs()).thenReturn(Sets.newHashSet("SGP-NOR"));

        checker.doAction();

        verify(eventMonitor, never()).logEvent(Mockito.anyString(), Mockito.anyString(), Mockito.anyLong());
    }

    private XpipeMeta mockXpipeMeta() {
        XpipeMeta meta = new XpipeMeta();
        meta.addDc(mockDc("SGP-ALI", true));
        meta.addDc(mockDc("jq", true));
        return meta;
    }

    private DcMeta mockDc(String dc, boolean withVacantAz) {
        DcMeta dcMeta = new DcMeta().setId(dc);

        ClusterMeta clusterMeta = new ClusterMeta().setId("cluster1").setType(ClusterType.ONE_WAY.toString());
        ShardMeta shardMeta = new ShardMeta().setId("shard1");

        RedisMeta withAz = new RedisMeta().setIp("10.0.0.2").setPort(6379).setAz("SGP-ALI-A");
        shardMeta.addRedis(withAz);

        if (withVacantAz) {
            // multiple vacant redis in one dc should still produce one event
            shardMeta.addRedis(new RedisMeta().setIp("10.0.0.1").setPort(6379));
            shardMeta.addRedis(new RedisMeta().setIp("10.0.0.4").setPort(6380));
        }

        // keeper without az should be ignored
        shardMeta.addKeeper(new KeeperMeta().setIp("10.0.0.3").setPort(6380));

        clusterMeta.addShard(shardMeta);
        dcMeta.addCluster(clusterMeta);
        return dcMeta;
    }
}
