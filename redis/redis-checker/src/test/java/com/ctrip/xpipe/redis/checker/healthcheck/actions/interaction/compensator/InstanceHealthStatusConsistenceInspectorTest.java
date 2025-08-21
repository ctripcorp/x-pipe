package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.compensator;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.cluster.GroupCheckerLeaderElector;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckInstanceManager;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.DefaultDelayPingActionCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.compensator.data.OutClientInstanceHealthHolder;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.compensator.data.UpDownInstances;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.compensator.data.XPipeInstanceHealthHolder;
import com.ctrip.xpipe.redis.checker.healthcheck.stability.StabilityHolder;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.*;

/**
 * @author lishanglin
 * date 2022/7/24
 */
@RunWith(MockitoJUnitRunner.class)
public class InstanceHealthStatusConsistenceInspectorTest extends AbstractRedisTest {

    @Mock
    private InstanceHealthStatusCollector collector;

    @Mock
    private InstanceStatusAdjuster adjuster;

    @Mock
    private StabilityHolder siteStability;

    @Mock
    private CheckerConfig config;

    @Mock
    private MetaCache metaCache;

    @Mock
    private GroupCheckerLeaderElector leaderElector;

    @Mock
    private XPipeInstanceHealthHolder xpipeInstanceHealthHolder;

    @Mock
    private OutClientInstanceHealthHolder outClientInstanceHealthHolder;

    @Mock
    private DefaultDelayPingActionCollector delayPingActionCollector;

    @Mock
    private HealthCheckInstanceManager healthCheckInstanceManager;

    private InstanceHealthStatusConsistenceInspector inspector;

    private String cluster = "cluster1", shard = "shard1";

    private HostPort master = new HostPort("127.0.0.1", 6379);

    @Before
    public void setupInstanceHealthStatusConsistenceCheckerTest() throws Exception {
        inspector = new InstanceHealthStatusConsistenceInspector(collector, adjuster, leaderElector, siteStability, config, metaCache, delayPingActionCollector, healthCheckInstanceManager);
        Mockito.when(leaderElector.amILeader()).thenReturn(true);
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(getXpipeMeta());
        Mockito.when(metaCache.findClusterShard(any())).thenReturn(Pair.of(cluster, shard));
        Mockito.when(metaCache.findMaster(cluster, shard)).thenReturn(master);
        Mockito.when(collector.collect()).thenReturn(Pair.of(xpipeInstanceHealthHolder, outClientInstanceHealthHolder));
        Mockito.when(config.getPingDownAfterMilli()).thenReturn(10000);
        Mockito.when(config.getRedisReplicationHealthCheckInterval()).thenReturn(2000);
        Mockito.when(config.getDownAfterCheckNums()).thenReturn(5);
        Mockito.when(siteStability.isSiteStable()).thenReturn(true);
    }

    @Override
    protected String getXpipeMetaConfigFile() {
        return "dc-meta-test.xml";
    }

    @Test
    public void testCheckAndAdjust() throws Exception {
        Mockito.when(xpipeInstanceHealthHolder.aggregate(anyMap(), anyInt()))
                .thenReturn(new UpDownInstances(Collections.singleton(new HostPort("10.0.0.1", 6379)),
                        Collections.singleton(new HostPort("10.0.0.2", 6379))));
        Mockito.when(outClientInstanceHealthHolder.extractReadable(any()))
                .thenReturn(new UpDownInstances(Collections.singleton(new HostPort("10.0.0.2", 6379)),
                        Collections.singleton(new HostPort("10.0.0.1", 6379))));
        Mockito.when(xpipeInstanceHealthHolder.aggregate(ArgumentMatchers.eq(master), anyInt())).thenReturn(true);
        inspector.inspect();

        Mockito.verify(adjuster).adjustInstances(ArgumentMatchers.eq(Sets.newHashSet(new HostPort("10.0.0.1", 6379), new HostPort("10.0.0.2", 6379))), anyLong());
    }

    @Test
    public void testNotMarkDownMasterUnhealthyInstances() throws Exception {
        Mockito.when(xpipeInstanceHealthHolder.aggregate(anyMap(), anyInt()))
                .thenReturn(new UpDownInstances(Collections.emptySet(), Collections.singleton(new HostPort("10.0.0.1", 6379))));
        Mockito.when(outClientInstanceHealthHolder.extractReadable(any()))
                .thenReturn(new UpDownInstances(Collections.singleton(new HostPort("10.0.0.1", 6379)), Collections.emptySet()));
        Mockito.when(xpipeInstanceHealthHolder.aggregate(ArgumentMatchers.eq(master), anyInt())).thenReturn(false);
        inspector.inspect();

        Mockito.verify(adjuster, Mockito.never()).adjustInstances(any(), anyLong());
    }

    @Test
    public void testFetchInterestedClusterInstances() {
        Map<String, Set<HostPort>> interestedClusterInstances = inspector.fetchInterestedClusterInstances();
        Set<HostPort> expectedInstances = new HashSet<>();
        getXpipeMeta().getDcs().get("oy").getClusters().get("cluster1").getShards().values()
                .forEach(shardMeta -> {
                    shardMeta.getRedises().forEach(redisMeta -> expectedInstances.add(new HostPort(redisMeta.getIp(), redisMeta.getPort())));
                });
        Assert.assertEquals(Collections.singletonMap("cluster1", expectedInstances), interestedClusterInstances);
    }

}
