package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.compensator;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.cluster.GroupCheckerLeaderElector;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.compensator.data.OutClientInstanceHealthHolder;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.compensator.data.UpDownInstances;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.compensator.data.XPipeInstanceHealthHolder;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.tuple.Pair;
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
    private CheckerConfig config;

    @Mock
    private MetaCache metaCache;

    @Mock
    private GroupCheckerLeaderElector leaderElector;

    @Mock
    private XPipeInstanceHealthHolder xpipeInstanceHealthHolder;

    @Mock
    private OutClientInstanceHealthHolder outClientInstanceHealthHolder;

    private InstanceHealthStatusConsistenceInspector inspector;

    @Before
    public void setupInstanceHealthStatusConsistenceCheckerTest() throws Exception {
        inspector = new InstanceHealthStatusConsistenceInspector(collector, adjuster, leaderElector, config, metaCache);
        Mockito.when(leaderElector.amILeader()).thenReturn(true);
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(getXpipeMeta());
        Mockito.when(collector.collect()).thenReturn(Pair.of(xpipeInstanceHealthHolder, outClientInstanceHealthHolder));
        Mockito.when(config.getPingDownAfterMilli()).thenReturn(10000);
        Mockito.when(config.getRedisReplicationHealthCheckInterval()).thenReturn(2000);
        Mockito.when(config.getDownAfterCheckNums()).thenReturn(5);
    }

    @Override
    protected String getXpipeMetaConfigFile() {
        return "dc-meta-test.xml";
    }

    @Test
    public void testCheckAndAdjust() throws Exception {
        Mockito.when(xpipeInstanceHealthHolder.aggregate(any(), anyInt()))
                .thenReturn(new UpDownInstances(Collections.singleton(new HostPort("10.0.0.1", 6379)),
                        Collections.singleton(new HostPort("10.0.0.2", 6379))));
        Mockito.when(outClientInstanceHealthHolder.extractReadable(any()))
                .thenReturn(new UpDownInstances(Collections.singleton(new HostPort("10.0.0.2", 6379)),
                        Collections.singleton(new HostPort("10.0.0.1", 6379))));
        inspector.inspect();

        Mockito.verify(adjuster).adjustInstances(ArgumentMatchers.eq(Collections.singleton(new HostPort("10.0.0.1", 6379))),
                ArgumentMatchers.eq(true), anyLong());
        Mockito.verify(adjuster).adjustInstances(ArgumentMatchers.eq(Collections.singleton(new HostPort("10.0.0.2", 6379))),
                ArgumentMatchers.eq(false), anyLong());
    }

    @Test
    public void testFetchInterestedInstance() {
        Set<HostPort> interested = inspector.fetchInterestedInstance();
        Set<HostPort> expected = new HashSet<>();
        getXpipeMeta().getDcs().get("oy").getClusters().get("cluster1").getShards().values()
                .forEach(shardMeta -> {
                    shardMeta.getRedises().forEach(redisMeta -> expected.add(new HostPort(redisMeta.getIp(), redisMeta.getPort())));
                });
        Assert.assertEquals(expected, interested);
    }

}
