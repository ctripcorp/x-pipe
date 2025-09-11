package com.ctrip.xpipe.redis.console.notifier.cluster;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.notifier.shard.ShardDeleteEvent;
import com.ctrip.xpipe.redis.console.notifier.shard.ShardEvent;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author chen.zhu
 *         <p>
 *         Feb 11, 2018
 */
public class ClusterDeleteEventFactoryTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private ClusterDeleteEventFactory clusterDeleteEventFactory;

    @Autowired
    private ClusterService clusterService;

    @Test
    public void createClusterEvent() throws Exception {

        List<ClusterTbl> clusters = clusterService.findAllClustersWithOrgInfo();
        ClusterEvent clusterEvent = clusterDeleteEventFactory.createClusterEvent(clusters.get(0).getClusterName(),
                clusters.get(0));

        Assert.assertTrue(clusterEvent instanceof ClusterDeleteEvent);

        for (ShardEvent shardEvent : clusterEvent.getShardEvents()) {
            Assert.assertTrue(shardEvent instanceof ShardDeleteEvent);
            Assert.assertEquals(clusterEvent.getClusterName(), shardEvent.getClusterName());
        }
    }

    @Test
    public void testMetaCacheGetNull() {
        List<ClusterTbl> clusters = clusterService.findAllClustersWithOrgInfo();
        MetaCache metaCache = mock(MetaCache.class);
        clusterDeleteEventFactory.setMetaCache(metaCache);
        when(metaCache.getSentinelMonitorName(anyString(), anyString())).thenThrow(new RuntimeException());
        ClusterEvent clusterEvent = clusterDeleteEventFactory.createClusterEvent(clusters.get(0).getClusterName(),
                clusters.get(0));

    }

    @Test
    public void testCreateForCRDTCluster() {
        String clusterId = "test-cluster";
        ClusterEvent clusterEvent = clusterDeleteEventFactory.createClusterEvent(clusterId,
                new ClusterTbl().setClusterName(clusterId).setClusterType(ClusterType.BI_DIRECTION.toString()));
        Assert.assertNull(clusterEvent);
    }

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/cluster-event-factory-test.sql");
    }
}