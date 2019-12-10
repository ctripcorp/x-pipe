package com.ctrip.xpipe.redis.console.healthcheck.meta;

import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckInstanceManager;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaClone;
import com.ctrip.xpipe.redis.core.meta.MetaUtils;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterMetaComparator;
import org.apache.tomcat.InstanceManager;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class DefaultDcMetaChangeManagerTest extends AbstractRedisTest {

    private DefaultDcMetaChangeManager manager;

    @Mock
    private HealthCheckInstanceManager instanceManager;


    @Before
    public void beforeDefaultDcMetaChangeManagerTest() {
        MockitoAnnotations.initMocks(this);
        when(instanceManager.getOrCreate(any())).thenReturn(null);
        manager = new DefaultDcMetaChangeManager(instanceManager);
    }

    @Test
    public void compare() {

    }

    @Test
    public void visitAdded() {
        manager.visitAdded(getDcMeta("oy").findCluster("cluster2"));
        verify(instanceManager, never()).getOrCreate(any());
    }

    @Test
    public void visitModified() {
        ClusterMeta clusterMeta = getDcMeta("oy").findCluster("cluster2");
        ClusterMeta clone = MetaClone.clone(clusterMeta);
        clone.getShards().get("shard2").addRedis(new RedisMeta());
        manager.visitModified(new ClusterMetaComparator(clusterMeta, clone));
        verify(instanceManager, never()).getOrCreate(any());
    }

    @Test
    public void visitRemoved() {
        manager = spy(manager);
        manager.compare(getDcMeta("oy"));
        verify(manager, never()).visitModified(any());
        verify(manager, never()).visitAdded(any());
        verify(manager, never()).visitRemoved(any());

        DcMeta dcMeta = MetaClone.clone(getDcMeta("oy"));

        ClusterMeta clusterMeta = dcMeta.getClusters().remove("cluster1");
        clusterMeta.setId("cluster3").getShards().values().forEach(shardMeta -> {
            shardMeta.setParent(clusterMeta);
            for (RedisMeta redis : shardMeta.getRedises()) {
                redis.setParent(shardMeta);
            }
        });
        dcMeta.addCluster(clusterMeta);
        manager.compare(dcMeta);
        verify(manager, atLeastOnce()).visitRemoved(any());
        verify(manager, atLeastOnce()).visitAdded(any());
        verify(manager, never()).visitModified(any());
    }



    protected String getXpipeMetaConfigFile() {
        return "dc-meta-test.xml";
    }
}