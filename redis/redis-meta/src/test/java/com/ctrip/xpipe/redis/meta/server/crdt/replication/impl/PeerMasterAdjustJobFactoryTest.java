package com.ctrip.xpipe.redis.meta.server.crdt.replication.impl;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.job.PeerMasterAdjustJob;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@RunWith(MockitoJUnitRunner.class)
public class PeerMasterAdjustJobFactoryTest extends AbstractMetaServerTest {

    @Mock
    protected DcMetaCache dcMetaCache;

    @Mock
    protected CurrentMetaManager currentMetaManager;

    private DefaultPeerMasterAdjustJobFactory factory;

    private String clusterId = "cluster1", shardId = "shardId";

    private String currentDc = "jq";

    private Set<String> relatedDcs = Sets.newHashSet("jq", "oy", "rb");

    private Set<String> upstreamDcs = Sets.newHashSet();

    private RedisMeta currentMaster = new RedisMeta().setGid(1L).setIp("127.0.0.1").setPort(6379);

    private Map<String, RedisMeta> allPeerMasters = new ConcurrentHashMap<String, RedisMeta>() {{
        put("oy", new RedisMeta().setGid(2L).setIp("127.0.0.2").setPort(6379));
        put("rb",new RedisMeta().setGid(3L).setIp("127.0.0.3").setPort(6379));
    }};

    @Before
    public void setupPeerMasterAdjustJobFactoryTest() throws Exception {
        factory = new DefaultPeerMasterAdjustJobFactory(dcMetaCache, currentMetaManager, getXpipeNettyClientKeyedObjectPool(), executors);

        Mockito.when(dcMetaCache.getCurrentDc()).thenReturn(currentDc);
        Mockito.doAnswer(invocation -> relatedDcs).when(dcMetaCache).getRelatedDcs(Mockito.anyString(), Mockito.anyString());
        Mockito.doAnswer(invocation -> upstreamDcs).when(currentMetaManager).getUpstreamPeerDcs(Mockito.anyString(), Mockito.anyString());
        Mockito.doAnswer(invocation -> currentMaster).when(currentMetaManager).getCurrentCRDTMaster(clusterId, shardId);
        Mockito.doAnswer(invocation -> allPeerMasters).when(currentMetaManager).getAllPeerMasters(clusterId, shardId);
    }

    @Test
    public void testForNotKnownDcs() {
        PeerMasterAdjustJob job = factory.buildPeerMasterAdjustJob(clusterId, shardId);
        Assert.assertNull(job);
    }

    @Test
    public void testForNoMaster() {
        upstreamDcs.add("oy");
        currentMaster = null;
        PeerMasterAdjustJob job = factory.buildPeerMasterAdjustJob(clusterId, shardId);
        Assert.assertNull(job);
    }

    @Test
    public void testForBuild() throws Exception {
        upstreamDcs.add("oy");
        upstreamDcs.add("rb");
        PeerMasterAdjustJob job = factory.buildPeerMasterAdjustJob(clusterId, shardId);
        Assert.assertNotNull(job);
        Assert.assertEquals(clusterId, getInstanceField(job, "clusterId"));
        Assert.assertEquals(shardId, getInstanceField(job, "shardId"));
        Assert.assertEquals(Pair.of(currentMaster.getIp(), currentMaster.getPort()), getInstanceField(job, "currentMaster"));
        Assert.assertEquals(false, getInstanceField(job, "doDelete"));
        Assert.assertEquals(allPeerMasters.entrySet().stream().map(entry -> {
            return new Pair(entry.getValue().getGid(), new DefaultEndPoint(entry.getValue().getIp(), entry.getValue().getPort()));
        }).collect(Collectors.toList()), getInstanceField(job, "upstreamPeerMasters"));
    }

    private Object getInstanceField(Object instance, String fieldName) throws Exception {
        Field field = instance.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(instance);
    }

}
