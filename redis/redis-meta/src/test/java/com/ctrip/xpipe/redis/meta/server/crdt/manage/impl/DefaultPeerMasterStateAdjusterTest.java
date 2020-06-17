package com.ctrip.xpipe.redis.meta.server.crdt.manage.impl;

import com.ctrip.xpipe.concurrent.OneThreadTaskExecutor;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.job.PeerMasterAdjustJob;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.util.*;

@RunWith(MockitoJUnitRunner.class)
public class DefaultPeerMasterStateAdjusterTest extends AbstractMetaServerTest {

    private DefaultPeerMasterStateAdjuster adjuster;

    private String clusterId = "cluster1", shardId = "shardId";

    @Mock
    private DcMetaCache dcMetaCache;

    @Mock
    private CurrentMetaManager currentMetaManager;

    @Mock
    private OneThreadTaskExecutor oneThreadTaskExecutor;

    private String currentDc = "jq";

    private Set<String> relatedDcs = Sets.newHashSet("jq", "oy", "rb");

    private Set<String> knownDcs = Sets.newHashSet(currentDc);

    private RedisMeta peerMaster = new RedisMeta().setGid(1).setIp("127.0.0.1").setPort(6379);

    private List<RedisMeta> allPeerMasters = new ArrayList<RedisMeta>() {{
        add(new RedisMeta().setGid(1).setIp("127.0.0.1").setPort(6379));
        add(new RedisMeta().setGid(2).setIp("127.0.0.2").setPort(6379));
        add(new RedisMeta().setGid(3).setIp("127.0.0.3").setPort(6379));
    }};

    @Before
    public void setupDefaultPeerMasterStateAdjusterTest() throws Exception {
        adjuster = new DefaultPeerMasterStateAdjuster(clusterId, shardId, dcMetaCache, currentMetaManager, getXpipeNettyClientKeyedObjectPool(), executors, scheduled);
        adjuster.setAdjustTaskExecutor(oneThreadTaskExecutor);

        Mockito.when(dcMetaCache.getCurrentDc()).thenReturn(currentDc);
        Mockito.doAnswer(invocation -> relatedDcs).when(dcMetaCache).getRelatedDcs(Mockito.anyString(), Mockito.anyString());
        Mockito.doAnswer(invocation -> knownDcs).when(currentMetaManager).getPeerMasterKnownDcs(Mockito.anyString(), Mockito.anyString());
        Mockito.doAnswer(invocation -> peerMaster).when(currentMetaManager).getPeerMaster(currentDc, clusterId, shardId);
        Mockito.doAnswer(invocation -> allPeerMasters).when(currentMetaManager).getAllPeerMasters(clusterId, shardId);
    }

    @Test
    public void testForAdjust() {
        Mockito.doAnswer(invocation -> {
            PeerMasterAdjustJob job = invocation.getArgumentAt(0, PeerMasterAdjustJob.class);
            Assert.assertEquals(clusterId, getInstanceField(job, "clusterId"));
            Assert.assertEquals(shardId, getInstanceField(job, "shardId"));
            Assert.assertEquals(Pair.of("127.0.0.1", 6379), getInstanceField(job, "currentMaster"));
            List<RedisMeta> peerMasters = (List<RedisMeta>) getInstanceField(job, "upstreamPeerMasters");
            Assert.assertEquals(2, peerMasters.size());
            Assert.assertTrue(peerMasters.contains(new RedisMeta().setGid(2).setIp("127.0.0.2").setPort(6379)));
            Assert.assertTrue(peerMasters.contains(new RedisMeta().setGid(3).setIp("127.0.0.3").setPort(6379)));
            return null;
        }).when(oneThreadTaskExecutor).executeCommand(Mockito.any());
        adjuster.adjust();
        Mockito.verify(oneThreadTaskExecutor, Mockito.times(1)).executeCommand(Mockito.any());
    }

    @Test
    public void testForNotKnownDcs() {
        knownDcs = Collections.emptySet();
        adjuster.adjust();
        Mockito.verify(oneThreadTaskExecutor, Mockito.never()).executeCommand(Mockito.any());
    }

    @Test
    public void testForTooMuchKnownDcs() {
        knownDcs = new HashSet<>(relatedDcs);
        knownDcs.add("fra");
        Mockito.doAnswer(invocation -> {
            PeerMasterAdjustJob job = invocation.getArgumentAt(0, PeerMasterAdjustJob.class);
            Assert.assertEquals(clusterId, getInstanceField(job, "clusterId"));
            Assert.assertEquals(shardId, getInstanceField(job, "shardId"));
            Assert.assertEquals(Pair.of("127.0.0.1", 6379), getInstanceField(job, "currentMaster"));
            List<RedisMeta> peerMasters = (List<RedisMeta>) getInstanceField(job, "upstreamPeerMasters");
            Assert.assertEquals(2, peerMasters.size());
            Assert.assertTrue(peerMasters.contains(new RedisMeta().setGid(2).setIp("127.0.0.2").setPort(6379)));
            Assert.assertTrue(peerMasters.contains(new RedisMeta().setGid(3).setIp("127.0.0.3").setPort(6379)));
            return null;
        }).when(oneThreadTaskExecutor).executeCommand(Mockito.any());
        adjuster.adjust();
        Mockito.verify(oneThreadTaskExecutor, Mockito.times(1)).executeCommand(Mockito.any());
        Mockito.verify(currentMetaManager, Mockito.times(1)).removePeerMaster("fra", clusterId, shardId);
    }

    @Test
    public void testForCleanUpPeerMaster() {
        Mockito.doAnswer(invocation -> {
            PeerMasterAdjustJob job = invocation.getArgumentAt(0, PeerMasterAdjustJob.class);
            Assert.assertEquals(clusterId, getInstanceField(job, "clusterId"));
            Assert.assertEquals(shardId, getInstanceField(job, "shardId"));
            Assert.assertEquals(Pair.of("127.0.0.1", 6379), getInstanceField(job, "currentMaster"));
            List<RedisMeta> peerMasters = (List<RedisMeta>) getInstanceField(job, "upstreamPeerMasters");
            Assert.assertEquals(0, peerMasters.size());
            return null;
        }).when(oneThreadTaskExecutor).executeCommand(Mockito.any());
        adjuster.clearPeerMaster();
        Mockito.verify(oneThreadTaskExecutor, Mockito.times(1)).executeCommand(Mockito.any());
    }

    @Test
    public void testForNoMaster() {
        peerMaster = null;
        adjuster.adjust();
        Mockito.verify(oneThreadTaskExecutor, Mockito.never()).executeCommand(Mockito.any());
    }

    private Object getInstanceField(Object instance, String fieldName) throws Exception {
        Field field = instance.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(instance);
    }

}
