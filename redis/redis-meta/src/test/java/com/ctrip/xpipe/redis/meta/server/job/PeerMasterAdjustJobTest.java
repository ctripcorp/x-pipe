package com.ctrip.xpipe.redis.meta.server.job;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.simpleserver.Server;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

@RunWith(MockitoJUnitRunner.class)
public class PeerMasterAdjustJobTest extends AbstractMetaServerTest {

    protected String clusterId = "cluster1", shardId = "shard1";

    protected Server redisServer;

    protected Set<String> peerofRequest = new HashSet<>();

    protected Map<Long, Pair<String, Integer> > currentPeerMaster = new HashMap<Long, Pair<String, Integer> >(){{
       put(1L, Pair.of("10.0.0.1", 6379)); // peer master deleted
       put(2L, Pair.of("10.0.0.2", 6379)); // peer master change
       put(3L, Pair.of("10.0.0.3", 6379)); // peer master unchange

    }};

    protected Map<Long, Pair<String, Integer> > expectPeerMaster = new HashMap<Long, Pair<String, Integer> >(){{
        put(2L, Pair.of("10.0.0.2", 7379)); // peer master change
        put(3L, Pair.of("10.0.0.3", 6379)); // peer master unchange
        put(4L, Pair.of("10.0.0.4", 6379)); // peer master added
    }};

    PeerMasterAdjustJob peerMasterAdjustJob;

    private static final String TEMP_CRDT_INFO = "peer%d_host:%s\r\n" + "peer%d_port:%d\r\n" + "peer%d_gid:%d\r\n";

    @Before
    public void setupPeerMasterChangeJobTest() throws Exception {
        mockMaster();
        peerMasterAdjustJob = new PeerMasterAdjustJob(clusterId, shardId, mockUpstreamPeerMaster(),
                Pair.of("127.0.0.1", redisServer.getPort()), true,
                getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", redisServer.getPort()))
                , scheduled,  executors);
    }

    @Test
    public void testPeerMasterChange() throws Exception {
        peerMasterAdjustJob.execute().get();
        logger.info("[testPeerMasterChange] {}", peerofRequest);
        Assert.assertEquals(3, peerofRequest.size());
        Assert.assertTrue(peerofRequest.contains("peerof 1 no one"));
        Assert.assertTrue(peerofRequest.contains("peerof 2 10.0.0.2 7379"));
        Assert.assertTrue(peerofRequest.contains("peerof 4 10.0.0.4 6379"));
    }

    @Test
    public void testClearPeerMaster() throws Exception {
        peerMasterAdjustJob = new PeerMasterAdjustJob(clusterId, shardId, Collections.emptyList(),
                Pair.of("127.0.0.1", redisServer.getPort()), true,
                getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", redisServer.getPort()))
                , scheduled,  executors);
        peerMasterAdjustJob.execute().get();
        logger.info("[testPeerMasterChange] {}", peerofRequest);
        Assert.assertEquals(3, peerofRequest.size());
        Assert.assertTrue(peerofRequest.contains("peerof 1 no one"));
        Assert.assertTrue(peerofRequest.contains("peerof 2 no one"));
        Assert.assertTrue(peerofRequest.contains("peerof 3 no one"));
    }

    @Test
    public void testAddPeerMaster() throws Exception {
        currentPeerMaster = Collections.emptyMap();
        peerMasterAdjustJob.execute().get();
        logger.info("[testPeerMasterChange] {}", peerofRequest);
        Assert.assertEquals(3, peerofRequest.size());
        Assert.assertTrue(peerofRequest.contains("peerof 2 10.0.0.2 7379"));
        Assert.assertTrue(peerofRequest.contains("peerof 3 10.0.0.3 6379"));
        Assert.assertTrue(peerofRequest.contains("peerof 4 10.0.0.4 6379"));
    }

    @Test
    public void testNoDoDeleted() throws Exception {
        peerMasterAdjustJob = new PeerMasterAdjustJob(clusterId, shardId, mockUpstreamPeerMaster(),
                Pair.of("127.0.0.1", redisServer.getPort()), false,
                getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", redisServer.getPort()))
                , scheduled,  executors);
        peerMasterAdjustJob.execute().get();
        logger.info("[testPeerMasterChange] {}", peerofRequest);
        Assert.assertEquals(2, peerofRequest.size());
        Assert.assertTrue(peerofRequest.contains("peerof 2 10.0.0.2 7379"));
        Assert.assertTrue(peerofRequest.contains("peerof 4 10.0.0.4 6379"));
    }

    @After
    public void afterPeerMasterChangeJobTest() throws Exception {
        redisServer.stop();
    }

    private List<RedisMeta> mockUpstreamPeerMaster() {
        List<RedisMeta> upstreamPeerMasters = new ArrayList<>();
        expectPeerMaster.forEach((gid, peerMaster) -> {
            upstreamPeerMasters.add(new RedisMeta().setGid(gid).setIp(peerMaster.getKey()).setPort(peerMaster.getValue()));
        });

        return upstreamPeerMasters;
    }

    private String mockCRDTInfoResp() {
        StringBuilder sb = new StringBuilder();
        AtomicInteger index = new AtomicInteger(0);
        currentPeerMaster.forEach((gid, peerMaster) -> {
            sb.append(String.format(TEMP_CRDT_INFO, index.get(), peerMaster.getKey(), index.get(), peerMaster.getValue(), index.get(), gid));
            index.incrementAndGet();
        });
        String content = sb.toString();
        return String.format("$%d\r\n%s", content.length(), content);
    }

    private void mockMaster() throws Exception {
        redisServer = startServer(randomPort(), new Function<String, String>() {
            @Override
            public String apply(String s) {
                if (s.startsWith("crdt.info")) {
                    return mockCRDTInfoResp();
                } else if (s.startsWith("peerof")) {
                    peerofRequest.add(s.trim());
                }

                return "+OK\r\n";
            }
        });
    }

}
