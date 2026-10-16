package com.ctrip.xpipe.redis.comparator.controller;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.comparator.compare.CompareLane;
import com.ctrip.xpipe.redis.comparator.compare.ShardComparator;
import com.ctrip.xpipe.redis.comparator.config.ComparatorConfig;
import com.ctrip.xpipe.redis.comparator.controller.ComparatorStatusController.ComparatorStatus;
import com.ctrip.xpipe.redis.comparator.controller.ComparatorStatusController.ShardStatus;
import com.ctrip.xpipe.redis.comparator.controller.ComparatorStatusController.StreamStatus;
import com.ctrip.xpipe.redis.comparator.meta.ShardCompareTaskManager.ShardCompareTask;
import com.ctrip.xpipe.redis.comparator.report.CompareReporter;
import com.ctrip.xpipe.redis.comparator.stream.StreamRingBuffer;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class ComparatorStatusControllerTest extends AbstractTest {

    @Test
    public void testHealthAndEmptyStatusWhenManagerAbsent() {
        ComparatorStatusController controller = new ComparatorStatusController();
        Assert.assertTrue(controller.health());
        ComparatorStatus status = controller.status();
        Assert.assertEquals(0, status.getShardsAssigned());
        Assert.assertEquals(0, status.getStreamsRunning());
        Assert.assertTrue(status.getShards().isEmpty());
    }

    @Test
    public void testStatusReturnsAssignedShardsAndStreams() {
        FakeLane a = new FakeLane("10.0.0.1:6380", 10, 16, 4);
        FakeLane b = new FakeLane("10.0.0.2:6380", 10, 16, 0);
        a.write(new byte[]{1, 2, 3, 4, 5, 6});
        b.write(new byte[]{1, 2, 3, 4});
        ShardComparator cmp = new ShardComparator("c1", "s1", Arrays.asList(a, b),
                new ComparatorConfig(), CompareReporter.NOOP);
        Assert.assertEquals(ShardComparator.CompareOnceResult.ADVANCED, cmp.compareOnce());

        ShardCompareTask task = new ShardCompareTask(10L, "c1", "s1");
        Map<String, CompareLane> streams = new LinkedHashMap<>();
        streams.put(a.getAddress(), a);
        streams.put(b.getAddress(), b);
        task.bind(cmp, streams);

        ComparatorStatusController controller = new ComparatorStatusController();
        ComparatorStatus status = controller.buildStatus(Collections.singletonMap(10L, task));
        Assert.assertEquals(1, status.getShardsAssigned());
        Assert.assertEquals(2, status.getStreamsRunning());
        ShardStatus shard = status.getShards().get(0);
        Assert.assertEquals(10L, shard.getDbId());
        Assert.assertEquals("c1", shard.getCluster());
        Assert.assertEquals("s1", shard.getShard());
        Assert.assertEquals(14L, shard.getComparedEnd());
        Assert.assertEquals(4L, shard.getComparedBytes());
        Assert.assertEquals(2, shard.getStreams().size());
        StreamStatus first = shard.getStreams().get(0);
        Assert.assertEquals("10.0.0.1:6380", first.getAddress());
        Assert.assertEquals("rid", first.getReplId());
        Assert.assertEquals(16L, first.getReceivedEnd());
        Assert.assertEquals(2L, first.getLagBytes());
        Assert.assertEquals(4, first.getReconnectCount());
        Assert.assertEquals(0L, shard.getStreams().get(1).getLagBytes());
    }

    @Test
    public void testControllerIsReadOnly() throws Exception {
        String text = new String(Files.readAllBytes(
                Paths.get("src/main/java/com/ctrip/xpipe/redis/comparator/controller/ComparatorStatusController.java")),
                StandardCharsets.UTF_8);
        Assert.assertFalse(text.contains("POST") || text.contains("PUT") || text.contains("DELETE"));
        Assert.assertFalse(text.contains("RequestMethod.POST"));
        Assert.assertTrue(text.contains("/health"));
        Assert.assertTrue(text.contains("/status"));
    }

    static final class FakeLane implements CompareLane {
        private final String address;
        private final StreamRingBuffer buffer;
        private final int reconnect;
        private final long continueOffset;

        FakeLane(String address, long start, int capacity, int reconnect) {
            this.address = address;
            this.continueOffset = start;
            this.buffer = new StreamRingBuffer(capacity, start);
            this.reconnect = reconnect;
        }

        void write(byte[] data) {
            buffer.write(Unpooled.wrappedBuffer(data));
        }

        @Override
        public String getAddress() { return address; }

        @Override
        public String getReplId() { return "rid"; }

        @Override
        public long getContinueOffset() { return continueOffset; }

        @Override
        public StreamRingBuffer getBuffer() { return buffer; }

        @Override
        public void disconnect() { }

        @Override
        public void reconnect() { }

        @Override
        public void close() { }

        @Override
        public int getStreamReconnectCount() { return reconnect; }
    }
}
