package com.ctrip.xpipe.redis.comparator.controller;

import com.ctrip.xpipe.redis.comparator.compare.CompareLane;
import com.ctrip.xpipe.redis.comparator.compare.ShardComparator;
import com.ctrip.xpipe.redis.comparator.meta.ShardCompareTaskManager;
import com.ctrip.xpipe.redis.comparator.meta.ShardCompareTaskManager.ShardCompareTask;
import com.ctrip.xpipe.redis.comparator.stream.StreamRingBuffer;
import com.ctrip.xpipe.spring.AbstractController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * 只读状态接口（spec Q14 / T-RP.4）。供 {@code startup.sh} 就绪检查与灰度排障。
 * 无写接口、无任何影响生产的操作。测试 profile 下 TaskManager 可不存在。
 */
@RestController
public class ComparatorStatusController extends AbstractController {

    @Autowired(required = false)
    private ShardCompareTaskManager taskManager;

    @RequestMapping("/health")
    public boolean health() {
        return true;
    }

    @RequestMapping(value = API_PREFIX + "/status", method = RequestMethod.GET)
    public ComparatorStatus status() {
        logger.info("[status]");
        return buildStatus(taskManager == null ? Collections.emptyMap() : taskManager.getTasks());
    }

    ComparatorStatus buildStatus(Map<Long, ShardCompareTask> tasks) {
        List<ShardStatus> shards = new ArrayList<>(tasks.size());
        int streamsRunning = 0;
        for (ShardCompareTask task : tasks.values()) {
            ShardStatus shard = shardStatus(task);
            streamsRunning += shard.getStreams().size();
            shards.add(shard);
        }
        return new ComparatorStatus(tasks.size(), streamsRunning, shards);
    }

    private static ShardStatus shardStatus(ShardCompareTask task) {
        ShardComparator cmp = task.getComparator();
        List<StreamStatus> streams = new ArrayList<>(task.getStreams().size());
        long comparedEnd = cmp == null ? 0L : cmp.getComparedEnd();
        for (CompareLane lane : task.getStreams().values()) {
            streams.add(streamStatus(lane, comparedEnd));
        }
        return new ShardStatus(task.getDbId(), task.getCluster(), task.getShard(),
                cmp == null ? 0L : cmp.getComparedEnd(),
                cmp == null ? 0L : cmp.getComparedBytes(),
                cmp == null ? 0L : cmp.getMismatchCount(),
                cmp == null ? 0L : cmp.getCompareLostCount(),
                cmp == null ? 0L : cmp.getRealignCount(),
                cmp == null ? 0L : cmp.getReplIdMismatchCount(),
                streams);
    }

    private static StreamStatus streamStatus(CompareLane lane, long comparedEnd) {
        StreamRingBuffer buffer = lane.getBuffer();
        long receivedEnd = buffer == null ? 0L : buffer.getReceivedEnd();
        long bufferStart = buffer == null ? 0L : buffer.getBufferStart();
        return new StreamStatus(lane.getAddress(), lane.getReplId(), receivedEnd, bufferStart,
                lane.getReplId() == null ? -1L : lane.getContinueOffset(),
                receivedEnd - comparedEnd, lane.getStreamReconnectCount());
    }

    public static final class ComparatorStatus {
        private final int shardsAssigned;
        private final int streamsRunning;
        private final List<ShardStatus> shards;

        public ComparatorStatus(int shardsAssigned, int streamsRunning, List<ShardStatus> shards) {
            this.shardsAssigned = shardsAssigned;
            this.streamsRunning = streamsRunning;
            this.shards = shards;
        }

        public int getShardsAssigned() { return shardsAssigned; }

        public int getStreamsRunning() { return streamsRunning; }

        public List<ShardStatus> getShards() { return shards; }
    }

    public static final class ShardStatus {
        private final long dbId;
        private final String cluster;
        private final String shard;
        private final long comparedEnd;
        private final long comparedBytes;
        private final long mismatchCount;
        private final long compareLostCount;
        private final long realignCount;
        private final long replIdMismatchCount;
        private final List<StreamStatus> streams;

        public ShardStatus(long dbId, String cluster, String shard, long comparedEnd, long comparedBytes,
                long mismatchCount, long compareLostCount, long realignCount, long replIdMismatchCount,
                List<StreamStatus> streams) {
            this.dbId = dbId;
            this.cluster = cluster;
            this.shard = shard;
            this.comparedEnd = comparedEnd;
            this.comparedBytes = comparedBytes;
            this.mismatchCount = mismatchCount;
            this.compareLostCount = compareLostCount;
            this.realignCount = realignCount;
            this.replIdMismatchCount = replIdMismatchCount;
            this.streams = streams;
        }

        public long getDbId() { return dbId; }

        public String getCluster() { return cluster; }

        public String getShard() { return shard; }

        public long getComparedEnd() { return comparedEnd; }

        public long getComparedBytes() { return comparedBytes; }

        public long getMismatchCount() { return mismatchCount; }

        public long getCompareLostCount() { return compareLostCount; }

        public long getRealignCount() { return realignCount; }

        public long getReplIdMismatchCount() { return replIdMismatchCount; }

        public List<StreamStatus> getStreams() { return streams; }
    }

    public static final class StreamStatus {
        private final String address;
        private final String replId;
        private final long receivedEnd;
        private final long bufferStart;
        private final long continueOffset;
        private final long lagBytes;
        private final int reconnectCount;

        public StreamStatus(String address, String replId, long receivedEnd, long bufferStart,
                long continueOffset, long lagBytes, int reconnectCount) {
            this.address = address;
            this.replId = replId;
            this.receivedEnd = receivedEnd;
            this.bufferStart = bufferStart;
            this.continueOffset = continueOffset;
            this.lagBytes = lagBytes;
            this.reconnectCount = reconnectCount;
        }

        public String getAddress() { return address; }

        public String getReplId() { return replId; }

        public long getReceivedEnd() { return receivedEnd; }

        public long getBufferStart() { return bufferStart; }

        public long getContinueOffset() { return continueOffset; }

        public long getLagBytes() { return lagBytes; }

        public int getReconnectCount() { return reconnectCount; }
    }
}
