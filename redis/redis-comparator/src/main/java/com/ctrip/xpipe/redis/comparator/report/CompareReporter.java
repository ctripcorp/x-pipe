package com.ctrip.xpipe.redis.comparator.report;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 比对事件回调（spec D25 / D26 / D35 ③ / D36 / T-CP.3）。真实现在 Phase RP。
 * <p>
 * 每条回调都带 {@code cluster}/{@code shard}，供日志定位与同分片限流（D36 ③）。
 * Hickwall 周期指标从 {@code ShardComparator} 字段聚合（D36 ②），
 * 禁止在本回调里 {@code MetricProxy.write}。
 */
public interface CompareReporter {

    CompareReporter NOOP = new CompareReporter() {
        @Override
        public void onMismatch(MismatchReport report) {
        }

        @Override
        public void onCompareLost(String cluster, String shard, long comparedEndBefore, long comparedEndAfter) {
        }

        @Override
        public void onReplIdMismatch(String cluster, String shard, List<String> replIds) {
        }
    };

    void onMismatch(MismatchReport report);

    void onCompareLost(String cluster, String shard, long comparedEndBefore, long comparedEndAfter);

    void onReplIdMismatch(String cluster, String shard, List<String> replIds);

    /**
     * 任务拆除时清掉该分片的失配 dump 限流桶（D36 ③）。默认空实现。
     */
    default void forgetShard(String cluster, String shard) {
    }

    final class LaneBytes {
        private final String address;
        private final byte[] chunk;
        private final boolean minority;

        public LaneBytes(String address, byte[] chunk, boolean minority) {
            this.address = Objects.requireNonNull(address, "address");
            this.chunk = Objects.requireNonNull(chunk, "chunk");
            this.minority = minority;
        }

        public String getAddress() { return address; }

        public byte[] getChunk() { return chunk; }

        public boolean isMinority() { return minority; }
    }

    final class MismatchReport {
        private final String cluster;
        private final String shard;
        private final long masterReplOffset;
        private final long comparedBytes;
        private final List<LaneBytes> lanes;
        private final Map<Byte, List<String>> groupsByValue;

        public MismatchReport(String cluster, String shard, long masterReplOffset, long comparedBytes,
                List<LaneBytes> lanes, Map<Byte, List<String>> groupsByValue) {
            this.cluster = Objects.requireNonNull(cluster, "cluster");
            this.shard = Objects.requireNonNull(shard, "shard");
            this.masterReplOffset = masterReplOffset;
            this.comparedBytes = comparedBytes;
            this.lanes = Collections.unmodifiableList(lanes);
            this.groupsByValue = Collections.unmodifiableMap(groupsByValue);
        }

        public String getCluster() { return cluster; }

        public String getShard() { return shard; }

        public long getMasterReplOffset() { return masterReplOffset; }

        public long getComparedBytes() { return comparedBytes; }

        public List<LaneBytes> getLanes() { return lanes; }

        public Map<Byte, List<String>> getGroupsByValue() { return groupsByValue; }
    }
}
