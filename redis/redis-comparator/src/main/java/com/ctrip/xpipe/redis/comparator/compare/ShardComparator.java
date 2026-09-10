package com.ctrip.xpipe.redis.comparator.compare;

import com.ctrip.xpipe.redis.comparator.config.ComparatorConfig;
import com.ctrip.xpipe.redis.comparator.report.CompareReporter;
import com.ctrip.xpipe.redis.comparator.report.CompareReporter.LaneBytes;
import com.ctrip.xpipe.redis.comparator.report.CompareReporter.MismatchReport;
import com.ctrip.xpipe.redis.comparator.stream.StreamRingBuffer.PeekStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * N 路同点比对核心（spec D2 / D25 / D26 / D35 ③ / §4.8.2）。
 * <p>
 * 单步 {@link #compareOnce()}，不起线程（线程壳 Phase CX）。只 peek 不 skip。
 */
public final class ShardComparator {

    public enum CompareOnceResult {
        ADVANCED,
        MISMATCH,
        NO_DATA,
        REALIGNED,
        REPL_ID_RESET
    }

    private static final Logger logger = LoggerFactory.getLogger(ShardComparator.class);

    private final String cluster;

    private final String shard;

    private final List<CompareLane> lanes;

    private final CompareReporter reporter;

    private final int chunkBytes;

    private final byte[][] peekBuf;

    private boolean started;

    private long comparedEnd;

    private long comparedBytes;

    private long mismatchCount;

    private long compareLostCount;

    private long realignCount;

    private long replIdMismatchCount;

    public ShardComparator(String cluster, String shard, List<CompareLane> lanes,
            ComparatorConfig config, CompareReporter reporter) {
        this.cluster = Objects.requireNonNull(cluster, "cluster");
        this.shard = Objects.requireNonNull(shard, "shard");
        if (lanes == null || lanes.size() < 2) {
            throw new IllegalArgumentException("need at least 2 lanes");
        }
        for (CompareLane lane : lanes) {
            Objects.requireNonNull(lane, "lane");
        }
        this.lanes = Collections.unmodifiableList(new ArrayList<>(lanes));
        this.reporter = Objects.requireNonNull(reporter, "reporter");
        this.chunkBytes = Objects.requireNonNull(config, "config").getCompareChunkBytes();
        if (this.chunkBytes <= 0) {
            throw new IllegalArgumentException("compare.chunk.bytes must be positive: " + chunkBytes);
        }
        this.peekBuf = new byte[this.lanes.size()][this.chunkBytes];
    }

    /**
     * 起点 {@code S0 = max(各路 continue offset)}；replId 不一致则全断重建
     *（{@code disconnect + reconnect}，新流 CONTINUE 时换新 RingBuffer），不计失配。
     */
    public boolean alignStart() {
        for (CompareLane lane : lanes) {
            if (lane.getReplId() == null) {
                started = false;
                return false;
            }
        }
        String first = lanes.get(0).getReplId();
        List<String> replIds = new ArrayList<>(lanes.size());
        long s0 = Long.MIN_VALUE;
        boolean same = true;
        for (CompareLane lane : lanes) {
            String id = lane.getReplId();
            replIds.add(id);
            if (!first.equals(id)) {
                same = false;
            }
            s0 = Math.max(s0, lane.getContinueOffset());
        }
        if (!same) {
            logger.warn("[replIdMismatch] cluster={}, shard={}, replIds={}", cluster, shard, replIds);
            replIdMismatchCount++;
            reporter.onReplIdMismatch(cluster, shard, replIds);
            for (CompareLane lane : lanes) {
                lane.disconnect();
                lane.reconnect();
            }
            started = false;
            return false;
        }
        comparedEnd = s0;
        started = true;
        return true;
    }

    public CompareOnceResult compareOnce() {
        if (!started) {
            long before = replIdMismatchCount;
            if (!alignStart()) {
                return replIdMismatchCount > before ? CompareOnceResult.REPL_ID_RESET : CompareOnceResult.NO_DATA;
            }
        }
        long minReceived = Long.MAX_VALUE;
        for (CompareLane lane : lanes) {
            minReceived = Math.min(minReceived, lane.getBuffer().getReceivedEnd());
        }
        long avail = minReceived - comparedEnd;
        if (avail <= 0) {
            return CompareOnceResult.NO_DATA;
        }
        int n = (int) Math.min(chunkBytes, avail);
        boolean overwritten = false;
        boolean notYet = false;
        for (int i = 0; i < lanes.size(); i++) {
            PeekStatus status = lanes.get(i).getBuffer().peek(comparedEnd, n, peekBuf[i]);
            if (status == PeekStatus.OVERWRITTEN) {
                overwritten = true;
            } else if (status != PeekStatus.HIT) {
                notYet = true;
            }
        }
        if (overwritten) {
            compareLostCount++;
            long from = comparedEnd;
            realign();
            reporter.onCompareLost(cluster, shard, from, comparedEnd);
            return CompareOnceResult.REALIGNED;
        }
        if (notYet) {
            return CompareOnceResult.NO_DATA;
        }
        int firstDiff = firstDiff(n);
        if (firstDiff < 0) {
            comparedEnd += n;
            comparedBytes += n;
            return CompareOnceResult.ADVANCED;
        }
        mismatchCount++;
        reporter.onMismatch(buildMismatch(firstDiff, n));
        comparedEnd += n;
        comparedBytes += n;
        return CompareOnceResult.MISMATCH;
    }

    /**
     * {@code comparedEnd = max(comparedEnd, max(各路 bufferStart))}，立即恢复、不等待、不动缓冲区。
     */
    public void realign() {
        long sPrime = comparedEnd;
        for (CompareLane lane : lanes) {
            sPrime = Math.max(sPrime, lane.getBuffer().getBufferStart());
        }
        comparedEnd = sPrime;
        realignCount++;
    }

    public String getCluster() { return cluster; }

    public String getShard() { return shard; }

    public long getComparedEnd() { return comparedEnd; }

    public long getComparedBytes() { return comparedBytes; }

    public long getMismatchCount() { return mismatchCount; }

    public long getCompareLostCount() { return compareLostCount; }

    public long getRealignCount() { return realignCount; }

    public long getReplIdMismatchCount() { return replIdMismatchCount; }

    public boolean isStarted() { return started; }

    private int firstDiff(int n) {
        for (int i = 0; i < n; i++) {
            byte b = peekBuf[0][i];
            for (int lane = 1; lane < lanes.size(); lane++) {
                if (peekBuf[lane][i] != b) {
                    return i;
                }
            }
        }
        return -1;
    }

    private MismatchReport buildMismatch(int firstDiff, int n) {
        Map<Byte, List<String>> groups = new LinkedHashMap<>();
        for (int i = 0; i < lanes.size(); i++) {
            byte value = peekBuf[i][firstDiff];
            groups.computeIfAbsent(value, k -> new ArrayList<>()).add(lanes.get(i).getAddress());
        }
        int maxSize = 0;
        for (List<String> members : groups.values()) {
            maxSize = Math.max(maxSize, members.size());
        }
        List<LaneBytes> laneBytes = new ArrayList<>(lanes.size());
        for (int i = 0; i < lanes.size(); i++) {
            byte value = peekBuf[i][firstDiff];
            boolean minority = groups.get(value).size() < maxSize;
            laneBytes.add(new LaneBytes(lanes.get(i).getAddress(), copyChunk(peekBuf[i], n), minority));
        }
        return new MismatchReport(cluster, shard, comparedEnd + firstDiff, comparedBytes,
                laneBytes, groups);
    }

    private static byte[] copyChunk(byte[] src, int n) {
        byte[] copy = new byte[n];
        System.arraycopy(src, 0, copy, 0, n);
        return copy;
    }
}
