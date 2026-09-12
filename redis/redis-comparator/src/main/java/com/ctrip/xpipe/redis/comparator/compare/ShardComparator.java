package com.ctrip.xpipe.redis.comparator.compare;

import com.ctrip.xpipe.redis.comparator.config.ComparatorConfig;
import com.ctrip.xpipe.redis.comparator.config.ComparatorConstants;
import com.ctrip.xpipe.redis.comparator.report.CompareReporter;
import com.ctrip.xpipe.redis.comparator.report.CompareReporter.LaneBytes;
import com.ctrip.xpipe.redis.comparator.report.CompareReporter.MismatchReport;
import com.ctrip.xpipe.redis.comparator.stream.StreamRingBuffer.PeekStatus;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadFactory;

/**
 * N 路同点比对核心（spec D2 / D25 / D26 / D33 / D35 ③ / §4.8.2）。
 * <p>
 * {@link #compareOnce()} 仍可同步驱动。生产由 {@link #start()} 起本分片专属
 * daemon 线程跑循环；无可比数据时仅在该线程有界等待 {@code COMPARE_WAIT_MILLI}。
 * 正常事件（{@code onCommands} / CONTINUE）经 {@link #wake()} {@code notify}；
 * 未唤醒则超时自旋。只 peek 不 skip。构造允许 0 路（{@code compareOnce} 在
 * {@code <2} 路回 {@code NO_DATA}），先 {@code start} 再由 TaskManager {@code open}。
 * <p>
 * 同步策略：{@code start}/{@code stop} 改 {@code running} / 线程引用；等待用
 * {@code waitLock}。{@code stop} 置门闩并抬 {@code loopEpoch}，不 wake、不 join；
 * {@link #wake()} 在 {@code stop} 之后直接 return。比对线程见门闩后自行退出。
 * N 路 peek 只在本线程，路间不加锁。{@code lanes} 整数组替换（keeper 增删 /
 * 端口变）；方法内先存成本地变量。{@code replaceLanes} 与 {@code alignStart}
 * 同锁：未对齐时先刷新本地 {@code lanes} 再算 S0，避免旧数组把
 * {@code compareOffsetAligned} 写成 true。锁内只写门闩 / 计数 / S0；
 * reporter 与 {@code disconnect}/{@code reconnect} 出锁后做，且只重置
 * 仍在当前 {@code lanes} 里的路。lane 由 {@code ShardCompareTaskManager}
 * 开闭；{@code stop} 只丢引用，不 {@code close}。
 * {@code wake} 绑死本实例：旧 lane 迟到的 notify 打在已 stop 的旧对象上。
 * {@code start()} 线程未起来则回滚 {@code running}，允许重试。
 */
public final class ShardComparator {

    public enum CompareOnceResult {
        ADVANCED,
        MISMATCH,
        NO_DATA,
        REALIGNED,
        REPL_ID_RESET
    }

    private static final CompareLane[] EMPTY_LANES = new CompareLane[0];

    private static final Logger logger = LoggerFactory.getLogger(ShardComparator.class);

    private final String cluster;

    private final String shard;

    private volatile CompareLane[] lanes;

    private final CompareReporter reporter;

    private final int chunkBytes;

    private final ThreadFactory threadFactory;

    private byte[][] peekBuf;

    private volatile boolean compareOffsetAligned;

    private boolean released;

    private long comparedEnd;

    private long comparedBytes;

    private long mismatchCount;

    private long compareLostCount;

    private long realignCount;

    private long replIdMismatchCount;

    private final Object waitLock = new Object();

    private volatile boolean running;

    private volatile int loopEpoch;

    private Thread compareThread;

    public ShardComparator(String cluster, String shard, List<CompareLane> lanes,
            ComparatorConfig config, CompareReporter reporter) {
        this(cluster, shard, lanes, config, reporter,
                XpipeThreadFactory.create(threadName(cluster, shard), true));
    }

    ShardComparator(String cluster, String shard, List<CompareLane> lanes,
            ComparatorConfig config, CompareReporter reporter, ThreadFactory threadFactory) {
        this.cluster = Objects.requireNonNull(cluster, "cluster");
        this.shard = Objects.requireNonNull(shard, "shard");
        this.lanes = copyLanes(lanes, 0);
        this.reporter = Objects.requireNonNull(reporter, "reporter");
        this.chunkBytes = Objects.requireNonNull(config, "config").getCompareChunkBytes();
        if (this.chunkBytes <= 0) {
            throw new IllegalArgumentException("compare.chunk.bytes must be positive: " + chunkBytes);
        }
        this.threadFactory = Objects.requireNonNull(threadFactory, "threadFactory");
        this.peekBuf = new byte[this.lanes.length][this.chunkBytes];
    }

    /**
     * 起本分片比对线程。已在跑则幂等返回。{@code newThread}/{@code Thread.start}
     * 失败则回滚 {@code running}，允许再次 {@code start}。
     */
    public synchronized void start() {
        if (released) {
            throw new IllegalStateException("stopped, create a new ShardComparator");
        }
        if (running) {
            return;
        }
        int epoch = loopEpoch + 1;
        Thread t;
        try {
            t = threadFactory.newThread(() -> runLoop(epoch));
        } catch (Throwable e) {
            logger.error("[start] cluster={} shard={}", cluster, shard, e);
            rethrowStartFailure(e);
            return;
        }
        running = true;
        loopEpoch = epoch;
        compareThread = t;
        try {
            t.start();
        } catch (Throwable e) {
            running = false;
            compareThread = null;
            logger.error("[start] cluster={} shard={}", cluster, shard, e);
            rethrowStartFailure(e);
        }
        logger.info("[start] cluster={} shard={} thread={}", cluster, shard, t.getName());
    }

    /**
     * {@code running=false}，丢掉 {@code lanes} 引用。不 {@code close} lane
     *（谁 open 谁 close，归 TaskManager）。不 wake、不 join、不 interrupt：
     * 比对线程在 {@code wait} 超时或本轮 {@code compareOnce} 结束后见门闩退出。
     * 本实例不可再 {@code start}。
     */
    public void stop() {
        synchronized (this) {
            running = false;
            released = true;
            loopEpoch++;
            compareThread = null;
            compareOffsetAligned = false;
            this.lanes = EMPTY_LANES;
        }
    }

    /**
     * 叫醒比对线程。只 {@code waitLock.notify()}。{@code stop()} 之后再调直接
     * return，不抛。旧实例上的迟到 notify 不会打到新 {@code ShardComparator}。
     */
    public void wake() {
        if (!running) {
            return;
        }
        synchronized (waitLock) {
            waitLock.notify();
        }
    }

    /**
     * 整数组替换当前绑定的路。先写 {@code compareOffsetAligned=false} 再发布新数组。
     * 不关闭旧数组里的对象（离开的路由 TaskManager 先 {@link CompareLane#close()}）。
     */
    public void replaceLanes(List<CompareLane> next) {
        CompareLane[] copy = copyLanes(next, 2);
        synchronized (this) {
            if (released) {
                throw new IllegalStateException("stopped, create a new ShardComparator");
            }
            compareOffsetAligned = false;
            this.lanes = copy;
        }
    }

    public boolean isRunning() {
        return running;
    }

    @VisibleForTesting
    public Thread getCompareThread() {
        return compareThread;
    }

    private static String threadName(String cluster, String shard) {
        return ComparatorConstants.COMPARE_THREAD_NAME_PREFIX + cluster + "-" + shard;
    }

    private void runLoop(int epoch) {
        while (running && loopEpoch == epoch) {
            try {
                CompareOnceResult result = compareOnce();
                if (result == CompareOnceResult.NO_DATA || result == CompareOnceResult.REPL_ID_RESET) {
                    awaitData();
                }
            } catch (Throwable t) {
                logger.error("[runLoop] cluster={} shard={}", cluster, shard, t);
                awaitData();
            }
        }
    }

    private void awaitData() {
        synchronized (waitLock) {
            if (!running) {
                return;
            }
            try {
                waitLock.wait(ComparatorConstants.COMPARE_WAIT_MILLI);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.info("[awaitData] interrupted cluster={} shard={}", cluster, shard);
            }
        }
    }

    /**
     * 起点 {@code S0 = max(各路 continue offset)}；replId 不一致则全断重建
     *（{@code disconnect + reconnect}，新流 CONTINUE 时换新 RingBuffer），不计失配。
     * 锁内只写门闩 / 计数 / S0；回调与断连出锁后做。
     */
    public boolean alignStart() {
        AlignSideEffect side;
        synchronized (this) {
            side = decideAlignStart(this.lanes);
        }
        applyReplIdReset(side);
        return side.aligned;
    }

    /**
     * 必须持有 {@code this}。只改本地状态，不做 IO / 回调。
     * 各路 {@code getReplId}/{@code getContinueOffset} 只读一次快照：
     * {@code replId} 由 IO 线程 {@code abandon} 置 null，两圈再读会 NPE
     * 或把单路掉线打成全员 reset。
     */
    private AlignSideEffect decideAlignStart(CompareLane[] lanes) {
        if (lanes.length < 2) {
            compareOffsetAligned = false;
            return AlignSideEffect.NOT_READY;
        }
        List<String> replIds = new ArrayList<>(lanes.length);
        long s0 = Long.MIN_VALUE;
        for (CompareLane lane : lanes) {
            String id = lane.getReplId();
            if (id == null) {
                compareOffsetAligned = false;
                return AlignSideEffect.NOT_READY;
            }
            replIds.add(id);
            s0 = Math.max(s0, lane.getContinueOffset());
        }
        String first = replIds.get(0);
        for (int i = 1; i < replIds.size(); i++) {
            if (!first.equals(replIds.get(i))) {
                logger.warn("[replIdMismatch] cluster={}, shard={}, replIds={}", cluster, shard, replIds);
                replIdMismatchCount++;
                compareOffsetAligned = false;
                return AlignSideEffect.mismatch(lanes, replIds);
            }
        }
        comparedEnd = s0;
        compareOffsetAligned = true;
        return AlignSideEffect.ALIGNED;
    }

    private void applyReplIdReset(AlignSideEffect side) {
        if (!side.replIdMismatch) {
            return;
        }
        reporter.onReplIdMismatch(cluster, shard, side.replIds);
        CompareLane[] current = this.lanes;
        for (CompareLane lane : side.resetLanes) {
            if (!isBound(current, lane)) {
                continue;
            }
            lane.disconnect();
            lane.reconnect();
        }
    }

    private static boolean isBound(CompareLane[] lanes, CompareLane target) {
        for (CompareLane lane : lanes) {
            if (lane == target) {
                return true;
            }
        }
        return false;
    }

    public CompareOnceResult compareOnce() {
        CompareLane[] lanes = this.lanes;
        if (lanes.length < 2) {
            return CompareOnceResult.NO_DATA;
        }
        if (!compareOffsetAligned) {
            AlignSideEffect side = null;
            synchronized (this) {
                lanes = this.lanes;
                if (lanes.length < 2) {
                    return CompareOnceResult.NO_DATA;
                }
                if (!compareOffsetAligned) {
                    side = decideAlignStart(lanes);
                }
            }
            if (side != null && !side.aligned) {
                applyReplIdReset(side);
                return side.replIdMismatch
                        ? CompareOnceResult.REPL_ID_RESET : CompareOnceResult.NO_DATA;
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
        byte[][] peekBuf = peekBufFor(lanes.length);
        boolean overwritten = false;
        boolean notYet = false;
        for (int i = 0; i < lanes.length; i++) {
            PeekStatus status = lanes[i].getBuffer().peek(comparedEnd, n, peekBuf[i]);
            if (status == PeekStatus.OVERWRITTEN) {
                overwritten = true;
            } else if (status != PeekStatus.HIT) {
                notYet = true;
            }
        }
        if (overwritten) {
            compareLostCount++;
            long from = comparedEnd;
            realign(lanes);
            reporter.onCompareLost(cluster, shard, from, comparedEnd);
            return CompareOnceResult.REALIGNED;
        }
        if (notYet) {
            return CompareOnceResult.NO_DATA;
        }
        int firstDiff = firstDiff(n, lanes, peekBuf);
        if (firstDiff < 0) {
            comparedEnd += n;
            comparedBytes += n;
            return CompareOnceResult.ADVANCED;
        }
        mismatchCount++;
        reporter.onMismatch(buildMismatch(firstDiff, n, lanes, peekBuf));
        comparedEnd += n;
        comparedBytes += n;
        return CompareOnceResult.MISMATCH;
    }

    /**
     * {@code comparedEnd = max(comparedEnd, max(各路 bufferStart))}，立即恢复、不等待、不动缓冲区。
     */
    public void realign() {
        realign(this.lanes);
    }

    private void realign(CompareLane[] lanes) {
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

    public boolean isCompareOffsetAligned() { return compareOffsetAligned; }

    @VisibleForTesting
    public CompareLane[] getLanes() {
        CompareLane[] lanes = this.lanes;
        return Arrays.copyOf(lanes, lanes.length);
    }

    private byte[][] peekBufFor(int n) {
        if (peekBuf == null || peekBuf.length != n) {
            peekBuf = new byte[n][chunkBytes];
        }
        return peekBuf;
    }

    private int firstDiff(int n, CompareLane[] lanes, byte[][] peekBuf) {
        for (int i = 0; i < n; i++) {
            byte b = peekBuf[0][i];
            for (int lane = 1; lane < lanes.length; lane++) {
                if (peekBuf[lane][i] != b) {
                    return i;
                }
            }
        }
        return -1;
    }

    private MismatchReport buildMismatch(int firstDiff, int n, CompareLane[] lanes, byte[][] peekBuf) {
        Map<Byte, List<String>> groups = new LinkedHashMap<>();
        for (int i = 0; i < lanes.length; i++) {
            byte value = peekBuf[i][firstDiff];
            groups.computeIfAbsent(value, k -> new ArrayList<>()).add(lanes[i].getAddress());
        }
        int maxSize = 0;
        for (List<String> members : groups.values()) {
            maxSize = Math.max(maxSize, members.size());
        }
        List<LaneBytes> laneBytes = new ArrayList<>(lanes.length);
        for (int i = 0; i < lanes.length; i++) {
            byte value = peekBuf[i][firstDiff];
            boolean minority = groups.get(value).size() < maxSize;
            laneBytes.add(new LaneBytes(lanes[i].getAddress(), copyChunk(peekBuf[i], n), minority));
        }
        return new MismatchReport(cluster, shard, comparedEnd + firstDiff, comparedBytes,
                laneBytes, groups);
    }

    private static byte[] copyChunk(byte[] src, int n) {
        byte[] copy = new byte[n];
        System.arraycopy(src, 0, copy, 0, n);
        return copy;
    }

    private static CompareLane[] copyLanes(List<CompareLane> lanes, int minSize) {
        if (lanes == null) {
            if (minSize <= 0) {
                return EMPTY_LANES;
            }
            throw new IllegalArgumentException("need at least " + minSize + " lanes");
        }
        if (lanes.size() < minSize) {
            throw new IllegalArgumentException("need at least " + minSize + " lanes");
        }
        CompareLane[] copy = new CompareLane[lanes.size()];
        for (int i = 0; i < lanes.size(); i++) {
            copy[i] = Objects.requireNonNull(lanes.get(i), "lane");
        }
        return copy;
    }

    private static void rethrowStartFailure(Throwable e) {
        if (e instanceof RuntimeException) {
            throw (RuntimeException) e;
        }
        if (e instanceof Error) {
            throw (Error) e;
        }
        throw new IllegalStateException(e);
    }

    private static final class AlignSideEffect {
        private static final AlignSideEffect NOT_READY = new AlignSideEffect(false, false, null, null);
        private static final AlignSideEffect ALIGNED = new AlignSideEffect(true, false, null, null);

        private final boolean aligned;
        private final boolean replIdMismatch;
        private final CompareLane[] resetLanes;
        private final List<String> replIds;

        private AlignSideEffect(boolean aligned, boolean replIdMismatch,
                CompareLane[] resetLanes, List<String> replIds) {
            this.aligned = aligned;
            this.replIdMismatch = replIdMismatch;
            this.resetLanes = resetLanes;
            this.replIds = replIds;
        }

        private static AlignSideEffect mismatch(CompareLane[] lanes, List<String> replIds) {
            return new AlignSideEffect(false, true, lanes, replIds);
        }
    }
}
