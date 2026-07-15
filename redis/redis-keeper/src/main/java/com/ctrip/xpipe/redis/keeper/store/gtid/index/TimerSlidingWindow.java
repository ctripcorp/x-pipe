package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import com.ctrip.xpipe.redis.core.store.CommandWriter;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.monitor.CommandStoreDelay;
import com.ctrip.xpipe.redis.keeper.store.cmd.OffsetNotifyingCommandWriter;
import com.ctrip.xpipe.utils.OffsetNotifier;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.nio.NioEventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import java.io.IOException;

/**
 * @author TB
 * @date 2026/4/25 10:50
 */

public class TimerSlidingWindow implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(TimerSlidingWindow.class);

    private CompositeByteBuf window;

    private final NioEventLoopGroup eventLoopGroup;
    private final KeeperConfig keeperConfig;
    private final CommandWriter commandWriter;
    private final CommandStoreDelay commandStoreDelay;
    private final OffsetNotifier offsetNotifier;

    private static final double TAU_MILLIS = 1000.0;              // EWMA 时间常数（1 秒），决定平滑程度

    // ---------- 速率估计器（EWMA） ----------
    private final RateEstimator rateEstimator = new RateEstimator(TAU_MILLIS);

    private volatile ScheduledFuture<?> delayFlushFuture;
    private volatile long scheduledDeadlineNano = -1;

    /** 串行化 window 聚合/刷盘，避免 delayFlush 与 backlogEndOffset 等路径并发消费同一 CompositeByteBuf */
    private final Object windowLock = new Object();


    public TimerSlidingWindow(KeeperConfig keeperConfig, CommandWriter commandWriter,
                              CommandStoreDelay commandStoreDelay, OffsetNotifier offsetNotifier,
                              NioEventLoopGroup eventLoopGroup) throws IOException {
        this.keeperConfig = keeperConfig;
        this.eventLoopGroup = eventLoopGroup;
        this.window = ByteBufAllocator.DEFAULT.compositeBuffer(1024);
        this.commandWriter = commandWriter;
        this.commandStoreDelay = commandStoreDelay;
        this.offsetNotifier = offsetNotifier;
    }

    public int write(ByteBuf data) throws IOException {
        return write(data,false);
    }

    public int write(ByteBuf data,boolean buildIndex) throws IOException {
        int dataSize = data.readableBytes();
        long now = System.nanoTime();

        // 1. 更新速率估计（EWMA，抗尖峰）
        rateEstimator.update(dataSize);
        double avgRate = rateEstimator.getRate();  // 单位：bytes/s
        if (avgRate <= keeperConfig.getCmdBatchLowRateBps()) {
            flushBuffer();
            flushSingleBuffer(data);
            return dataSize;
        }

        synchronized (windowLock) {
            int windowSize = window.readableBytes();
            boolean wasEmpty = windowSize == 0;

            appendToWindow(data, buildIndex, dataSize);
            windowSize += dataSize;

            if (shouldFlushWindow(wasEmpty, now, windowSize)) {
                flushBuffer();
                return windowSize;
            }

            scheduleDelayFlushIfAbsent();
        }
        return dataSize;
    }

    /** 手动刷新所有数据 */
    public void flushAll() throws IOException {
        flushBuffer();
    }

    /** 未刷盘字节快照；调用方不得假定与后续操作原子一致 */
    public int bufferSize(){
        return window.readableBytes();
    }

    @Override
    public void close() throws IOException {
    }

    // ==================== 内部实现 ====================

    private void appendToWindow(ByteBuf data, boolean buildIndex, int dataSize) {
        if (buildIndex) {
            data.retain();
            window.addComponent(true, data);
        } else {
            ByteBuf slice = data.retainedSlice();
            window.addComponent(true, slice);
            data.skipBytes(dataSize);
        }
    }

    private boolean shouldFlushWindow(boolean wasEmpty, long now, int windowSize) {
        if (windowSize >= keeperConfig.getCmdBatchWriteSize()) {
            return true;
        }
        // 窗口内后续写入：若已超过首次 schedule 的 deadline，立即刷盘
        return !wasEmpty && scheduledDeadlineNano > 0 && now >= scheduledDeadlineNano;
    }

    /** 直接落盘单条数据，不经过聚合窗口 */
    private void flushSingleBuffer(ByteBuf data) throws IOException {
        commandStoreDelay.beginWrite();
        commandWriter.write(data);
        long offset = commandWriter.totalLength() - 1;
        commandStoreDelay.endWrite(offset);
        if (!(commandWriter instanceof OffsetNotifyingCommandWriter)) {
            offsetNotifier.offsetIncreased(offset);
        }
    }

    /** 将聚合窗口中的数据批量写入磁盘（唯一刷盘入口） */
    private void flushBuffer() throws IOException {
        synchronized (windowLock) {
            int windowSize = window.readableBytes();
            if (windowSize == 0) {
                cancelDelayFlush();
                return;
            }

            commandStoreDelay.beginWrite();

            int wrote = commandWriter.write(window);
            if (wrote != windowSize) {
                logger.warn("[flushBuffer] window size {}.write size {}", windowSize, wrote);
            }
            long offset = commandWriter.totalLength() - 1;
            commandStoreDelay.endWrite(offset);
            if (!(commandWriter instanceof OffsetNotifyingCommandWriter)) {
                offsetNotifier.offsetIncreased(offset);
            }

            try {
                window.release();
            } catch (Throwable t) {
                logger.warn("[release] failed to release window buffer", t);
            } finally {
                window = ByteBufAllocator.DEFAULT.compositeBuffer(1024);
            }
            cancelDelayFlush();
        }
    }

    /** 窗口有数据且尚无 pending 定时刷盘时，安排一次兜底刷盘 */
    private void scheduleDelayFlushIfAbsent() {
        if (window.readableBytes() == 0) {
            return;
        }
        ScheduledFuture<?> future = delayFlushFuture;
        if (future != null && !future.isDone()) {
            return;
        }

        long flushIntervalMillis = keeperConfig.getCmdBatchFlushIntervalMillis();
        scheduledDeadlineNano = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(flushIntervalMillis);
        long delayMillis = Math.max(flushIntervalMillis, 1);
        delayFlushFuture = eventLoopGroup.schedule(this::delayFlush, delayMillis, TimeUnit.MILLISECONDS);
    }

    /** 取消尚未执行的定时刷盘任务 */
    private void cancelDelayFlush() {
        ScheduledFuture<?> future = this.delayFlushFuture;
        if (future != null && !future.isDone()) {
            future.cancel(false);
        }
        this.delayFlushFuture = null;
        this.scheduledDeadlineNano = -1;
    }

    /** 定时器回调：若数据仍存在且已超时，执行刷盘 */
    private void delayFlush() {
        try {
            flushBuffer();
        } catch (IOException e) {
            logger.error("[delayFlush] failed to flush buffer", e);
        }
    }

    // ==================== 速率估计器（Linux 风格 EWMA） ====================

    /**
     * 时间自适应 EWMA 速率估计器（修正首次采样偏差）。
     */
    private static class RateEstimator {
        private final double tauMillis;
        private double estimatedRate;               // 当前估计 (bytes/s)
        private long lastUpdateTime = -1;
        private long pendingBytes = 0;              // 累积未处理的字节（用于合并同一毫秒的写入）

        RateEstimator(double tauMillis) {
            this.tauMillis = tauMillis;
        }

        /**
         * 输入本次写入字节数，更新估计速率。
         * 首次调用只记录时间，不产生速率值；
         * 后续调用使用真实时间间隔计算瞬时速率，并平滑更新。
         */
        void update(long bytes) {
            long now = System.currentTimeMillis();

            if (lastUpdateTime < 0) {
                // 第一次：只记住时间和字节，不更新速率
                lastUpdateTime = now;
                pendingBytes = bytes;
                return;
            }

            // 累积同一毫秒内的多次写入
            long intervalMs = now - lastUpdateTime;
            if (intervalMs == 0) {
                pendingBytes += bytes;
                return;
            }

            // -------------------- 真实采样 --------------------
            // 将累积的字节和本次字节合并计算瞬时速率
            long totalBytes = pendingBytes + bytes;
            double instantRate = (totalBytes * 1000.0) / intervalMs;

            // 动态 alpha
            double alpha = 1.0 - Math.exp(-intervalMs / tauMillis);
            // 若是第二次采样（即尚没有有效 estimatedRate），则直接用瞬时速率
            if (estimatedRate == 0.0) {
                estimatedRate = instantRate;
            } else {
                estimatedRate = estimatedRate + alpha * (instantRate - estimatedRate);
            }

            // 重置状态
            pendingBytes = 0;
            lastUpdateTime = now;
        }

        /**
         * 获取当前估计速率 (bytes/s)。
         * 考虑空闲衰减：若长时间未调用 update()，速率自动向 0 衰减。
         */
        double getRate() {
            long now = System.currentTimeMillis();
            if (lastUpdateTime < 0) return 0;

            long idleMs = now - lastUpdateTime;
            if (idleMs > tauMillis) {
                double alpha = 1.0 - Math.exp(-idleMs / tauMillis);
                estimatedRate = estimatedRate * (1.0 - alpha);
                lastUpdateTime = now;      // 避免重复衰减
            }
            return estimatedRate;
        }
    }
}
