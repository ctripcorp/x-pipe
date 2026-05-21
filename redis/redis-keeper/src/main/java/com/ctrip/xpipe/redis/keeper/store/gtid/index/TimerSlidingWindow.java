package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import com.ctrip.xpipe.redis.core.store.CommandWriter;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.monitor.CommandStoreDelay;
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

    // ---------- 聚合窗口 ----------
    private CompositeByteBuf window;
    private final NioEventLoopGroup eventLoopGroup;
    private final KeeperConfig keeperConfig;
    private final CommandWriter commandWriter;
    private final CommandStoreDelay commandStoreDelay;
    private final OffsetNotifier offsetNotifier;

    private volatile ScheduledFuture<?> delayFlushFuture;
    private long firstByteTime = -1;
    private volatile int currentWindowThreshold;

    public TimerSlidingWindow(KeeperConfig keeperConfig, CommandWriter commandWriter,
                              CommandStoreDelay commandStoreDelay, OffsetNotifier offsetNotifier,
                              NioEventLoopGroup eventLoopGroup) throws IOException {
        this.keeperConfig = keeperConfig;
        this.eventLoopGroup = eventLoopGroup;
        this.window = ByteBufAllocator.DEFAULT.compositeBuffer(1024);
        this.commandWriter = commandWriter;
        this.commandStoreDelay = commandStoreDelay;
        this.offsetNotifier = offsetNotifier;
        this.currentWindowThreshold = keeperConfig.getCmdBatchWriteSize();
    }


    public int write(ByteBuf data) throws IOException {
        int dataSize = data.readableBytes();
        long now = System.currentTimeMillis();
        int windowSize = window.readableBytes();
        boolean wasEmpty = windowSize == 0;
        data.retain();
        window.addComponent(true, data);
        if (wasEmpty) {
            firstByteTime = now;
            scheduleDelayFlush();   // 启动最大驻留时间定时器
        }

        currentWindowThreshold =  keeperConfig.getCmdBatchWriteSize();

        // 触发刷盘条件： 达到字节阈值
        windowSize += dataSize;
        if (windowSize >= currentWindowThreshold) {
            flushBuffer();
        }

        return dataSize;
    }

    /**
     * 手动刷新所有数据（通常在关闭或强制同步时调用）
     */
    public void flushAll() throws IOException {
        flushBuffer();
    }

    @Override
    public void close() throws IOException {
        flushAll();
    }

    // ==================== 内部实现 ====================

    /** 将聚合窗口中的数据批量写入磁盘 */
    private void flushBuffer() throws IOException {
        if (window.readableBytes() == 0) return;

        commandStoreDelay.beginWrite();
        commandWriter.write(window);
        long offset = commandWriter.totalLength() - 1;
        commandStoreDelay.endWrite(offset);
        offsetNotifier.offsetIncreased(offset);

        // 释放旧窗口并创建新窗口
        try {
            window.release();
            window = ByteBufAllocator.DEFAULT.compositeBuffer(1024);
        } catch (Throwable t) {
            logger.warn("[release] failed to release window buffer", t);
        }
        firstByteTime = 0;         // 窗口已空
        cancelDelayFlush();         // 刷盘后无需再触发延迟任务
    }

    private void scheduleDelayFlush() {
        cancelDelayFlush();

        long flushInterval = keeperConfig.getCmdBatchFlushIntervalMillis();
        long deadline = firstByteTime + flushInterval;
        long delay = deadline - System.currentTimeMillis();

        if (delay <= 0) {
            delayFlush();
        } else {
            delayFlushFuture = eventLoopGroup.schedule(this::delayFlush, delay, TimeUnit.MILLISECONDS);
        }
    }

    /** 取消尚未执行的延迟刷盘任务 */
    private void cancelDelayFlush() {
        ScheduledFuture<?> future = this.delayFlushFuture;
        if (future != null && !future.isDone()) {
            future.cancel(false);
        }
        this.delayFlushFuture = null;
    }

    /** 延迟任务回调：执行定时刷盘 */
    private void delayFlush() {
        try {
            flushBuffer();
        } catch (IOException e) {
            logger.error("[delayFlush] failed to flush buffer", e);
        } finally {
            delayFlushFuture = null;
        }
    }
}