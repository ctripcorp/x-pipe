package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import com.ctrip.xpipe.redis.core.store.CommandWriter;
import com.ctrip.xpipe.redis.core.store.IndexStore;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.monitor.CommandStoreDelay;
import com.ctrip.xpipe.redis.keeper.store.AbstractCommandStore;
import com.ctrip.xpipe.utils.OffsetNotifier;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.nio.NioEventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author TB
 * @date 2026/4/25 10:50
 */
public class TimerSlidingWindow implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(TimerSlidingWindow.class);


    private CompositeByteBuf window;
    private KeeperConfig keeperConfig;    // 3 ms
    private final CommandWriter commandWriter;
    private final NioEventLoopGroup eventLoopGroup;
    private final CommandStoreDelay commandStoreDelay;
    private final OffsetNotifier offsetNotifier;

    private volatile ScheduledFuture<?> flushFuture;

    public TimerSlidingWindow(KeeperConfig keeperConfig, CommandWriter commandWriter,
                              CommandStoreDelay commandStoreDelay, OffsetNotifier offsetNotifier,
                              NioEventLoopGroup eventLoopGroup) throws IOException {
        this.keeperConfig = keeperConfig;
        this.window = ByteBufAllocator.DEFAULT.compositeBuffer(1024);
        this.commandWriter = commandWriter;
        this.commandStoreDelay = commandStoreDelay;
        this.offsetNotifier = offsetNotifier;
        this.eventLoopGroup = eventLoopGroup;
    }

    public int write(ByteBuf data) throws IOException {

        if (window.readableBytes() > this.keeperConfig.getCmdBatchWriteSize()) {
            cancelFlushFuture();
            flushBuffer();
        }

        data.retain();
        window.addComponent(true, data);

        if (window.readableBytes() > 0 && (flushFuture == null || flushFuture.isDone())) {
            flushFuture = eventLoopGroup.schedule(
                    this::scheduledFlush,
                    this.keeperConfig.getCmdBatchFlushIntervalMillis(), // 2ms
                    TimeUnit.MILLISECONDS
            );
        }

        return data.readableBytes();
    }


    private void scheduledFlush() {
        try {
            flushBuffer();
        } catch (IOException e) {
            logger.error("[scheduledFlush] flush buffer",e);
        }finally {
            flushFuture = null;
        }
    }

    private void cancelFlushFuture() {
        ScheduledFuture<?> future = this.flushFuture;
        if (future != null && !future.isDone()) {
            future.cancel(false);
            this.flushFuture = null;
        }
    }


    private void flushBuffer() throws IOException {
        if (window.readableBytes() == 0) return;
        commandStoreDelay.beginWrite();

        commandWriter.write(window);

        long offset = commandWriter.totalLength() - 1;
        commandStoreDelay.endWrite(offset);

        offsetNotifier.offsetIncreased(offset);

        try {
            window.release();
            window = ByteBufAllocator.DEFAULT.compositeBuffer(1024);
        } catch (Throwable t) {
            logger.warn("[release]", t);
        }
    }

    public void flushAll() throws IOException {
        cancelFlushFuture();
        flushBuffer();
    }

    @Override
    public  void close() throws IOException {
        flushAll();
    }
}

