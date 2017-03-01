package com.ctrip.xpipe.netty;

import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.FileRegion;
import io.netty.util.concurrent.ScheduledFuture;

/**
 * @author leoliang
 *
 *         2017年3月1日
 */
public class ChannelTrafficStatisticsHandler extends ChannelDuplexHandler {

    private final long reportIntervalMillis;

    private volatile ScheduledFuture<?> nextCheck;

    private String remoteAddr = "";

    private AtomicLong writtenBytes = new AtomicLong(0L);
    private AtomicLong readBytes = new AtomicLong(0L);

    private volatile int state = 0; // 0 - none, 1 - initialized, 2 - destroyed

    public ChannelTrafficStatisticsHandler(long reportInterval, TimeUnit unit) {
        if (unit == null) {
            throw new NullPointerException("unit");
        }

        if (reportInterval <= 0) {
            throw new IllegalArgumentException(String.format("Invalid reportInterval %s", reportInterval));
        }

        this.reportIntervalMillis = unit.toMillis(reportInterval);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        if (ctx.channel().isActive() && ctx.channel().isRegistered()) {
            // channelActvie() event has been fired already, which means this.channelActive() will
            // not be invoked. We have to initialize here instead.
            initialize(ctx);
        } else {
            // channelActive() event has not been fired yet. this.channelActive() will be invoked
            // and initialization will occur there.
        }
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        destroy();
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        // Initialize early if channel is active already.
        if (ctx.channel().isActive()) {
            initialize(ctx);
        }
        super.channelRegistered(ctx);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        // This method will be invoked only if this handler was added
        // before channelActive() event is fired. If a user adds this handler
        // after the channelActive() event, initialize() will be called by beforeAdd().
        initialize(ctx);
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        destroy();
        super.channelInactive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof ByteBuf) {
            readBytes.addAndGet(((ByteBuf) msg).readableBytes());
        }
        ctx.fireChannelRead(msg);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof ByteBuf) {
            writtenBytes.addAndGet(((ByteBuf) msg).writableBytes());
        } else if (msg instanceof FileRegion) {
            writtenBytes.addAndGet(((FileRegion) msg).count());
        }

        ctx.write(msg, promise);
    }

    private void initialize(ChannelHandlerContext ctx) {
        // Avoid the case where destroy() is called before scheduling timeouts.
        switch (state) {
            case 1:
            case 2:
                return;
        }

        state = 1;
        remoteAddr = parseIpPort(ctx.channel());

        nextCheck = ctx.executor().schedule(new ReportingTask(ctx), reportIntervalMillis, TimeUnit.MILLISECONDS);
    }

    private void destroy() {
        state = 2;

        if (nextCheck != null) {
            nextCheck.cancel(false);
            nextCheck = null;
        }
    }

    protected void reportTraffic(ChannelHandlerContext ctx, long readBytes, long writtenBytes) throws Exception {
        ctx.fireUserEventTriggered(new TrafficReportingEvent(readBytes, writtenBytes, remoteAddr));
    }

    private String parseIpPort(final Channel channel) {
        if (null == channel) {
            return "";
        }
        final SocketAddress remote = channel.remoteAddress();
        final String addr = remote != null ? remote.toString() : "";

        if (addr.length() > 0) {
            int index = addr.lastIndexOf("/");
            if (index >= 0) {
                return addr.substring(index + 1);
            }

            return addr;
        }

        return "";
    }

    private final class ReportingTask implements Runnable {
        private final ChannelHandlerContext ctx;

        public ReportingTask(ChannelHandlerContext ctx) {
            this.ctx = ctx;
        }

        @Override
        public void run() {
            if (!ctx.channel().isOpen()) {
                return;
            }

            try {
                reportTraffic(ctx, readBytes.getAndSet(0L), writtenBytes.getAndSet(0L));
            } catch (Throwable t) {
                ctx.fireExceptionCaught(t);
            } finally {
                nextCheck = ctx.executor().schedule(this, reportIntervalMillis, TimeUnit.MILLISECONDS);
            }
        }
    }
}
