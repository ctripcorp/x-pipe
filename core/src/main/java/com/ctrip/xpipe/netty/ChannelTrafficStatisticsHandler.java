package com.ctrip.xpipe.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.FileRegion;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.ScheduledFuture;

import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author leoliang
 *
 *         2017年3月1日
 */
public abstract class ChannelTrafficStatisticsHandler extends AbstractNettyHandler {

    private final long reportIntervalMillis;

    private volatile ScheduledFuture<?> nextCheck;

    private String ip = "";
    private int port = -1;

    private AtomicLong writtenBytes = new AtomicLong(0L);
    private AtomicLong readBytes = new AtomicLong(0L);

    private volatile int state = 0; // 0 - none, 1 - initialized, 2 - destroyed

    public ChannelTrafficStatisticsHandler(long reportIntervalMillis) {
        if (reportIntervalMillis <= 0) {
            throw new IllegalArgumentException(String.format("Invalid reportIntervalMillis %s", reportIntervalMillis));
        }

        this.reportIntervalMillis = reportIntervalMillis;
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
        super.handlerAdded(ctx);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        destroy();
        super.handlerRemoved(ctx);
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

        doChannelRead(ctx, msg);
        super.channelRead(ctx, msg);
    }

    protected abstract void doChannelRead(ChannelHandlerContext ctx, Object msg) throws Exception;

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof ByteBuf) {
            if (logger.isDebugEnabled()) logger.debug("[write]{}", ((ByteBuf) msg).toString(CharsetUtil.UTF_8));
            writtenBytes.addAndGet(((ByteBuf) msg).readableBytes());
        } else if (msg instanceof FileRegion) {
            writtenBytes.addAndGet(((FileRegion) msg).count());
        }

        doWrite(ctx, msg, promise);
        super.write(ctx, msg, promise);
    }

    protected abstract void doWrite(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception;

    protected void initialize(ChannelHandlerContext ctx) {
        // Avoid the case where destroy() is called before scheduling timeouts.
        switch (state) {
            case 1:
            case 2:
                return;
        }

        state = 1;
        parseIpPort(ctx.channel());

        nextCheck = ctx.executor().schedule(new ReportingTask(ctx), reportIntervalMillis, TimeUnit.MILLISECONDS);
    }

    protected void destroy() {
        state = 2;

        if (nextCheck != null) {
            nextCheck.cancel(false);
            nextCheck = null;
        }
    }

    protected void reportTraffic() {
        doReportTraffic(readBytes.getAndSet(0), writtenBytes.getAndSet(0), ip, port);
    }

    protected abstract void doReportTraffic(long readBytes, long writtenBytes, String remoteIp, int remotePort);

    protected void parseIpPort(final Channel channel) {
        if (null == channel) {
            return;
        }
        final SocketAddress remote = channel.remoteAddress();
        final String addr = remote != null ? remote.toString() : "";

        if (addr.length() > 0) {
            int index = addr.lastIndexOf("/");
            if (index >= 0) {
                String ipPort = addr.substring(index + 1);
                String[] splits = ipPort.split(":");
                if (splits != null && splits.length == 2) {
                    ip = splits[0];
                    try {
                        port = Integer.parseInt(splits[1]);
                    } catch (NumberFormatException e) {
                        // ignore
                    }
                }
            }

        }
    }

    protected final class ReportingTask implements Runnable {
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
                reportTraffic();
            } catch (Throwable t) {
                ctx.fireExceptionCaught(t);
            } finally {
                nextCheck = ctx.executor().schedule(this, reportIntervalMillis, TimeUnit.MILLISECONDS);
            }
        }
    }

    protected long getWrittenBytes() {
        return writtenBytes.get();
    }

    protected long getReadBytes() {
        return readBytes.get();
    }
}
