package com.ctrip.xpipe.redis.comparator.stream;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.command.DefaultCommandFuture;
import com.ctrip.xpipe.netty.NettySimpleMessageHandler;
import com.ctrip.xpipe.netty.commands.DefaultNettyClient;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.netty.commands.NettyClientHandler;
import com.ctrip.xpipe.pool.FixedObjectPool;
import com.ctrip.xpipe.redis.comparator.compare.CompareLane;
import com.ctrip.xpipe.redis.comparator.config.ComparatorConfig;
import com.ctrip.xpipe.redis.comparator.config.ComparatorConstants;
import com.ctrip.xpipe.redis.core.exception.RdbRejectedException;
import com.ctrip.xpipe.redis.core.protocal.PsyncObserver;
import com.ctrip.xpipe.redis.core.protocal.cmd.CmdTailGapAllowedSync;
import com.ctrip.xpipe.redis.core.protocal.cmd.CmdTailStreamListener;
import com.ctrip.xpipe.redis.core.protocal.cmd.ConfigGetCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.Replconf;
import com.ctrip.xpipe.redis.core.protocal.cmd.Replconf.ReplConfType;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.store.RdbStore;
import com.ctrip.xpipe.utils.VisibleForTesting;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoop;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 单路 Keeper 增量流（spec §4.8.1 / D3 / D31 / D33 ① / D35 ①②④⑤⑥）。
 * <p>
 * 同步策略：每个 {@code KeeperReplStream} 自管一根复制连接（{@link Bootstrap}
 * 异步 connect + {@link FixedObjectPool} 包住 {@link NettyClient}），不走共享 keyed pool，
 * 也不在流内 {@code new NettyClientFactory} / {@code EventLoopGroup}。
 * 构造注入 {@link EventLoop}（同一分片 N 路同一条）；{@code EventLoopGroup} 由应用持有
 * 并管理生命周期，流不 shutdown group。建链超时用 {@code CONNECT_TIMEOUT_MILLIS}，
 * 失败关 channel，禁止在 {@code scheduled} 上 {@code ChannelFuture.get}。
 * 同一时刻最多一个 {@link SyncSession}；{@code reconnect()} 用新 session 顶掉旧的；
 * 退避定时器在已有 session 时不再 {@code connect()}。
 * {@code disconnect()} 关闭自动建流，已排队的退避回调不得再 {@code connect()}；
 * {@code reconnect()} / {@code start()} 重新打开。
 * IO 线程只拷贝 + 发布 + 唤醒（唤醒失败只 ERROR，不打断收包）；{@code EventMonitor}
 * 丢到 {@code scheduled}，不在 event loop 上打点。
 * <p>
 * {@code dataAvailable} 只允许唤醒本分片比对线程，禁止比对 / dump / 打点 / 等待。
 * 建流前发 {@code REPLCONF listening-port}；CONTINUE 后 ACK 定时器挂
 * {@code scheduled}，值取本路 {@code receivedEnd}，禁止 {@code comparedEnd}；不发 {@code capa}。
 */
public final class KeeperReplStream implements CompareLane {

    public static final String MONITOR_TYPE = "KeeperReplStream";

    public static final String PREPARE_WATCH_OFF = "prepareWatchOff";

    public static final String FULL_RESYNC = "fullResync";

    public static final String STREAM_FAIL = "streamFail";

    @VisibleForTesting
    static final int TEST_HTTP_PORT = 18080;

    private static final Logger logger = LoggerFactory.getLogger(KeeperReplStream.class);

    private static final LoggingHandler LOGGING_HANDLER = new LoggingHandler(LogLevel.DEBUG);

    private final Endpoint endpoint;

    /** 生产即 Spring {@code SCHEDULED_EXECUTOR}（D35 ②⑥：退避与 ACK 都挂这根）。 */
    private final ScheduledExecutorService scheduled;

    /** 同一分片 N 路共用；来自上层 {@code EventLoopGroup}，流不 shutdown group。 */
    private final EventLoop eventLoop;

    private final ComparatorConfig config;

    private final Runnable dataAvailable;

    private final String cluster;

    private final String shard;

    private final PrepareWatchProbe prepareWatchProbe;

    private final boolean executeSync;

    private final EventMonitor eventMonitor;

    private final int listeningPort;

    private final ReplconfProbe replconfProbe;

    private final AtomicBoolean running = new AtomicBoolean();

    private volatile boolean autoConnect;

    private final AtomicInteger streamReconnectCount = new AtomicInteger();

    private final Object sessionLock = new Object();

    private final Object reconnectLock = new Object();

    private volatile String replId;

    private volatile long continueOffset;

    private volatile StreamRingBuffer buffer;

    private volatile SyncSession session;

    private volatile boolean cacheValid;

    private volatile Boolean cachedPrepareWatch;

    private volatile int reconnectDelayMilli = ComparatorConstants.STREAM_RECONNECT_MIN_MILLI;

    private volatile int ackIntervalMilli = ComparatorConstants.REPLCONF_ACK_INTERVAL_MILLI;

    private volatile int connectTimeoutMilli = ComparatorConstants.STREAM_CONNECT_TIMEOUT_MILLI;

    private ScheduledFuture<?> reconnectFuture;

    public KeeperReplStream(Endpoint endpoint, ScheduledExecutorService scheduled,
            ComparatorConfig config, Runnable dataAvailable, String cluster, String shard, int listeningPort,
            EventLoop eventLoop) {
        this(endpoint, scheduled, config, dataAvailable, cluster, shard, listeningPort, eventLoop,
                null, true, EventMonitor.DEFAULT, null);
    }

    @VisibleForTesting
    KeeperReplStream(Endpoint endpoint, ScheduledExecutorService scheduled, ComparatorConfig config,
            Runnable dataAvailable, EventLoop eventLoop, PrepareWatchProbe prepareWatchProbe,
            boolean executeSync, EventMonitor eventMonitor) {
        this(endpoint, scheduled, config, dataAvailable, eventLoop, prepareWatchProbe, executeSync,
                eventMonitor, null);
    }

    @VisibleForTesting
    KeeperReplStream(Endpoint endpoint, ScheduledExecutorService scheduled, ComparatorConfig config,
            Runnable dataAvailable, EventLoop eventLoop, PrepareWatchProbe prepareWatchProbe,
            boolean executeSync, EventMonitor eventMonitor, ReplconfProbe replconfProbe) {
        this(endpoint, scheduled, config, dataAvailable, "c", "s", TEST_HTTP_PORT, eventLoop,
                prepareWatchProbe, executeSync, eventMonitor, replconfProbe);
    }

    private KeeperReplStream(Endpoint endpoint, ScheduledExecutorService scheduled, ComparatorConfig config,
            Runnable dataAvailable, String cluster, String shard, int listeningPort, EventLoop eventLoop,
            PrepareWatchProbe prepareWatchProbe, boolean executeSync, EventMonitor eventMonitor,
            ReplconfProbe replconfProbe) {
        this.endpoint = Objects.requireNonNull(endpoint, "endpoint");
        this.scheduled = Objects.requireNonNull(scheduled, "scheduled");
        this.eventLoop = Objects.requireNonNull(eventLoop, "eventLoop");
        this.config = Objects.requireNonNull(config, "config");
        this.dataAvailable = dataAvailable == null ? () -> { } : dataAvailable;
        this.cluster = cluster == null ? "" : cluster;
        this.shard = shard == null ? "" : shard;
        if (listeningPort <= 0) {
            throw new IllegalArgumentException("listeningPort must be positive: " + listeningPort);
        }
        this.listeningPort = listeningPort;
        this.prepareWatchProbe = prepareWatchProbe;
        this.executeSync = executeSync;
        this.eventMonitor = eventMonitor == null ? EventMonitor.DEFAULT : eventMonitor;
        this.replconfProbe = replconfProbe;
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        autoConnect = true;
        beginAttempt(false);
    }

    public void stop() {
        running.set(false);
        autoConnect = false;
        disconnect();
    }

    public int getStreamReconnectCount() {
        return streamReconnectCount.get();
    }

    public void invalidatePrepareWatchCache() {
        cacheValid = false;
    }

    @Override
    public String getAddress() {
        return endpoint.getHost() + ":" + endpoint.getPort();
    }

    @Override
    public String getReplId() {
        return replId;
    }

    @Override
    public long getContinueOffset() {
        return continueOffset;
    }

    @Override
    public StreamRingBuffer getBuffer() {
        return buffer;
    }

    @Override
    public void disconnect() {
        autoConnect = false;
        cancelReconnect();
        abandonSession();
    }

    @Override
    public void reconnect() {
        if (!running.get()) {
            return;
        }
        autoConnect = true;
        cancelReconnect();
        reconnectDelayMilli = ComparatorConstants.STREAM_RECONNECT_MIN_MILLI;
        streamReconnectCount.incrementAndGet();
        beginAttempt(true);
    }

    private void beginAttempt(boolean replace) {
        if (!running.get() || !autoConnect) {
            return;
        }
        SyncSession next = new SyncSession();
        SyncSession displaced;
        synchronized (sessionLock) {
            if (!running.get() || !autoConnect) {
                return;
            }
            if (!replace && session != null && !session.abandoned) {
                return;
            }
            displaced = abandonLocked();
            session = next;
        }
        releaseConnection(displaced);
        startPrepareWatch(next);
    }

    private void startPrepareWatch(SyncSession owner) {
        if (cacheValid) {
            DefaultCommandFuture<Boolean> done = new DefaultCommandFuture<>();
            done.setSuccess(Boolean.TRUE.equals(cachedPrepareWatch));
            done.addListener(future -> onPrepareWatch(owner, future));
            return;
        }
        if (prepareWatchProbe != null) {
            prepareWatchProbe.query().addListener(future -> onPrepareWatch(owner, future));
            return;
        }
        attachConnection(owner).addListener(future -> {
            if (!stillCurrent(owner)) {
                return;
            }
            if (!future.isSuccess()) {
                logger.error("[connect] {}", desc(), future.cause());
                failAndRetry(owner, STREAM_FAIL);
                return;
            }
            try {
                new ConfigGetCommand.ConfigGetPrepareWatch(owner.pool, scheduled).execute()
                        .addListener(watch -> onPrepareWatch(owner, watch));
            } catch (Throwable t) {
                logger.error("[prepareWatch] {}", desc(), t);
                failAndRetry(owner, STREAM_FAIL);
            }
        });
    }

    private CommandFuture<Void> attachConnection(SyncSession owner) {
        DefaultCommandFuture<Void> done = new DefaultCommandFuture<>();
        if (owner.pool != null) {
            done.setSuccess(null);
            return done;
        }
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(eventLoop)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMilli)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(LOGGING_HANDLER);
                        pipeline.addLast(new NettySimpleMessageHandler());
                        pipeline.addLast(new NettyClientHandler());
                    }
                });
        ChannelFuture connectFuture = bootstrap.connect(endpoint.getHost(), endpoint.getPort());
        connectFuture.addListener((ChannelFuture future) -> {
            if (!stillCurrent(owner)) {
                future.channel().close();
                completeOnce(done, new IllegalStateException("disconnected"));
                return;
            }
            if (!future.isSuccess() || !future.channel().isActive()) {
                future.channel().close();
                completeOnce(done, future.cause() == null
                        ? new IllegalStateException("connect fail") : future.cause());
                return;
            }
            NettyClient client = new DefaultNettyClient(future.channel());
            future.channel().attr(NettyClientHandler.KEY_CLIENT).set(client);
            synchronized (sessionLock) {
                if (owner.abandoned || session != owner) {
                    future.channel().close();
                    completeOnce(done, new IllegalStateException("disconnected"));
                    return;
                }
                owner.pool = new FixedObjectPool<>(client);
            }
            if (!done.isDone()) {
                done.setSuccess(null);
            }
        });
        return done;
    }

    private static void completeOnce(DefaultCommandFuture<Void> future, Throwable cause) {
        if (!future.isDone()) {
            future.setFailure(cause);
        }
    }

    private void onPrepareWatch(SyncSession owner, CommandFuture<Boolean> future) {
        if (!stillCurrent(owner)) {
            return;
        }
        try {
            if (!future.isSuccess()) {
                logger.error("[prepareWatch] query fail, {}", desc(), future.cause());
                failAndRetry(owner, STREAM_FAIL);
                return;
            }
            Boolean enabled = future.getNow();
            if (stillCurrent(owner)) {
                cachedPrepareWatch = enabled;
                cacheValid = true;
            }
            if (!Boolean.TRUE.equals(enabled)) {
                logger.info("[prepareWatchOff] {}, skip stream", desc());
                failAndRetry(owner, PREPARE_WATCH_OFF);
                return;
            }
            openSync(owner);
        } catch (Throwable t) {
            logger.error("[prepareWatch] {}", desc(), t);
            failAndRetry(owner, STREAM_FAIL);
        }
    }

    private void openSync(SyncSession owner) {
        if (!stillCurrent(owner)) {
            return;
        }
        if (executeSync && owner.pool == null) {
            attachConnection(owner).addListener(future -> {
                if (!stillCurrent(owner)) {
                    return;
                }
                if (!future.isSuccess()) {
                    logger.error("[openSync] connect {}", desc(), future.cause());
                    failAndRetry(owner, STREAM_FAIL);
                    return;
                }
                sendListeningPort(owner).addListener(port -> onListeningPort(owner, port));
            });
            return;
        }
        sendListeningPort(owner).addListener(future -> onListeningPort(owner, future));
    }

    private CommandFuture<Object> sendListeningPort(SyncSession owner) {
        if (replconfProbe != null) {
            return replconfProbe.listeningPort(listeningPort);
        }
        if (owner.pool == null) {
            DefaultCommandFuture<Object> done = new DefaultCommandFuture<>();
            done.setSuccess(null);
            return done;
        }
        return new Replconf(owner.pool, ReplConfType.LISTENING_PORT, scheduled,
                String.valueOf(listeningPort)).execute();
    }

    private void onListeningPort(SyncSession owner, CommandFuture<Object> future) {
        if (!stillCurrent(owner)) {
            return;
        }
        if (!future.isSuccess()) {
            logger.error("[listening-port] {} port={}", desc(), listeningPort, future.cause());
            failAndRetry(owner, STREAM_FAIL);
            return;
        }
        launchPsync(owner);
    }

    private void launchPsync(SyncSession owner) {
        if (!stillCurrent(owner)) {
            return;
        }
        CmdTailGapAllowedSync sync = new CmdTailGapAllowedSync(owner.pool, owner, scheduled);
        owner.sync = sync;
        sync.addPsyncObserver(owner);
        sync.future().addListener(f -> onSyncFinished(owner, f));
        boolean launch;
        synchronized (sessionLock) {
            if (!stillCurrent(owner)) {
                owner.abandoned = true;
                launch = false;
            } else {
                launch = executeSync;
            }
        }
        if (owner.abandoned) {
            closeSyncQuietly(sync);
            releaseConnection(owner);
            return;
        }
        if (launch) {
            sync.execute();
            if (owner.abandoned || !stillCurrent(owner)) {
                closeSyncQuietly(sync);
                releaseConnection(owner);
            }
        }
    }

    private void onSyncFinished(SyncSession owner, CommandFuture<?> future) {
        if (owner.abandoned || !running.get()) {
            return;
        }
        Throwable cause = future.cause();
        String name = isFullResync(cause) ? FULL_RESYNC : STREAM_FAIL;
        logger.error("[streamFail] {} event={}", desc(owner), name, cause);
        failAndRetry(owner, name);
    }

    private void failAndRetry(SyncSession owner, String eventName) {
        if (!finishAttempt(owner)) {
            return;
        }
        logEventQuietly(eventName);
        scheduleReconnect();
    }

    private boolean finishAttempt(SyncSession owner) {
        boolean mine;
        synchronized (sessionLock) {
            mine = running.get() && session == owner && !owner.abandoned;
            if (mine) {
                abandonLocked();
            }
        }
        if (mine) {
            releaseConnection(owner);
        }
        return mine;
    }

    private void abandonSession() {
        SyncSession displaced;
        synchronized (sessionLock) {
            displaced = abandonLocked();
        }
        releaseConnection(displaced);
    }

    private SyncSession abandonLocked() {
        replId = null;
        SyncSession current = session;
        session = null;
        if (current == null) {
            return null;
        }
        current.abandoned = true;
        cancelAckLocked(current);
        return current;
    }

    private void cancelAckLocked(SyncSession owner) {
        if (owner.ackFuture != null) {
            owner.ackFuture.cancel(false);
            owner.ackFuture = null;
        }
    }

    private void closeSyncQuietly(CmdTailGapAllowedSync sync) {
        if (sync == null) {
            return;
        }
        try {
            sync.close();
            if (!sync.future().isDone()) {
                sync.future().setFailure(new IllegalStateException("disconnected"));
            }
        } catch (Throwable t) {
            logger.warn("[closeSync] {}", desc(), t);
        }
    }

    private void releaseConnection(SyncSession owner) {
        if (owner == null) {
            return;
        }
        closeSyncQuietly(owner.sync);
        if (owner.pool != null) {
            closeChannel(owner.pool.getObject());
        }
        owner.pool = null;
    }

    private static void closeChannel(NettyClient client) {
        if (client != null && client.channel() != null) {
            client.channel().close();
        }
    }

    private void logEventQuietly(String eventName) {
        try {
            scheduled.execute(() -> {
                try {
                    eventMonitor.logEvent(MONITOR_TYPE, eventName);
                } catch (Throwable t) {
                    logger.warn("[logEvent] {} {}", eventName, desc(), t);
                }
            });
        } catch (Throwable t) {
            logger.warn("[logEvent] schedule fail {} {}", eventName, desc(), t);
        }
    }

    private void wakeCompareThread() {
        try {
            dataAvailable.run();
        } catch (Throwable t) {
            logger.error("[dataAvailable] {}", desc(), t);
        }
    }

    private void scheduleReconnect() {
        if (!running.get() || !autoConnect) {
            return;
        }
        synchronized (reconnectLock) {
            if (!running.get() || !autoConnect) {
                return;
            }
            if (reconnectFuture != null && !reconnectFuture.isDone()) {
                return;
            }
            streamReconnectCount.incrementAndGet();
            int delay = reconnectDelayMilli;
            bumpReconnectDelay();
            reconnectFuture = scheduled.schedule(() -> {
                synchronized (reconnectLock) {
                    reconnectFuture = null;
                }
                try {
                    beginAttempt(false);
                } catch (Throwable t) {
                    logger.error("[reconnect] {}", desc(), t);
                }
            }, delay, TimeUnit.MILLISECONDS);
        }
    }

    private void bumpReconnectDelay() {
        long next = (long) reconnectDelayMilli * 2;
        reconnectDelayMilli = next >= ComparatorConstants.STREAM_RECONNECT_MAX_MILLI
                ? ComparatorConstants.STREAM_RECONNECT_MAX_MILLI
                : (int) next;
    }

    private void startAckTimer(SyncSession owner) {
        synchronized (sessionLock) {
            if (!stillCurrent(owner)) {
                return;
            }
            cancelAckLocked(owner);
            owner.ackFuture = scheduled.scheduleWithFixedDelay(() -> {
                try {
                    if (!stillCurrent(owner)) {
                        return;
                    }
                    sendAck(owner);
                } catch (Throwable t) {
                    logger.warn("[ack] {}", desc(owner), t);
                }
            }, 0, ackIntervalMilli, TimeUnit.MILLISECONDS);
        }
    }

    private void sendAck(SyncSession owner) {
        StreamRingBuffer buf = owner.writeBuffer;
        if (buf == null) {
            return;
        }
        long receivedEnd = buf.getReceivedEnd();
        if (replconfProbe != null) {
            replconfProbe.ack(receivedEnd);
            return;
        }
        if (owner.pool == null) {
            return;
        }
        try {
            CommandFuture<Object> future = new Replconf(owner.pool, ReplConfType.ACK, scheduled,
                    String.valueOf(receivedEnd)).execute();
            future.addListener(f -> {
                if (!f.isSuccess()) {
                    logger.warn("[ack] send fail {}", desc(owner), f.cause());
                }
            });
        } catch (Throwable t) {
            logger.warn("[ack] send fail {}", desc(owner), t);
        }
    }

    private void cancelReconnect() {
        synchronized (reconnectLock) {
            if (reconnectFuture != null) {
                reconnectFuture.cancel(false);
                reconnectFuture = null;
            }
        }
    }

    private boolean stillCurrent(SyncSession owner) {
        return running.get() && owner != null && !owner.abandoned && session == owner;
    }

    private static boolean isFullResync(Throwable cause) {
        while (cause != null) {
            if (cause instanceof RdbRejectedException) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    private String desc() {
        return cluster + "," + shard + "," + getAddress();
    }

    private String desc(SyncSession owner) {
        String id = owner == null ? replId : owner.publishedReplId;
        if (id == null) {
            id = replId;
        }
        return desc() + ",replId=" + id;
    }

    @VisibleForTesting
    void setReconnectDelayMilli(int delayMilli) {
        this.reconnectDelayMilli = delayMilli;
    }

    @VisibleForTesting
    void setAckIntervalMilli(int intervalMilli) {
        this.ackIntervalMilli = intervalMilli;
    }

    @VisibleForTesting
    void setConnectTimeoutMilli(int timeoutMilli) {
        this.connectTimeoutMilli = timeoutMilli;
    }

    @VisibleForTesting
    int reconnectDelayMilli() {
        return reconnectDelayMilli;
    }

    @VisibleForTesting
    CmdTailGapAllowedSync currentSync() {
        SyncSession current = session;
        return current == null ? null : current.sync;
    }

    @FunctionalInterface
    interface PrepareWatchProbe {
        CommandFuture<Boolean> query();
    }

    interface ReplconfProbe {
        CommandFuture<Object> listeningPort(int port);

        default void ack(long receivedEnd) {
        }
    }

    private interface StreamObserver extends CmdTailStreamListener, PsyncObserver {
        @Override default void onFullSync(long masterRdbOffset) { }
        @Override default void reFullSync() { }
        @Override default void beginWriteRdb(EofType eofType, String replId, long masterRdbOffset) { }
        @Override default void readAuxEnd(RdbStore rdbStore, Map<String, String> auxMap) { }
        @Override default void endWriteRdb() { }
        @Override default void onContinue(String requestReplId, String responseReplId) { }
    }

    private final class SyncSession implements StreamObserver {

        private CmdTailGapAllowedSync sync;

        private FixedObjectPool<NettyClient> pool;

        private volatile StreamRingBuffer writeBuffer;

        private volatile boolean abandoned;

        private volatile String publishedReplId;

        private ScheduledFuture<?> ackFuture;

        @Override
        public void onCommands(ByteBuf buf) {
            StreamRingBuffer target = writeBuffer;
            if (target == null) {
                return;
            }
            target.write(buf);
            if (!abandoned) {
                wakeCompareThread();
            }
        }

        @Override
        public void onKeeperContinue(String continueReplId, long beginOffset) {
            if (abandoned) {
                return;
            }
            StreamRingBuffer next = new StreamRingBuffer(config, beginOffset);
            writeBuffer = next;
            synchronized (sessionLock) {
                if (abandoned || session != this) {
                    return;
                }
                buffer = next;
                continueOffset = beginOffset;
                replId = continueReplId;
                publishedReplId = continueReplId;
                reconnectDelayMilli = ComparatorConstants.STREAM_RECONNECT_MIN_MILLI;
            }
            startAckTimer(this);
            wakeCompareThread();
        }
    }
}
