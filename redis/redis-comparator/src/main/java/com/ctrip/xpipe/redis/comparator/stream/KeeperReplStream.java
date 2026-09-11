package com.ctrip.xpipe.redis.comparator.stream;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.command.DefaultCommandFuture;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.netty.commands.NettyClientFactory;
import com.ctrip.xpipe.pool.FixedObjectPool;
import com.ctrip.xpipe.redis.comparator.compare.CompareLane;
import com.ctrip.xpipe.redis.comparator.config.ComparatorConfig;
import com.ctrip.xpipe.redis.comparator.config.ComparatorConstants;
import com.ctrip.xpipe.redis.core.exception.RdbRejectedException;
import com.ctrip.xpipe.redis.core.protocal.PsyncObserver;
import com.ctrip.xpipe.redis.core.protocal.cmd.CmdTailGapAllowedSync;
import com.ctrip.xpipe.redis.core.protocal.cmd.CmdTailStreamListener;
import com.ctrip.xpipe.redis.core.protocal.cmd.ConfigGetCommand;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.store.RdbStore;
import com.ctrip.xpipe.utils.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 单路 Keeper 增量流（spec §4.8.1 / D3 / D31 / D33 ① / D35 ①③）。
 * <p>
 * 同步策略：每个 {@code KeeperReplStream} 自管一根复制连接（{@link FixedObjectPool}
 * 包住 {@link NettyClient}，对齐 {@code AbstractRedisMasterReplication}），不走共享
 * keyed pool。同一时刻最多一个 {@link SyncSession}；{@code reconnect()} 用新
 * session 顶掉旧的；退避定时器在已有 session 时不再 {@code connect()}。
 * IO 线程只拷贝 + 发布 + 唤醒（唤醒失败只 ERROR，不打断收包）；{@code EventMonitor}
 * 丢到 {@code scheduled}，不在 event loop 上打点。
 * <p>
 * {@code dataAvailable} 只允许唤醒本分片比对线程，禁止比对 / dump / 打点 / 等待。
 * {@code REPLCONF} / 指数退避上限留给 Phase SR（ACK 走本 session 的同一条连接）。
 */
public final class KeeperReplStream implements CompareLane {

    public static final String MONITOR_TYPE = "KeeperReplStream";

    public static final String PREPARE_WATCH_OFF = "prepareWatchOff";

    public static final String FULL_RESYNC = "fullResync";

    public static final String STREAM_FAIL = "streamFail";

    private static final Logger logger = LoggerFactory.getLogger(KeeperReplStream.class);

    private final Endpoint endpoint;

    private final ScheduledExecutorService scheduled;

    private final ComparatorConfig config;

    private final Runnable dataAvailable;

    private final String cluster;

    private final String shard;

    private final PrepareWatchProbe prepareWatchProbe;

    private final boolean executeSync;

    private final EventMonitor eventMonitor;

    private final AtomicBoolean running = new AtomicBoolean();

    private final Object sessionLock = new Object();

    private final Object reconnectLock = new Object();

    private volatile String replId;

    private volatile long continueOffset;

    private volatile StreamRingBuffer buffer;

    private volatile SyncSession session;

    private volatile boolean cacheValid;

    private volatile Boolean cachedPrepareWatch;

    private volatile int reconnectDelayMilli = ComparatorConstants.STREAM_RECONNECT_MIN_MILLI;

    private ScheduledFuture<?> reconnectFuture;

    public KeeperReplStream(Endpoint endpoint, ScheduledExecutorService scheduled,
            ComparatorConfig config, Runnable dataAvailable, String cluster, String shard) {
        this(endpoint, scheduled, config, dataAvailable, cluster, shard, null, true, EventMonitor.DEFAULT);
    }

    @VisibleForTesting
    KeeperReplStream(Endpoint endpoint, ScheduledExecutorService scheduled, ComparatorConfig config,
            Runnable dataAvailable, PrepareWatchProbe prepareWatchProbe, boolean executeSync,
            EventMonitor eventMonitor) {
        this(endpoint, scheduled, config, dataAvailable, "c", "s",
                prepareWatchProbe, executeSync, eventMonitor);
    }

    private KeeperReplStream(Endpoint endpoint, ScheduledExecutorService scheduled, ComparatorConfig config,
            Runnable dataAvailable, String cluster, String shard, PrepareWatchProbe prepareWatchProbe,
            boolean executeSync, EventMonitor eventMonitor) {
        this.endpoint = Objects.requireNonNull(endpoint, "endpoint");
        this.scheduled = Objects.requireNonNull(scheduled, "scheduled");
        this.config = Objects.requireNonNull(config, "config");
        this.dataAvailable = dataAvailable == null ? () -> { } : dataAvailable;
        this.cluster = cluster == null ? "" : cluster;
        this.shard = shard == null ? "" : shard;
        this.prepareWatchProbe = prepareWatchProbe;
        this.executeSync = executeSync;
        this.eventMonitor = eventMonitor == null ? EventMonitor.DEFAULT : eventMonitor;
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        beginAttempt(false);
    }

    public void stop() {
        running.set(false);
        disconnect();
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
        cancelReconnect();
        abandonSession();
    }

    @Override
    public void reconnect() {
        if (!running.get()) {
            return;
        }
        cancelReconnect();
        beginAttempt(true);
    }

    private void beginAttempt(boolean replace) {
        SyncSession next = new SyncSession();
        SyncSession displaced;
        synchronized (sessionLock) {
            if (!running.get()) {
                return;
            }
            if (!replace && session != null && !session.abandoned) {
                return;
            }
            displaced = abandonLocked();
            session = next;
        }
        releaseConnection(displaced);
        queryPrepareWatch(next).addListener(future -> onPrepareWatch(next, future));
    }

    private CommandFuture<Boolean> queryPrepareWatch(SyncSession owner) {
        if (cacheValid) {
            DefaultCommandFuture<Boolean> done = new DefaultCommandFuture<>();
            done.setSuccess(Boolean.TRUE.equals(cachedPrepareWatch));
            return done;
        }
        if (prepareWatchProbe != null) {
            return prepareWatchProbe.query();
        }
        try {
            if (!attachConnection(owner)) {
                DefaultCommandFuture<Boolean> done = new DefaultCommandFuture<>();
                done.setFailure(new IllegalStateException("disconnected"));
                return done;
            }
            return new ConfigGetCommand.ConfigGetPrepareWatch(owner.pool, scheduled).execute();
        } catch (Throwable t) {
            DefaultCommandFuture<Boolean> done = new DefaultCommandFuture<>();
            done.setFailure(t);
            return done;
        }
    }

    private boolean attachConnection(SyncSession owner) throws Exception {
        NettyClientFactory factory = new NettyClientFactory(endpoint, true);
        factory.start();
        NettyClient client;
        try {
            client = factory.makeObject().getObject();
        } catch (Exception e) {
            stopFactory(factory);
            throw e;
        }
        FixedObjectPool<NettyClient> pool = new FixedObjectPool<>(client);
        synchronized (sessionLock) {
            if (owner.abandoned || session != owner) {
                closeChannel(client);
                stopFactory(factory);
                return false;
            }
            owner.factory = factory;
            owner.pool = pool;
            return true;
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
            try {
                if (!attachConnection(owner)) {
                    return;
                }
            } catch (Throwable t) {
                logger.error("[openSync] connect {}", desc(), t);
                failAndRetry(owner, STREAM_FAIL);
                return;
            }
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
        return current;
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
        stopFactory(owner.factory);
        owner.pool = null;
        owner.factory = null;
    }

    private static void closeChannel(NettyClient client) {
        if (client != null && client.channel() != null) {
            client.channel().close();
        }
    }

    private void stopFactory(NettyClientFactory factory) {
        if (factory == null) {
            return;
        }
        try {
            factory.stop();
        } catch (Exception e) {
            logger.warn("[stopFactory] {}", desc(), e);
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
        if (!running.get()) {
            return;
        }
        synchronized (reconnectLock) {
            if (!running.get()) {
                return;
            }
            if (reconnectFuture != null && !reconnectFuture.isDone()) {
                return;
            }
            reconnectFuture = scheduled.schedule(() -> {
                synchronized (reconnectLock) {
                    reconnectFuture = null;
                }
                try {
                    beginAttempt(false);
                } catch (Throwable t) {
                    logger.error("[reconnect] {}", desc(), t);
                }
            }, reconnectDelayMilli, TimeUnit.MILLISECONDS);
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
    CmdTailGapAllowedSync currentSync() {
        SyncSession current = session;
        return current == null ? null : current.sync;
    }

    @FunctionalInterface
    interface PrepareWatchProbe {
        CommandFuture<Boolean> query();
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

        private NettyClientFactory factory;

        private FixedObjectPool<NettyClient> pool;

        private volatile StreamRingBuffer writeBuffer;

        private volatile boolean abandoned;

        private volatile String publishedReplId;

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
            }
            wakeCompareThread();
        }
    }
}
