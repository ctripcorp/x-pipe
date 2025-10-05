package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.framework.foundation.Foundation;
import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.command.CommandExecutionException;
import com.ctrip.xpipe.command.FailSafeCommandWrapper;
import com.ctrip.xpipe.command.SequenceCommandChain;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.exception.XpipeException;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.netty.NettySimpleMessageHandler;
import com.ctrip.xpipe.netty.commands.DefaultNettyClient;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.FixedObjectPool;
import com.ctrip.xpipe.redis.core.protocal.CAPA;
import com.ctrip.xpipe.redis.core.protocal.Psync;
import com.ctrip.xpipe.redis.core.protocal.cmd.*;
import com.ctrip.xpipe.redis.core.protocal.cmd.Replconf.ReplConfType;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbConstant;
import com.ctrip.xpipe.redis.core.store.RdbStore;
import com.ctrip.xpipe.redis.keeper.RdbDumper;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisMaster;
import com.ctrip.xpipe.redis.keeper.RedisMasterReplication;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.KeeperResourceManager;
import com.ctrip.xpipe.redis.keeper.exception.psync.PsyncCommandFailException;
import com.ctrip.xpipe.redis.keeper.exception.psync.PsyncMasterDisconnectedException;
import com.ctrip.xpipe.redis.keeper.netty.NettySlaveHandler;
import com.ctrip.xpipe.redis.keeper.ratelimit.LeakyBucketBasedMasterReplicationListener;
import com.ctrip.xpipe.utils.ChannelUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.ctrip.xpipe.redis.core.protocal.cmd.AbstractRedisCommand.PROXYED_REDIS_CONNECTION_COMMAND_TIME_OUT_MILLI;

/**
 * @author wenchao.meng
 * <p>
 * Aug 24, 2016
 */
public abstract class AbstractRedisMasterReplication extends AbstractLifecycle implements RedisMasterReplication {

    public static String KEY_MASTER_CONNECT_RETRY_DELAY_SECONDS = "KEY_MASTER_CONNECT_RETRY_DELAY_SECONDS";

    private static String REPL_CONF_CRDT_MARK = "1";

    public static int DEFAULT_REPLICATION_TIMEOUT_MILLI = 60000;

    protected int masterConnectRetryDelaySeconds = Integer.parseInt(System.getProperty(KEY_MASTER_CONNECT_RETRY_DELAY_SECONDS, "2"));

    private static LoggingHandler loggingHandler = new LoggingHandler(LogLevel.DEBUG);

    private final int replTimeoutMilli;

    private long repl_transfer_lastio;

    private AtomicReference<RdbDumper> rdbDumper = new AtomicReference<RdbDumper>(null);

    protected FixedObjectPool<NettyClient> clientPool;

    protected ScheduledExecutorService scheduled;

    public static final int REPLCONF_INTERVAL_MILLI = 1000;

    protected RedisMaster redisMaster;

    protected long connectedTime;

    protected volatile Channel masterChannel;

    private NioEventLoopGroup nioEventLoopGroup;

    protected RedisKeeperServer redisKeeperServer;

    protected AtomicReference<Command<?>> currentCommand = new AtomicReference<Command<?>>(null);

    private int commandTimeoutMilli;

    protected RedisMasterReplicationObserver replicationObserver;

    protected KeeperResourceManager resourceManager;

    private AtomicInteger runningReconnect = new AtomicInteger(0);

    public AbstractRedisMasterReplication(RedisKeeperServer redisKeeperServer, RedisMaster redisMaster, NioEventLoopGroup nioEventLoopGroup,
                                          ScheduledExecutorService scheduled, KeeperResourceManager resourceManager) {

        this.redisKeeperServer = redisKeeperServer;
        this.redisMaster = redisMaster;
        this.nioEventLoopGroup = nioEventLoopGroup;
        this.replTimeoutMilli = redisKeeperServer.getKeeperConfig() == null ? DEFAULT_REPLICATION_TIMEOUT_MILLI :
                redisKeeperServer.getKeeperConfig().getKeyReplicationTimeoutMilli();
        this.scheduled = scheduled;
        this.resourceManager = resourceManager;
        this.commandTimeoutMilli = initCommandTimeoutMilli();
    }

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        updateReplicationObserver(new LeakyBucketBasedMasterReplicationListener(this, redisKeeperServer, resourceManager, scheduled));
    }

    public RedisMaster getRedisMaster() {
        return redisMaster;
    }

    protected abstract String getSimpleName();

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        startReplication();
    }

    public void startReplication() {

        Channel localMasterChannel = this.masterChannel;
        if (localMasterChannel != null && localMasterChannel.isOpen()) {
            logger.warn("[startReplication][channel alive, don't do replication]{}", localMasterChannel);
            return;
        }
        logger.info("[startReplication]{}", redisMaster.masterEndPoint());
        connectWithMaster();

    }

    protected void connectWithMaster() {

        if (!(getLifecycleState().isStarting() || getLifecycleState().isStarted())) {
            logger.info("[connectWithMaster][do not connect, is stopped!!]{}", redisMaster.masterEndPoint());
            return;
        }

        Bootstrap b = new Bootstrap();
        b.group(nioEventLoopGroup).channel(NioSocketChannel.class).option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(loggingHandler);
                        p.addLast(new NettySimpleMessageHandler());
                        p.addLast(createHandler());
                    }
                });

        doConnect0(b);
    }

    protected int getMaxGap() {
        return redisKeeperServer.getKeeperConfig().getXsyncMaxGap();
    }

    private void doConnect0(Bootstrap b) {
        try {
            doConnect(b);
        } catch (Exception e) {
            logger.error("[doConnect0] ", e);
            scheduleReconnect(masterConnectRetryDelaySeconds * 1000);
        }
    }

    protected void scheduleReconnect(int timeMilli) {

        if (!(getLifecycleState().isStarting() || getLifecycleState().isStarted())) {
            logger.info("[scheduleReconnect][do not connect, is stopped!!]{}", redisMaster.masterEndPoint());
            return;
        }
        if (!runningReconnect.compareAndSet(0, 1)) {
            logger.info("[scheduleReconnect][multi reconnect, skip] {}", runningReconnect.get());
            return;
        }
        scheduled.schedule(new AbstractExceptionLogTask() {
            @Override
            public void doRun() {
                try {
                    runningReconnect.set(0);
                    connectWithMaster();
                } catch (Throwable th) {
                    logger.error("[scheduleReconnect]" + AbstractRedisMasterReplication.this, th);
                }
            }
        }, Math.max(10, timeMilli), TimeUnit.MILLISECONDS);
        // at least wait 10ms so that runningReconnect checking can avoid concurrent reconnecting
    }

    protected abstract void doConnect(Bootstrap b);

    protected ChannelFuture tryConnect(Bootstrap b) {
        Endpoint endpoint = redisMaster.masterEndPoint();
        logger.info("[tryConnect][begin]{}", endpoint);
        return b.connect(endpoint.getHost(), endpoint.getPort());
    }

    @Override
    public void reconnectMaster() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void handleResponse(Channel channel, ByteBuf byteBuf) throws XpipeException {

        if (!(getLifecycleState().isStarted() || getLifecycleState().isStarting())) {
            throw new RedisMasterReplicationStateException(this,
                    String.format("not stated: %s, do not receive message:%d, %s", getLifecycleState().getPhaseName(), byteBuf.readableBytes(), ByteBufUtils.readToString(byteBuf)));
        }

        onReceiveMessage(byteBuf.readableBytes());

        repl_transfer_lastio = System.currentTimeMillis();
        clientPool.getObject().handleResponse(channel, byteBuf);
    }

    protected void onReceiveMessage(int messageLength) {
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public void masterConnected(Channel channel) {
        logger.info("[masterConnected] {}", ChannelUtil.getDesc(channel));
        if (null != masterChannel) {
            logger.info("[masterConnected][unexpected connected channel][{}][{}] reset channels", ChannelUtil.getDesc(masterChannel), ChannelUtil.getDesc(channel));
            masterChannel.close();
            channel.close();
            return;
        }

        connectedTime = System.currentTimeMillis();
        this.masterChannel = channel;
        clientPool = new FixedObjectPool<NettyClient>(new DefaultNettyClient(channel));
        if (replicationObserver != null) {
            replicationObserver.onMasterConnected();
        }
        checkTimeout(channel);

        checkKeeper();

        SequenceCommandChain chain = new SequenceCommandChain(false);
        chain.add(listeningPortCommand());

        // for proxy connect init time
        Replconf capa;
        if (tryRordb()) {
            capa = new Replconf(clientPool, ReplConfType.CAPA, scheduled, commandTimeoutMilli,
                    CAPA.EOF.toString(), CAPA.PSYNC2.toString(), CAPA.RORDB.toString());
        } else {
            capa = new Replconf(clientPool, ReplConfType.CAPA, scheduled, commandTimeoutMilli,
                    CAPA.EOF.toString(), CAPA.PSYNC2.toString());
        }
        chain.add(new FailSafeCommandWrapper<>(capa));
        chain.add(new FailSafeCommandWrapper<>(keeperIdcCommand()));

        try {
            executeCommand(chain).addListener(new CommandFutureListener() {

                @Override
                public void operationComplete(CommandFuture commandFuture) throws Exception {
                    if (commandFuture.isSuccess()) {
                        sendReplicationCommand();
                    } else {
                        logger.error("[operationComplete][listeningPortCommand] close channel{} and wait for reconnect",
                                ChannelUtil.getDesc(channel), commandFuture.cause());
                        channel.close();
                    }
                }
            });
        } catch (Exception e) {
            logger.error("[masterConnected]" + channel, e);
        }
    }

    private void checkKeeper() {
        Replconf replconf = new Replconf(clientPool, ReplConfType.KEEPER, scheduled, commandTimeoutMilli);
        executeCommand(replconf).addListener(new CommandFutureListener<Object>() {

            @Override
            public void operationComplete(CommandFuture<Object> commandFuture) throws Exception {
                if (commandFuture.isSuccess()) {
                    redisMaster.setKeeper();
                }
            }
        });
    }

    private void checkTimeout(final Channel channel) {

        logger.info("[checkTimeout]{} ms, {}", replTimeoutMilli, ChannelUtil.getDesc(channel));
        final ScheduledFuture<?> repliTimeoutCheckFuture = scheduled.scheduleAtFixedRate(new AbstractExceptionLogTask() {

            @Override
            protected void doRun() throws Exception {

                long current = System.currentTimeMillis();
                if ((current - repl_transfer_lastio) >= replTimeoutMilli) {
                    logger.info("[doRun][no interaction with master for a long time, close connection]{}, {}", channel, AbstractRedisMasterReplication.this);
                    channel.close();
                }
            }
        }, replTimeoutMilli, Math.min(10000, replTimeoutMilli), TimeUnit.MILLISECONDS);

        channel.closeFuture().addListener(new ChannelFutureListener() {

            @Override
            public void operationComplete(ChannelFuture future) throws Exception {

                logger.info("[cancelTimeout]{}ms, {}", replTimeoutMilli, channel);
                repliTimeoutCheckFuture.cancel(true);
            }
        });
    }

    protected void resetMasterChannel(Channel channel) {
        if (channel.equals(this.masterChannel)) {
            logger.info("[masterDisconnected]{}", channel);
            this.masterChannel = null;
        } else {
            logger.info("[masterDisconnected][unexpected disconnected channel][{}][{}] ignore",
                    ChannelUtil.getDesc(masterChannel), ChannelUtil.getDesc(channel));
        }
    }

    @Override
    public void masterDisconnected(Channel channel) {

        resetMasterChannel(channel);
        dumpFail(new PsyncMasterDisconnectedException(channel));
    }

    @Override
    public boolean canSendPsync() {
        if (replicationObserver != null) {
            return replicationObserver.canSendPsync();
        }
        return true;
    }

    protected <V> CommandFuture<V> executeCommand(Command<V> command) {

        if (command != null) {
            currentCommand.set(command);
            return command.execute();
        }
        return null;
    }

    private Replconf keeperIdcCommand() {
        return new Replconf(clientPool, ReplConfType.IDC, scheduled, commandTimeoutMilli,
                FoundationService.DEFAULT.getDataCenter());
    }

    private Replconf listeningPortCommand() {

        Replconf replconf = new Replconf(clientPool, ReplConfType.LISTENING_PORT, scheduled, commandTimeoutMilli,
                String.valueOf(redisKeeperServer.getListeningPort()));
        return replconf;
    }

    protected void sendReplicationCommand() throws CommandExecutionException {
        if (canSendPsync()) {
            executeCommand(psyncCommand());
        } else {
            EventMonitor.DEFAULT.logAlertEvent("[lack-token]" + redisKeeperServer.getReplId());
            doWhenCannotPsync();
        }
    }

    protected abstract void doWhenCannotPsync();

    protected Psync psyncCommand() {

        if (getLifecycleState().isStopping() || getLifecycleState().isStopped()) {
            logger.info("[psyncCommand][stopped][before]{}", this);
            return null;
        }

        Psync psync = createPsync();
        psync.addPsyncObserver(replicationObserver);
        psync.future().addListener(new CommandFutureListener<Object>() {

            @Override
            public void operationComplete(CommandFuture<Object> commandFuture) throws Exception {

                if (!commandFuture.isSuccess()) {
                    logger.error("[operationComplete][psyncCommand][fail]" + AbstractRedisMasterReplication.this, commandFuture.cause());

                    dumpFail(new PsyncCommandFailException(commandFuture.cause()));
                    psyncFail(commandFuture.cause());
                }
            }
        });

        // double check lifecycle state
        // avoid replication stop and dispose before create psync
        if (getLifecycleState().isStopping() || getLifecycleState().isStopped()) {
            logger.info("[psyncCommand][stopped][after]{}", this);
            return null;
        } else return psync;
    }

    protected abstract Psync createPsync();

    protected abstract void psyncFail(Throwable cause);

    protected ChannelDuplexHandler createHandler() {

        KeeperConfig keeperConfig = redisKeeperServer.getKeeperConfig();
        return new NettySlaveHandler(this, redisKeeperServer, keeperConfig != null ? keeperConfig.getTrafficReportIntervalMillis() : KeeperConfig.DEFAULT_TRAFFIC_REPORT_INTERVAL_MILLIS);
    }

    @Override
    public void updateReplicationObserver(RedisMasterReplicationObserver observer) {
        this.replicationObserver = observer;
    }

    @Override
    protected void doStop() throws Exception {

        stopReplication();
        super.doStop();
    }

    protected void stopReplication() {

        logger.info("[stopReplication]{}", redisMaster.masterEndPoint());
        disconnectWithMaster();
    }

    protected void disconnectWithMaster() {
        Channel localMasterChannel = masterChannel;
        if (localMasterChannel != null && localMasterChannel.isOpen()) {
            logger.info("[disconnectWithMaster]{}", localMasterChannel);
            localMasterChannel.close();
        }
    }

    @Override
    public String toString() {

        return String.format("%s(M:%s, %s)", getClass().getSimpleName(), redisMaster, ChannelUtil.getDesc(masterChannel));
    }

    @Override
    public void onFullSync(long masterRdbOffset) {
        doOnFullSync(masterRdbOffset);
    }

    protected abstract void doOnFullSync(long masterRdbOffset);

    @Override
    public void reFullSync() {
        doReFullSync();
    }

    @Override
    public void readAuxEnd(RdbStore rdbStore, Map<String, String> auxMap) {
        String gtidExecuted = auxMap.get(RdbConstant.REDIS_RDB_AUX_KEY_GTID_EXECUTED);
        if (gtidExecuted != null) {
            logger.info("[readAuxEnd][gtid-executed] {}", gtidExecuted);
            rdbStore.updateRdbGtidSet(gtidExecuted);
        } else {
            String gtidSet = auxMap.getOrDefault(RdbConstant.REDIS_RDB_AUX_KEY_GTID, GtidSet.EMPTY_GTIDSET);
            logger.info("[readAuxEnd][gtid] {}", gtidSet);
            rdbStore.updateRdbGtidSet(gtidSet);
        }

        RdbStore.Type rdbType = auxMap.containsKey(RdbConstant.REDIS_RDB_AUX_KEY_RORDB) ? RdbStore.Type.RORDB : RdbStore.Type.NORMAL;
        logger.info("[readAuxEnd][rdb] {}", rdbType);
        rdbStore.updateRdbType(rdbType);
        doRdbTypeConfirm(rdbStore);

        if (null != rdbDumper.get()) {
            // rdbDumper may be reset by dumpFail
            rdbDumper.get().auxParseFinished(rdbType);
        }
    }

    protected void doRdbTypeConfirm(RdbStore rdbStore) {
        try {
            if (rdbStore.isGapAllowed()) {
                redisMaster.getCurrentReplicationStore().confirmRdbGapAllowed(rdbStore);
            } else {
                redisMaster.getCurrentReplicationStore().confirmRdb(rdbStore);
            }
        } catch (Throwable th) {
            dumpFail(th);
        }
    }

    protected abstract void doReFullSync();

    @Override
    public void beginWriteRdb(EofType eofType, String replId, long masterRdbOffset) throws IOException {

        doBeginWriteRdb(eofType, masterRdbOffset);
        rdbDumper.get().beginReceiveRdbData(replId, masterRdbOffset);
    }

    protected abstract void doBeginWriteRdb(EofType eofType, long masterRdbOffset) throws IOException;

    @Override
    public void endWriteRdb() {
        dumpFinished();
        doEndWriteRdb();
    }

    protected abstract void doEndWriteRdb();

    @Override
    public void onContinue(String requestReplId, String responseReplId) {
        doOnContinue();
    }

    @Override
    public void onKeeperContinue(String replId, long beginOffset) {
        doOnContinue();
    }

    protected abstract void doOnContinue();

    @Override
    public void onXFullSync(String replId, long replOff, String masterUuid, GtidSet gtidLost) {
        doOnXFullSync(replId, replOff, masterUuid, gtidLost);
    }

    protected abstract void doOnXFullSync(String replId, long replOff, String masterUuid, GtidSet gtidLost);

    @Override
    public void onXContinue(String replId, long replOff, String masterUuid, GtidSet gtidCont) {
        doOnXContinue(replId, replOff, masterUuid, gtidCont);
    }

    protected abstract void doOnXContinue(String replId, long replOff, String masterUuid, GtidSet gtidCont);

    @Override
    public void onSwitchToXsync(String replId, long replOff, String masterUuid, GtidSet gtidCont, GtidSet gtidLost) {
        doOnSwitchToXsync(replId, replOff, masterUuid, gtidCont, gtidLost);
    }

    protected abstract void doOnSwitchToXsync(String replId, long replOff, String masterUuid, GtidSet gtidCont, GtidSet gtidLost);

    @Override
    public void onSwitchToPsync(String replId, long replOff) {
        doOnSwitchToPsync(replId, replOff);
    }

    protected abstract void doOnSwitchToPsync(String replId, long replOff);

    @Override
    public void onUpdateXsync() {
        doOnUpdateXsync();
    }

    protected abstract void doOnUpdateXsync();

    protected void dumpFinished() {
        logger.info("[dumpFinished]{}", this);
        RdbDumper dumper = rdbDumper.get();
        if (dumper != null) {
            rdbDumper.set(null);
            dumper.dumpFinished();
        }
    }

    protected void dumpFail(Throwable th) {
        if (replicationObserver != null) {
            replicationObserver.onDumpFail(th);
        }
        RdbDumper dumper = rdbDumper.get();
        if (dumper != null) {
            rdbDumper.set(null);
            dumper.dumpFail(th);
        }
    }

    public void setRdbDumper(RdbDumper dumper) {

        if (this.rdbDumper.get() != null) {
            logger.info("[setRdbDumper][replace]{}", this.rdbDumper.get());
        }
        this.rdbDumper.set(dumper);
    }

    public RdbDumper getRdbDumper() {
        return rdbDumper.get();
    }

    @Override
    public RedisMaster redisMaster() {
        return redisMaster;
    }

    private int initCommandTimeoutMilli() {
        return PROXYED_REDIS_CONNECTION_COMMAND_TIME_OUT_MILLI;
    }

    @VisibleForTesting
    protected int commandTimeoutMilli() {
        return commandTimeoutMilli;
    }
}
