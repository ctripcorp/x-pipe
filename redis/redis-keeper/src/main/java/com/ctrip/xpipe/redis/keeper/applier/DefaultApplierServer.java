package com.ctrip.xpipe.redis.keeper.applier;

import com.ctrip.xpipe.api.cluster.LeaderElector;
import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.client.redis.AsyncRedisClient;
import com.ctrip.xpipe.client.redis.AsyncRedisClientFactory;
import com.ctrip.xpipe.cluster.ElectContext;
import com.ctrip.xpipe.concurrent.LongTimeAlertTask;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.netty.NettySimpleMessageHandler;
import com.ctrip.xpipe.netty.commands.NettyKeyedPoolClientFactory;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.ApplierInstanceMeta;
import com.ctrip.xpipe.redis.core.entity.ApplierMeta;
import com.ctrip.xpipe.redis.core.meta.MetaZkConfig;
import com.ctrip.xpipe.redis.core.protocal.ApplierSyncObserver;
import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import com.ctrip.xpipe.redis.core.redis.rdb.parser.DefaultRdbParser;
import com.ctrip.xpipe.redis.core.store.ClusterId;
import com.ctrip.xpipe.redis.core.store.ShardId;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.applier.sync.*;
import com.ctrip.xpipe.redis.keeper.applier.sequence.ApplierSequenceController;
import com.ctrip.xpipe.redis.keeper.applier.sequence.DefaultSequenceController;
import com.ctrip.xpipe.redis.keeper.applier.threshold.GTIDDistanceThreshold;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.handler.ApplierCommandHandlerManager;
import com.ctrip.xpipe.redis.keeper.impl.ApplierRedisClient;
import com.ctrip.xpipe.redis.keeper.netty.ApplierChannelHandlerFactory;
import com.ctrip.xpipe.redis.keeper.netty.NettyApplierHandler;
import com.ctrip.xpipe.utils.ClusterShardAwareThreadFactory;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Slight
 * <p>
 * Jun 01, 2022 08:16
 */
public class DefaultApplierServer extends AbstractInstanceNode implements ApplierServer, ApplierSyncObserver {

    /* component */

    @InstanceDependency
    public ApplierSequenceController sequenceController;

    /*@InstanceDependency
    public ApplierLwmManager lwmManager;*/

    @InstanceDependency
    public ApplierSyncReplication replication;

    @InstanceDependency
    public ApplierCommandDispatcher dispatcher;

    @InstanceDependency
    public AtomicLong offsetRecorder;

    @InstanceDependency
    public AtomicReference<GTIDDistanceThreshold> gtidDistanceThreshold;

    @InstanceDependency
    public InstanceComponentWrapper<LeaderElector> leaderElectorWrapper;

    /* cardinal info */

    @InstanceDependency
    public AsyncRedisClient client;

    @InstanceDependency
    public AtomicReference<String> replId;

    public final int listeningPort;

    public final ClusterId clusterId;

    public final ShardId shardId;

    public final ApplierMeta applierMeta;

    private volatile STATE state = STATE.NONE;

    /* utility */

    @InstanceDependency
    public InstanceComponentWrapper<XpipeNettyClientKeyedObjectPool> pool;

    @InstanceDependency
    public RedisOpParser parser;

    @InstanceDependency
    public ExecutorService stateThread;

    @InstanceDependency
    public ScheduledExecutorService workerThreads;

    @InstanceDependency
    public ScheduledExecutorService scheduled;

    @InstanceDependency
    public ExecutorService lwmThread;

    @InstanceDependency
    public AtomicReference<ApplierStatistic> applierStatisticRef;

    @InstanceDependency
    public AtomicReference<ApplierConfig> applierConfigRef;

    @InstanceDependency
    public AtomicReference<GtidSet> lostGtidSet;

    @InstanceDependency
    public AtomicReference<GtidSet> startGtidSet;

    @InstanceDependency
    public AtomicReference<GtidSet> execGtidSet;

    @InstanceDependency
    public RdbParser<?> rdbParser;

    @InstanceDependency
    public AtomicBoolean protoChanged;

    private long startTime;

    private final Map<Channel, RedisClient> redisClients = new ConcurrentHashMap<Channel, RedisClient>();

    private EventLoopGroup bossGroup;

    private EventLoopGroup workerGroup;

    private ServerSocketChannel serverSocketChannel;

    private ExecutorService clientExecutors;

    //TODO change value
    private static final int DEFAULT_KEYED_CLIENT_POOL_SIZE = 100;

    private static final int DEFAULT_NTEEY_BOSS_THREADS_SIZE = 1;

    private static final int DEFAULT_NTEEY_WORK_THREADS_SIZE = 2;

    private static final int DEFAULT_LONG_TIME_ALERT_TASK_MILLI = 1000;

    public DefaultApplierServer(String clusterName, ClusterId clusterId, ShardId shardId, ApplierMeta applierMeta,
                                LeaderElectorManager leaderElectorManager, RedisOpParser parser, KeeperConfig keeperConfig) throws Exception {
        this(clusterName, clusterId, shardId, applierMeta, leaderElectorManager, parser, keeperConfig, 1, 1,
                null, null, null, null, null);
    }

    public DefaultApplierServer(String clusterName, ClusterId clusterId, ShardId shardId, ApplierMeta applierMeta,
                                LeaderElectorManager leaderElectorManager, RedisOpParser parser, KeeperConfig keeperConfig,
                                int stateThreadNum, int workerThreadNum,
                                Long qpsThreshold, Long bytesPerSecondThreshold, Long memoryThreshold, Long concurrencyThreshold, String subenv) throws Exception {
        this.sequenceController = new DefaultSequenceController(qpsThreshold, bytesPerSecondThreshold, memoryThreshold, concurrencyThreshold);
        this.dispatcher = new DefaultCommandDispatcher();
        this.replication = new DefaultGapAllowReplication(this);
        this.offsetRecorder = new AtomicLong(-1);
        this.replId = new AtomicReference<>("?");

        this.parser = parser;
        this.leaderElectorWrapper = new InstanceComponentWrapper<>(createLeaderElector(clusterId, shardId, applierMeta,
                leaderElectorManager));

        this.gtidDistanceThreshold = new AtomicReference<>();
        this.listeningPort = applierMeta.getPort();
        this.clusterId = clusterId;
        this.shardId = shardId;
        this.applierMeta = applierMeta;

        stateThread = Executors.newFixedThreadPool(stateThreadNum,
                ClusterShardAwareThreadFactory.create(clusterId, shardId, "state-" + makeApplierThreadName()));

        workerThreads = Executors.newScheduledThreadPool(workerThreadNum,
                ClusterShardAwareThreadFactory.create(clusterId, shardId, "worker-" + makeApplierThreadName()));

        /* TODO: dispose client when applier closed */
        this.client = AsyncRedisClientFactory.DEFAULT.createClient(clusterName, subenv, workerThreads);

        lwmThread = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(10), ClusterShardAwareThreadFactory.create(clusterId, shardId, "lwm-" + makeApplierThreadName()),
                new ThreadPoolExecutor.DiscardPolicy());

        scheduled = Executors.newScheduledThreadPool(1,
                ClusterShardAwareThreadFactory.create(clusterId, shardId, "sch-" + makeApplierThreadName()));

        pool = new InstanceComponentWrapper<>(new XpipeNettyClientKeyedObjectPool(DEFAULT_KEYED_CLIENT_POOL_SIZE,
                new NettyKeyedPoolClientFactory(new ApplierChannelHandlerFactory(keeperConfig.getApplierReadIdleSeconds()))));

        applierConfigRef = new AtomicReference<>(new ApplierConfig());
        applierStatisticRef = new AtomicReference<>(new ApplierStatistic());
        startGtidSet = new AtomicReference<>(new GtidSet(GtidSet.EMPTY_GTIDSET));
        lostGtidSet = new AtomicReference<>(new GtidSet(GtidSet.EMPTY_GTIDSET));
        execGtidSet = new AtomicReference<>(new GtidSet(GtidSet.EMPTY_GTIDSET));
        rdbParser = new DefaultRdbParser();
        protoChanged = new AtomicBoolean(false);
    }

    private LeaderElector createLeaderElector(ClusterId clusterId, ShardId shardId, ApplierMeta applierMeta,
                                              LeaderElectorManager leaderElectorManager) {

        String leaderElectionZKPath = MetaZkConfig.getApplierLeaderLatchPath(clusterId, shardId);
        String leaderElectionID = MetaZkConfig.getApplierLeaderElectionId(applierMeta);
        ElectContext ctx = new ElectContext(leaderElectionZKPath, leaderElectionID);
        return leaderElectorManager.createLeaderElector(ctx);
    }

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        String threadPoolName = makeApplierThreadName();
        bossGroup = new NioEventLoopGroup(DEFAULT_NTEEY_BOSS_THREADS_SIZE,
                ClusterShardAwareThreadFactory.create(clusterId, shardId, "boss-" + threadPoolName));
        workerGroup = new NioEventLoopGroup(DEFAULT_NTEEY_WORK_THREADS_SIZE,
                ClusterShardAwareThreadFactory.create(clusterId, shardId, "work-" + threadPoolName));
        clientExecutors = Executors.newSingleThreadExecutor(ClusterShardAwareThreadFactory.create(clusterId, shardId,
                "RedisClient-" + threadPoolName));
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        startServer();
        startTime = System.currentTimeMillis();
    }

    @Override
    protected void doStop() throws Exception {
        super.doStop();
        clearClients();
        stopServer();
    }

    @Override
    protected void doDispose() throws Exception {
        super.doDispose();
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
        stateThread.shutdownNow();
        client.shutdown();
        workerThreads.shutdownNow();
        scheduled.shutdownNow();
        clientExecutors.shutdownNow();
    }

    @Override
    public int getListeningPort() {
        return listeningPort;
    }

    @Override
    public ApplierInstanceMeta getApplierInstanceMeta() {
        return new ApplierInstanceMeta(clusterId, shardId, applierMeta);
    }

    @Override
    public void setStateActive(Endpoint endpoint, GtidSet gtidSet, ApplierConfig config) {
        this.applierConfigRef.set(config);
        this.applierStatisticRef.set(new ApplierStatistic());
        this.state = STATE.ACTIVE;
        replication.connect(endpoint, gtidSet);
    }

    @Override
    public void setStateBackup() {
        this.state = STATE.BACKUP;
        replication.connect(null);
    }

    @Override
    public void freezeConfig() {
        client.freezeConfig();
    }

    @Override
    public void stopFreezeConfig() {
        client.stopFreezeConfig();
    }

    @Override
    public long getFreezeLastMillis() {
        return client.getFreezeLastMillis();
    }

    @Override
    public STATE getState() {
        return state;
    }

    @Override
    public Endpoint getUpstreamEndpoint() {
        return replication.endpoint();
    }

    @Override
    public long getEndOffset() {
        return offsetRecorder.get();
    }

    @Override
    public GtidSet getStartGtidSet() {
        return startGtidSet.get();
    }

    @Override
    public GtidSet getLostGtidSet() {
        return lostGtidSet.get();
    }

    @Override
    public GtidSet getExecGtidSet() {
        return execGtidSet.get();
    }

    @Override
    public void processCommandSequentially(Runnable runnable) {
        clientExecutors.execute(new LongTimeAlertTask(runnable, DEFAULT_LONG_TIME_ALERT_TASK_MILLI));
    }

    protected void startServer() throws InterruptedException {

        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .handler(infoLoggingHandler)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(debugLoggingHandler);
                        p.addLast(new NettySimpleMessageHandler());
                        p.addLast(new NettyApplierHandler(DefaultApplierServer.this, new ApplierCommandHandlerManager()));
                    }
                });
        serverSocketChannel = (ServerSocketChannel) b.bind(listeningPort).sync().channel();
    }

    private void stopServer() {

        if (serverSocketChannel != null) {
            serverSocketChannel.close();
        }
    }


    @Override
    public RedisClient<ApplierServer> clientConnected(Channel channel) {
        RedisClient<ApplierServer> redisClient = new ApplierRedisClient(channel, this);
        redisClients.put(channel, redisClient);

        return redisClient;
    }

    @Override
    public void clientDisconnected(Channel channel) {
        redisClients.remove(channel);
    }

    private void clearClients() {
        for (Map.Entry<Channel, RedisClient> entry : redisClients.entrySet()) {
            RedisClient client = entry.getValue();
            try {
                logger.info("[clearClients]close:{}", client);
                client.close();
            } catch (IOException e) {
                logger.error("[clearClients]" + client, e);
            }
        }
        redisClients.clear();
    }

    private String makeApplierThreadName() {
        return String.format("applier:%s", StringUtil.makeSimpleName(clusterId.toString(), shardId.toString()));
    }

    @Override
    public String info() {

        String info = "os:" + OsUtils.osInfo() + RedisProtocol.CRLF;
        info += "run_id:" + applierMeta.getId() + RedisProtocol.CRLF;
        info += "uptime_in_seconds:" + (System.currentTimeMillis() - startTime) / 1000;
        return info;
    }

    @Override
    public ApplierHealth checkHealth() {
        if (state != STATE.ACTIVE) return ApplierHealth.healthy();

        ApplierConfig config = applierConfigRef.get();
        ApplierStatistic applierStatistic = applierStatisticRef.get();
        long drop = applierStatistic.getDroppedKeys();
        long trans = applierStatistic.getTransKeys();
        long total = drop + trans;
        if (total < 100) return ApplierHealth.healthy(); // avoid misjudgment in the case of a small sample size

        long ration = drop * 100 / total;
        if (config.getDropAllowKeys() > 0 && drop > config.getDropAllowKeys()) {
            return ApplierHealth.unhealthy("DROP_KEYS");
        } else if (config.getDropAllowRation() > 0 && ration > config.getDropAllowRation()) {
            return ApplierHealth.unhealthy("DROP_RATION");
        }

        if(!config.getProtoChangeAllow() && protoChanged.get()) {
            return ApplierHealth.unhealthy("PROTO_CHANGE");
        }


        return ApplierHealth.healthy();
    }

    @Override
    public ApplierStatistic getStatistic() {
        return applierStatisticRef.get();
    }

    @VisibleForTesting
    protected void setState(STATE state, ApplierConfig config, ApplierStatistic statistic) {
        this.applierConfigRef.set(config);
        this.applierStatisticRef.set(statistic);
        this.state = state;
    }

    @Override
    public SERVER_ROLE role() {
        return SERVER_ROLE.APPLIER;
    }

    @Override
    public void doOnFullSync(String replId, long replOffset) {

    }

    @Override
    public void doOnXFullSync(GtidSet lost, long replOffset) {

    }

    @Override
    public void doOnXContinue(GtidSet lost, long replOffset) {

    }

    @Override
    public void doOnContinue(String newReplId) {

    }

    @Override
    public void doOnAppendCommand(ByteBuf byteBuf) {

    }

    @Override
    public void endReadRdb() {

    }

    @Override
    public void protoChange() {
        logger.info("PROTO changed");
        protoChanged.set(true);
    }
}
