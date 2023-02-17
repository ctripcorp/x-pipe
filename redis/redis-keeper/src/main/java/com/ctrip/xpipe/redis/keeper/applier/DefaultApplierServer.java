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
import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.store.ClusterId;
import com.ctrip.xpipe.redis.core.store.ShardId;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.applier.lwm.ApplierLwmManager;
import com.ctrip.xpipe.redis.keeper.applier.lwm.DefaultLwmManager;
import com.ctrip.xpipe.redis.keeper.applier.sequence.ApplierSequenceController;
import com.ctrip.xpipe.redis.keeper.applier.sequence.DefaultSequenceController;
import com.ctrip.xpipe.redis.keeper.applier.threshold.GTIDDistanceThreshold;
import com.ctrip.xpipe.redis.keeper.applier.xsync.ApplierCommandDispatcher;
import com.ctrip.xpipe.redis.keeper.applier.xsync.ApplierXsyncReplication;
import com.ctrip.xpipe.redis.keeper.applier.xsync.DefaultCommandDispatcher;
import com.ctrip.xpipe.redis.keeper.applier.xsync.DefaultXsyncReplication;
import com.ctrip.xpipe.redis.keeper.handler.ApplierCommandHandlerManager;
import com.ctrip.xpipe.redis.keeper.impl.ApplierRedisClient;
import com.ctrip.xpipe.redis.keeper.netty.ApplierChannelHandlerFactory;
import com.ctrip.xpipe.redis.keeper.netty.NettyApplierHandler;
import com.ctrip.xpipe.utils.ClusterShardAwareThreadFactory;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.StringUtil;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Slight
 * <p>
 * Jun 01, 2022 08:16
 */
public class DefaultApplierServer extends AbstractInstanceNode implements ApplierServer {

    /* component */

    @InstanceDependency
    public ApplierSequenceController sequenceController;

    @InstanceDependency
    public ApplierLwmManager lwmManager;

    @InstanceDependency
    public ApplierXsyncReplication replication;

    @InstanceDependency
    public ApplierCommandDispatcher dispatcher;

    @InstanceDependency
    public AtomicReference<GTIDDistanceThreshold> gtidDistanceThreshold;

    @InstanceDependency
    public InstanceComponentWrapper<LeaderElector> leaderElectorWrapper;

    //@InstanceDependency
    //public QPSThreshold qpsThreshold;

    /* cardinal info */

    @InstanceDependency
    public AsyncRedisClient client;

    @InstanceDependency
    public AtomicReference<GtidSet> gtid_executed;

    public final int listeningPort;

    public final ClusterId clusterId;

    public final ShardId shardId;

    public final ApplierMeta applierMeta;

    private STATE state = STATE.NONE;

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
    public ExecutorService lwmThread;

    @InstanceDependency
    public ScheduledExecutorService scheduled;

    private long startTime;

    private final Map<Channel, RedisClient> redisClients = new ConcurrentHashMap<Channel, RedisClient>();

    private EventLoopGroup bossGroup ;

    private EventLoopGroup workerGroup;

    private ServerSocketChannel serverSocketChannel;

    private ExecutorService clientExecutors;

    //TODO change value
    private static final int DEFAULT_KEYED_CLIENT_POOL_SIZE = 100;

    private static final int DEFAULT_NTEEY_BOSS_THREADS_SIZE = 1;

    private static final int DEFAULT_NTEEY_WORK_THREADS_SIZE = 2;

    private static final int DEFAULT_LONG_TIME_ALERT_TASK_MILLI = 1000;

    public DefaultApplierServer(String clusterName, ClusterId clusterId, ShardId shardId, ApplierMeta applierMeta,
                                LeaderElectorManager leaderElectorManager, RedisOpParser parser) throws Exception {
        this.sequenceController = new DefaultSequenceController();
        this.lwmManager = new DefaultLwmManager();
        this.replication = new DefaultXsyncReplication();
        this.dispatcher = new DefaultCommandDispatcher();

        this.parser = parser;
        this.leaderElectorWrapper = new InstanceComponentWrapper<>(createLeaderElector(clusterId, shardId, applierMeta,
                leaderElectorManager));

        this.gtidDistanceThreshold = new AtomicReference<>();
        this.gtid_executed = new AtomicReference<>();
        this.listeningPort = applierMeta.getPort();
        this.clusterId = clusterId;
        this.shardId = shardId;
        this.applierMeta = applierMeta;

        stateThread = Executors.newFixedThreadPool(1,
                ClusterShardAwareThreadFactory.create(clusterId, shardId, "state-" + makeApplierThreadName()));

        workerThreads = Executors.newScheduledThreadPool(8,
                ClusterShardAwareThreadFactory.create(clusterId, shardId, "worker-" + makeApplierThreadName()));

        /* TODO: dispose client when applier closed */
        this.client = AsyncRedisClientFactory.DEFAULT.createClient(clusterName, workerThreads);

        lwmThread = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(10), ClusterShardAwareThreadFactory.create(clusterId, shardId, "lwm-" + makeApplierThreadName()),
                new ThreadPoolExecutor.DiscardPolicy());

        scheduled = Executors.newScheduledThreadPool(1,
                ClusterShardAwareThreadFactory.create(clusterId, shardId, "sch-" + makeApplierThreadName()));

        pool = new InstanceComponentWrapper<>(new XpipeNettyClientKeyedObjectPool(DEFAULT_KEYED_CLIENT_POOL_SIZE,
                new NettyKeyedPoolClientFactory(new ApplierChannelHandlerFactory())));
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
                ClusterShardAwareThreadFactory.create(clusterId, shardId, "work-"+ threadPoolName));
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
        lwmThread.shutdownNow();
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
    public void setStateActive(Endpoint endpoint, GtidSet gtidSet) {
        this.state = STATE.ACTIVE;
        replication.connect(endpoint, gtidSet);
    }

    @Override
    public void setStateBackup() {
        this.state = STATE.BACKUP;
        replication.connect(null);
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
    public void processCommandSequentially(Runnable runnable) {
        clientExecutors.execute(new LongTimeAlertTask(runnable, DEFAULT_LONG_TIME_ALERT_TASK_MILLI));
    }

    protected void startServer() throws InterruptedException {

        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new LoggingHandler(LogLevel.DEBUG));
                        p.addLast(new NettySimpleMessageHandler());
                        p.addLast(new NettyApplierHandler(DefaultApplierServer.this, new ApplierCommandHandlerManager()));
                    }
                });
        serverSocketChannel = (ServerSocketChannel) b.bind(listeningPort).sync().channel();
    }

    private void stopServer() {

        if(serverSocketChannel != null){
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
        info += "uptime_in_seconds:" + (System.currentTimeMillis() - startTime)/1000;
        return info;
    }

    @Override
    public SERVER_ROLE role() {
        return SERVER_ROLE.APPLIER;
    }
}
