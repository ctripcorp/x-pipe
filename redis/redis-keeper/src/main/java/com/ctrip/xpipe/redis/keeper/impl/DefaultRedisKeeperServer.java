package com.ctrip.xpipe.redis.keeper.impl;


import com.ctrip.xpipe.api.cluster.LeaderElector;
import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.api.monitor.Task;
import com.ctrip.xpipe.api.monitor.TransactionMonitor;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.api.proxy.ProxyEnabled;
import com.ctrip.xpipe.cluster.ElectContext;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.concurrent.LongTimeAlertTask;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.netty.NettySimpleMessageHandler;
import com.ctrip.xpipe.observer.NodeAdded;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.meta.MetaZkConfig;
import com.ctrip.xpipe.redis.core.protocal.CAPA;
import com.ctrip.xpipe.redis.core.protocal.PsyncObserver;
import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbConstant;
import com.ctrip.xpipe.redis.core.store.*;
import com.ctrip.xpipe.redis.keeper.*;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.KeeperReplDelayConfig;
import com.ctrip.xpipe.redis.keeper.config.KeeperResourceManager;
import com.ctrip.xpipe.redis.keeper.config.ReplDelayConfigCache;
import com.ctrip.xpipe.redis.keeper.exception.RedisSlavePromotionException;
import com.ctrip.xpipe.redis.keeper.handler.CommandHandlerManager;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
import com.ctrip.xpipe.redis.keeper.monitor.KeepersMonitorManager;
import com.ctrip.xpipe.redis.keeper.netty.NettyMasterHandler;
import com.ctrip.xpipe.redis.keeper.ratelimit.SyncRateManager;
import com.ctrip.xpipe.redis.keeper.store.DefaultFullSyncListener;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.util.KeeperReplIdAwareThreadFactory;
import com.ctrip.xpipe.utils.*;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.internal.ConcurrentSet;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.ctrip.xpipe.redis.core.store.FULLSYNC_FAIL_CAUSE.FULLSYNC_PROGRESS_NOT_SUPPORTED;
import static com.ctrip.xpipe.redis.keeper.SLAVE_STATE.*;

/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午2:08:26
 */
public class DefaultRedisKeeperServer extends AbstractRedisServer implements RedisKeeperServer {

	private static final int DEFAULT_SCHEDULED_CORE_POOL_SIZE = 1;
	private static final int DEFAULT_BOSS_EVENT_LOOP_SIZE = 1;
	// master thread size must be one
	// otherwise we hardly finish all old replication work before new replication start on master address changing
	private static final int DEFAULT_MASTER_EVENT_LOOP_SIZE = 1;
	private static final int DEFAULT_RDB_EVENT_LOOP_SIZE = 1;
	private static final int DEFAULT_MASTER_CONFIG_EVENT_LOOP_SIZE = 1;

	public static String KEY_DEFAULT_KEEPER_WORKER_GROUP_THREAD_COUNT = "DEFAULT_KEEPER_WORKER_GROUP_THREAD_COUNT";
	public static int DEFAULT_KEEPER_WORKER_GROUP_THREAD_COUNT = Integer.parseInt(System.getProperty(KEY_DEFAULT_KEEPER_WORKER_GROUP_THREAD_COUNT, "5"));
	private static final int DEFAULT_LONG_TIME_ALERT_TASK_MILLI = 1000;

	private static String KEY_SEQ_FSYNC_CHECK_PERIOD_SEC = "SEQ_FSYNC_CHECK_PERIOD_SEC";
	public static int DEFAULT_FSYNC_CHECK_PERIOD_SEC = Integer.parseInt(System.getProperty(KEY_SEQ_FSYNC_CHECK_PERIOD_SEC, "5"));

	/**
	 * when keeper is active, it's redis master, else it's another keeper
	 */
	private volatile RedisMaster keeperRedisMaster;

	private AtomicBoolean crossRegion;
	
	private long keeperStartTime;

	private SyncRateManager syncRateManager;
	
	@VisibleForTesting ReplicationStoreManager replicationStoreManager;

	private ServerSocketChannel serverSocketChannel;
	
    private EventLoopGroup bossGroup ;
    private EventLoopGroup workerGroup;
    private NioEventLoopGroup masterEventLoopGroup;
	private NioEventLoopGroup rdbOnlyEventLoopGroup;
	private NioEventLoopGroup masterConfigEventLoopGroup;

	private final Map<Channel, RedisClient<RedisKeeperServer>>  redisClients = new ConcurrentHashMap<>();

	/**
	 * redis slaves receiving rdb or loading rdb
	 */
	private final Set<RedisSlave> loadingSlaves = new ConcurrentSet<>();

	ScheduledFuture<?> fsyncSeqScheduledFuture;

	private String threadPoolName;

	private volatile boolean isStartIndexing;
	private volatile ExecutorService indexingExecutors; //also treated as a state

	private ScheduledExecutorService scheduled;
	private ExecutorService clientExecutors;

	private final ClusterId clusterId;
	private final ShardId shardId;
	private final ReplId replId;
	private final File baseDir;
	
	private volatile RedisKeeperServerState redisKeeperServerState;
	private KeeperConfig keeperConfig; 
	
	private KeeperMeta currentKeeperMeta;
	private LeaderElector leaderElector;

	private LeaderElectorManager leaderElectorManager;

	private volatile long lastResetElectionTime = 0;

	private volatile AtomicReference<RdbDumper> rdbDumper = new AtomicReference<RdbDumper>(null);
	private long lastDumpTime = -1;

	private AtomicLong lastRdbDumpTime = new AtomicLong(-1);
	private AtomicLong lastRordbDumpTime = new AtomicLong(-1);

	//for test
	private AtomicInteger  rdbDumpTryCount = new AtomicInteger();
	
	private KeepersMonitorManager keepersMonitorManager;
	private KeeperMonitor keeperMonitor;

	private KeeperResourceManager resourceManager;

	private RedisOpParser redisOpParser;

	private ReplDelayConfigCache replDelayConfigCache;

	public DefaultRedisKeeperServer(Long replId, KeeperMeta currentKeeperMeta, KeeperConfig keeperConfig, File baseDir,
									LeaderElectorManager leaderElectorManager,
									KeepersMonitorManager keepersMonitorManager,
									KeeperResourceManager resourceManager, SyncRateManager syncRateManager){

		this(replId, currentKeeperMeta, keeperConfig, baseDir, leaderElectorManager, keepersMonitorManager, resourceManager, syncRateManager, null, null);
	}

	public DefaultRedisKeeperServer(Long replId, KeeperMeta currentKeeperMeta, KeeperConfig keeperConfig, File baseDir,
									LeaderElectorManager leaderElectorManager,
									KeepersMonitorManager keepersMonitorManager, KeeperResourceManager resourceManager,
									SyncRateManager syncRateManager, RedisOpParser redisOpParser, ReplDelayConfigCache replDelayConfigCache){

		this.clusterId = ClusterId.from(((ClusterMeta) currentKeeperMeta.parent().parent()).getDbId());
		this.shardId = ShardId.from(currentKeeperMeta.parent().getDbId());
		this.replId = ReplId.from(replId);
		this.currentKeeperMeta = currentKeeperMeta;
		this.baseDir = baseDir;
		this.keeperConfig = keeperConfig;
		this.keepersMonitorManager = keepersMonitorManager;
		this.keeperMonitor = keepersMonitorManager.getOrCreate(this);
		this.leaderElectorManager = leaderElectorManager;
		this.resourceManager = resourceManager;
		this.redisOpParser = redisOpParser;
		this.crossRegion = new AtomicBoolean(false);
		this.syncRateManager = syncRateManager;
		this.replDelayConfigCache = replDelayConfigCache;
	}

	protected ReplicationStoreManager createReplicationStoreManager(KeeperConfig keeperConfig, ClusterId clusterId, ShardId shardId, ReplId replId,
																	KeeperMeta currentKeeperMeta, File baseDir, KeeperMonitor keeperMonitor) {
		return new DefaultReplicationStoreManager.ClusterAndShardCompatible(keeperConfig, replId, currentKeeperMeta.getId(),
				baseDir, keeperMonitor, redisOpParser, syncRateManager).setDeprecatedClusterAndShard(clusterId, shardId);
	}

	private LeaderElector createLeaderElector(){
		
		String leaderElectionZKPath = MetaZkConfig.getKeeperLeaderLatchPath(replId);
		String leaderElectionID = MetaZkConfig.getKeeperLeaderElectionId(currentKeeperMeta);
		ElectContext ctx = new ElectContext(leaderElectionZKPath, leaderElectionID);
		return leaderElectorManager.createLeaderElector(ctx);
	}

	@Override
	public void resetElection() {
		try {
			LifecycleHelper.stopIfPossible(leaderElector);
			LifecycleHelper.startIfPossible(leaderElector);
			this.lastResetElectionTime = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
		} catch (Throwable th) {
			logger.info("[resetElection][fail][{}]", replId, th);
		}
	}

	@Override
	public boolean isLeader() {
		return getLifecycleState().isStarted() && leaderElector.hasLeaderShip();
	}

	@Override
	public long getLastElectionResetTime() {
		return this.lastResetElectionTime;
	}

	@Override
	public void releaseRdb() throws IOException {
		getCurrentReplicationStore().releaseRdb();
	}

	@Override
	protected void doInitialize() throws Exception {
		super.doInitialize();

		replicationStoreManager = createReplicationStoreManager(keeperConfig, clusterId, shardId, replId,
				currentKeeperMeta, baseDir, keeperMonitor);
		replicationStoreManager.addObserver(new ReplicationStoreManagerListener());
		replicationStoreManager.initialize();
		
		threadPoolName = String.format("keeper:%s", replId);
		logger.info("[doInitialize][keeper config]{}", keeperConfig);

		clientExecutors = Executors.newSingleThreadExecutor(KeeperReplIdAwareThreadFactory.create(replId, "RedisClient-" + threadPoolName));
		scheduled = Executors.newScheduledThreadPool(DEFAULT_SCHEDULED_CORE_POOL_SIZE , KeeperReplIdAwareThreadFactory.create(replId, "sch-" + threadPoolName));
		bossGroup = new NioEventLoopGroup(DEFAULT_BOSS_EVENT_LOOP_SIZE, KeeperReplIdAwareThreadFactory.create(replId, "boss-" + threadPoolName));
		workerGroup = new NioEventLoopGroup(DEFAULT_KEEPER_WORKER_GROUP_THREAD_COUNT, KeeperReplIdAwareThreadFactory.create(replId, "work-"+ threadPoolName));
		masterEventLoopGroup = new NioEventLoopGroup(DEFAULT_MASTER_EVENT_LOOP_SIZE, KeeperReplIdAwareThreadFactory.create(replId, "master-" + threadPoolName));
		rdbOnlyEventLoopGroup = new NioEventLoopGroup(DEFAULT_RDB_EVENT_LOOP_SIZE, KeeperReplIdAwareThreadFactory.create(replId, "rdbOnly-" + threadPoolName));
		masterConfigEventLoopGroup = new NioEventLoopGroup(DEFAULT_MASTER_CONFIG_EVENT_LOOP_SIZE, KeeperReplIdAwareThreadFactory.create(replId, "masterConfig-" + threadPoolName));


		this.resetReplAfterLongTimeDown();
		this.leaderElector = createLeaderElector();
		this.leaderElector.initialize();
	 	this.redisKeeperServerState = initKeeperServerState();
	 	logger.info("[doInitialize]{}", this.redisKeeperServerState.keeperState());

	}

	@Override
	public XSyncContinue locateContinueGtidSet(GtidSet gtidSet) throws Exception {
		return getCurrentReplicationStore().locateContinueGtidSet(gtidSet);
	}

	@Override
	public void switchToPSync(String replId, long offset) throws IOException {
		getCurrentReplicationStore().switchToPSync(replId, offset);
		closeSlaves("toPSync " + replId + ":" + offset);
	}

	@Override
	public void switchToXSync(String replId, long replOff, String masterUuid, GtidSet gtidSet) throws IOException {
		getCurrentReplicationStore().switchToXSync(replId, replOff, masterUuid, gtidSet);
		closeSlaves("toXSync " + gtidSet);
	}

	private void resetReplAfterLongTimeDown() {
		try {
			ReplicationStore replicationStore = replicationStoreManager.getCurrent();
			if (null == replicationStore || null == replicationStore.getMetaStore().getReplId()) {
				logger.debug("[resetReplAfterLongTimeDown][empty] skip");
				return;
			}

			long lastReplDataUpdatedAt = replicationStore.lastReplDataUpdatedAt();
			long currentTime = System.currentTimeMillis();
			long safeDownTime = TimeUnit.SECONDS.toMillis(keeperConfig.getMaxReplKeepSecondsAfterDown());
			long replDownTime = currentTime - lastReplDataUpdatedAt;
			if (replDownTime > safeDownTime) {
				logger.info("[resetReplAfterLongTimeDown][down long] reset, {} - {} > {}", currentTime, lastReplDataUpdatedAt, safeDownTime);
				replicationStoreManager.create();
			} else if (replDownTime < 0) {
				logger.info("[resetReplAfterLongTimeDown][time rollback] reset {} - {} < 0", currentTime, lastReplDataUpdatedAt);
				replicationStoreManager.create();
			} else {
				logger.debug("[resetReplAfterLongTimeDown][safe] {} - {} <= {}", currentTime, lastReplDataUpdatedAt, safeDownTime);
			}

		} catch (Throwable th) {
			logger.info("[resetReplAfterLongTimeDown][fail]", th);
		}
	}
	
	private RedisKeeperServerState initKeeperServerState() {
		
		try {
			ReplicationStore replicationStore = replicationStoreManager.getCurrent();
			if(replicationStore == null){
				return new RedisKeeperServerStateUnknown(this);  
			}
			KeeperState keeperState = replicationStore.getMetaStore().dupReplicationStoreMeta().getKeeperState();
			if(keeperState == null){
				logger.warn("[initKeeperServerState][keeperState null]");
				return new RedisKeeperServerStateUnknown(this);
			}
			
			RedisKeeperServerState redisKeeperServerState = null; 
			switch(keeperState){
				case ACTIVE:
					redisKeeperServerState = new RedisKeeperServerStatePreActive(this);
					break;
				case BACKUP:
					redisKeeperServerState = new RedisKeeperServerStatePreBackup(this);
					break;
				case UNKNOWN:
					redisKeeperServerState = new RedisKeeperServerStateUnknown(this);
					break;
				//wrong store state
				case PRE_ACTIVE:
				case PRE_BACKUP:
				default:
					logger.warn("[initKeeperServerState][error state]{}", keeperState);
					redisKeeperServerState = new RedisKeeperServerStateUnknown(this);
					break;
			}
			return redisKeeperServerState;
		} catch (Exception e) {
			logger.error("[initKeeperServerState]" + this, e);
		}
		return new RedisKeeperServerStateUnknown(this);  
	}

	@Override
	protected void doStart() throws Exception {
		super.doStart();
		keeperMonitor.start();
		replicationStoreManager.start();
		keeperStartTime = System.currentTimeMillis();
		startServer();
		LifecycleHelper.startIfPossible(keeperRedisMaster);
		this.leaderElector.start();
		fsyncSeqScheduledFuture = this.scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
			@Override
			protected void doRun() throws Exception {
				updateLoadingSlaves();
				continueFsyncSequentially();
			}
		}, DEFAULT_FSYNC_CHECK_PERIOD_SEC, DEFAULT_FSYNC_CHECK_PERIOD_SEC, TimeUnit.SECONDS);
	}

	@VisibleForTesting
	protected void continueFsyncSequentially() {
		if (!getRedisKeeperServerState().keeperState().isActive()) return;

		int maxLoadingSlavesCnt = keeperConfig.getCrossRegionMaxLoadingSlavesCnt();
		Set<RedisSlave> slaves = slaves();
		int currentLoadingSlaves = loadingSlaves.size();
		if (maxLoadingSlavesCnt >= 0 && crossRegion.get() && currentLoadingSlaves >= maxLoadingSlavesCnt) return;

		for (RedisSlave slave: slaves) {
			if (slave.getSlaveState() == REDIS_REPL_WAIT_SEQ_FSYNC) {
				continueFsyncToSlave(slave);
			}
		}
	}

	private void continueFsyncToSlave(RedisSlave slave) {
		try {
			logger.info("[continueFsyncToSlave]{}", slave);
			slave.processPsyncSequentially(new Runnable() {
				@Override
				public void run() {
					try {
						fullSyncToSlave(slave);
					} catch (Throwable th) {
						try {
							logger.error("[continueFsyncToSlave][run]{}", slave, th);
							if(slave.isOpen()){
								slave.close();
							}
						} catch (IOException e) {
							logger.error("[continueFsyncToSlave][close]{}", slave, th);
						}
					}
				}
			});
		} catch (Throwable th) {
			logger.info("[continueFsyncToSlave][fail]{}", slave, th);
		}
	}
	
	@Override
	protected void doStop() throws Exception {
		if (null != fsyncSeqScheduledFuture) {
			fsyncSeqScheduledFuture.cancel(false);
		}
		keeperMonitor.stop();
		clearClients();
		clearLoadingSlaves();
		this.leaderElector.stop();
		LifecycleHelper.stopIfPossible(keeperRedisMaster);
		stopServer();
		replicationStoreManager.stop();
		super.doStop();
	}

	private void clearClients() {
		for (Entry<Channel, RedisClient<RedisKeeperServer>> entry : redisClients.entrySet()) {
			RedisClient<RedisKeeperServer> client = entry.getValue();
			try {
				logger.info("[clearClients]close:{}", client);
				client.close();
			} catch (IOException e) {
				logger.error("[clearClients]" + client, e);
			}
		}
		redisClients.clear();
	}

	@Override
	protected void doDispose() throws Exception {

		LifecycleHelper.disposeIfPossible(keeperRedisMaster);
		this.leaderElector.dispose();
		masterConfigEventLoopGroup.shutdownGracefully();
		masterEventLoopGroup.shutdownGracefully();
		rdbOnlyEventLoopGroup.shutdownGracefully();
		bossGroup.shutdownGracefully();
		workerGroup.shutdownGracefully();
		replicationStoreManager.dispose();
		this.scheduled.shutdownNow();
		this.clientExecutors.shutdownNow();
		if (null != indexingExecutors) indexingExecutors.shutdown();
		super.doDispose();
	}

	
	@Override
	public synchronized void reconnectMaster() {
		
		Endpoint target = redisKeeperServerState.getMaster();
		logger.info("[reconnectMaster]{} -> {}", this, target);

		if(keeperRedisMaster != null && target != null && keeperRedisMaster.getLifecycleState().isStarted()){
			Endpoint current = keeperRedisMaster.masterEndPoint();
			if(current != null && isMasterSame(current, target)) {
				logger.info("[reconnectMaster][master the same]{},{}", current, target);
				return;
			}
		}
		
		stopAndDisposeMaster();
		if(target == null){
			logger.info("[reconnectMaster][target null][close master connection]{}, {}", this, redisKeeperServerState);
			return;
		}
		initAndStartMaster(target);
		this.crossRegion.set(keeperRedisMaster.usingProxy());
	}

	private boolean isMasterSame(Endpoint current, Endpoint target) {
		boolean result = ObjectUtils.equals(current, target);
		if(!result) {
			return false;
		}
		if(current instanceof ProxyEnabled && target instanceof ProxyEnabled) {
			result = ((ProxyEnabled) current).isSameWith((ProxyEnabled) target);
		}
		return result;
	}

	private void initAndStartMaster(Endpoint target) {
		try {
			this.keeperRedisMaster = new DefaultRedisMaster(this, (DefaultEndPoint)target, masterEventLoopGroup,
					rdbOnlyEventLoopGroup, masterConfigEventLoopGroup, replicationStoreManager, scheduled, resourceManager);

			if(getLifecycleState().isStopping() || getLifecycleState().isStopped()){
				logger.info("[initAndStartMaster][stopped, exit]{}, {}", target, this);
				return;
			}
			LifecycleHelper.initializeIfPossible(this.keeperRedisMaster);
			LifecycleHelper.startIfPossible(this.keeperRedisMaster);
		} catch (Exception e) {
			logger.error("[doReplicationMaster]" + this + "," + keeperRedisMaster, e);
		}
	}

	private void stopServer() {
		
		if(serverSocketChannel != null){
			serverSocketChannel.close();
		}
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
                 p.addLast(new NettyMasterHandler(DefaultRedisKeeperServer.this, new CommandHandlerManager(), keeperConfig.getTrafficReportIntervalMillis()));
             }
         });
        serverSocketChannel = (ServerSocketChannel) b.bind(currentKeeperMeta.getPort()).sync().channel();
    }
		

	@Override
	public RedisClient<RedisKeeperServer> clientConnected(Channel channel) {

		DefaultRedisClient redisClient = new DefaultRedisClient(channel, this);
		redisClient.setReplDelayConfigCache(replDelayConfigCache);
		redisClients.put(channel, redisClient);
		
		redisClient.addObserver(new Observer() {
			
			@Override
			public void update(Object args, Observable observable) {
				
				if(args instanceof RedisSlave){
					becomeSlave(((RedisClient<?>)observable).channel(), (RedisSlave)args);
				}
			}
		});
		
		return redisClient;
	}

	protected void becomeSlave(Channel channel, RedisSlave redisSlave) {

		logger.info("[update][redis client become slave]{}", channel);

		synchronized (channel) {
			if (redisClients.get(channel) == null) {
				logger.info("[update][redis client become slave]{}{}", channel, "slave is already closed");
				return;
			}
			redisClients.put(channel, redisSlave);
		}
	}

	@Override
	public void clientDisconnected(Channel channel) {

		synchronized (channel) {
			redisClients.remove(channel);
		}
	}

	@Override
	public String toString() {
		return String.format("%s(%s:%d)", getClass().getSimpleName(), currentKeeperMeta.getIp(), currentKeeperMeta.getPort());
	}

	@Override
	public ReplId getReplId() {
		return replId;
	}

	@Override
	public String getKeeperRunid() {
		
		return this.currentKeeperMeta.getId();
	}

	
	@Override
	public KeeperRepl getKeeperRepl() {
		
		return new DefaultKeeperRepl(getCurrentReplicationStore());
	}
	
	
	protected ReplicationStore getCurrentReplicationStore(){

		if(!getLifecycleState().isInitialized()){

			throw new RedisKeeperServerStateException(toString(), getLifecycleState().getPhaseName());
		}
		
		try {
			ReplicationStore replicationStore = replicationStoreManager.createIfNotExist(); 
			return replicationStore;
		} catch (IOException e) {
			logger.error("[getCurrentReplicationStore]" + this, e);
			throw new XpipeRuntimeException("[getCurrentReplicationStore]" + this, e);
		}
	}

	@Override
	public Set<RedisClient> allClients() {
		return new HashSet<>(redisClients.values());
	}

	@Override
	public SERVER_ROLE role() {
		return SERVER_ROLE.KEEPER;
	}

	@Override
	public String info() {
		
		String info = "os:" + OsUtils.osInfo() + RedisProtocol.CRLF;
		info += "run_id:" + currentKeeperMeta.getId() + RedisProtocol.CRLF;
		info += "uptime_in_seconds:" + (System.currentTimeMillis() - keeperStartTime)/1000;
		return info;
	}

	@Override
	public Set<RedisSlave> slaves() {

		Set<RedisSlave> slaves = new HashSet<>();

		for (Entry<Channel, RedisClient<RedisKeeperServer>> entry : redisClients.entrySet()) {

			RedisClient<RedisKeeperServer> redisClient = entry.getValue();
			if(redisClient instanceof RedisSlave){
				slaves.add((RedisSlave)redisClient);
			}
		}
		return slaves;
	}

   public ReplicationStore getReplicationStore() {
	   return getCurrentReplicationStore();
   }

	@Override
	public int getListeningPort() {
		return currentKeeperMeta.getPort();
	}

	@Override
	public void beginWriteRdb(EofType eofType, String replId, long offset) {
	}

	@Override
	public void endWriteRdb() {
		
	}

	@Override
	public void reFullSync() {

		closeSlaves("reFullSync");
	}

	@Override
	public void onFullSync(long masterRdbOffset) {
		//alert full sync
		String alert = String.format("FULL(S)->%s[%s]", getRedisMaster().metaInfo(), getReplId());
		EventMonitor.DEFAULT.logAlertEvent(alert);

	}

	@Override
	public void readAuxEnd(RdbStore rdbStore, Map<String, String> auxMap) {
		String gtidSet = auxMap.getOrDefault(RdbConstant.REDIS_RDB_AUX_KEY_GTID, GtidSet.EMPTY_GTIDSET);
		try {
			if (isStartIndexing) {
				EventMonitor.DEFAULT.logEvent("INDEX.START", replId + " - " + gtidSet);
				startIndexing();
			}
		} catch (Throwable t) {
			EventMonitor.DEFAULT.logAlertEvent("INDEX.START.FAIL: " + replId + " - " + gtidSet);
		}
	}

	@Override
	public void closeSlaves(String reason) {
		
		for(RedisSlave redisSlave : slaves()){
			try {
				logger.info("[{}][close slave]{}", reason, redisSlave);
				redisSlave.close();
			} catch (Exception e) {
				logger.error("[beginWriteRdb][close slaves]", e);
			}
		}
	}

	@Override
	public void onContinue(String requestReplId, String responseReplId) {
		
		if( responseReplId != null && !requestReplId.equals(responseReplId) ){
			closeSlaves(String.format("replid changed: %s->%s", requestReplId, responseReplId));
		}
	}

	@Override
	public void onKeeperContinue(String replId, long beginOffset) {
		// do nothing
	}

	@Override
	public synchronized void setRedisKeeperServerState(final RedisKeeperServerState redisKeeperServerState){
		
		TransactionMonitor transactionMonitor = TransactionMonitor.DEFAULT;
		String name = String.format("%s,%s", replId, redisKeeperServerState);
		transactionMonitor.logTransactionSwallowException("setRedisKeeperServerState", name, new Task() {
			
			@Override
			public void go() throws Exception {
				
				RedisKeeperServerState previous = DefaultRedisKeeperServer.this.redisKeeperServerState;
				logger.info("[setRedisKeeperServerState]{}, {}->{}", this, previous, redisKeeperServerState);
				DefaultRedisKeeperServer.this.redisKeeperServerState = redisKeeperServerState;
				notifyObservers(new KeeperServerStateChanged(previous, redisKeeperServerState));
			}

			@Override
			public Map getData() {
				return null;
			}

		});
		
	}
	
	@Override
	public synchronized boolean compareAndDo(RedisKeeperServerState expected, Runnable action) {
		if(this.redisKeeperServerState == expected){
			action.run();
			return true;
		}
		return false;
	}


	@Override
	public KeeperMeta getCurrentKeeperMeta() {
		return this.currentKeeperMeta;
	}

	@Override
	public void stopAndDisposeMaster() {

		if(this.keeperRedisMaster != null){
			try {
				LifecycleHelper.stopIfPossible(this.keeperRedisMaster);
				LifecycleHelper.disposeIfPossible(this.keeperRedisMaster);
				this.keeperRedisMaster = null;
			} catch (Exception e) {
				logger.error("[reconnectMaster][stop previois master]" + this.keeperRedisMaster, e);
			}
		}
	}

	@Override
	public RedisKeeperServerState getRedisKeeperServerState() {
		return this.redisKeeperServerState;
	}

	@Override
	public boolean gapAllowSyncEnabled() {
		//TODO enable by config
		return true;
	}

	@Override
	public RedisMaster getRedisMaster() {
		return keeperRedisMaster;
	}

	@Override
	public void promoteSlave(String ip, int port) throws RedisSlavePromotionException {
		
		logger.info("[promoteSlave]{}:{}", ip, port);
		RedisPromotor promotor = new RedisPromotor(this, ip, port, scheduled);
		promotor.promote();
	}
	
	@Override
	public void fullSyncToSlave(final RedisSlave redisSlave, boolean freshRdbNeeded) throws IOException {
		
		logger.info("[fullSyncToSlave]{}, {}", redisSlave, rdbDumper.get());

		if (crossRegion.get() && !redisSlave.isKeeper() && !tryFullSyncToSlaveWithOthers(redisSlave)) {
			redisSlave.waitForSeqFsync();
			return;
		}

		boolean tryRordb = false; // slave and master all support rordb or not
		if (redisSlave.capaOf(CAPA.RORDB)) {
			try {
				logger.info("[fullSyncToSlave][rordb]{}", redisSlave);
				tryRordb = keeperRedisMaster.checkMasterSupportRordb().get();
				logger.info("[fullSyncToSlave][rordb] masterSupport:{}", tryRordb);
			} catch (Throwable th) {
				logger.info("[fullSyncToSlave][rordb]{}", redisSlave, th);
			}
		}

		if(rdbDumper.get() == null){
			logger.info("[fullSyncToSlave][dumper null]{}", redisSlave);
			if (!freshRdbNeeded) {
				FullSyncListener fullSyncListener = new DefaultFullSyncListener(redisSlave);
				FULLSYNC_FAIL_CAUSE failCause = getCurrentReplicationStore().fullSyncIfPossible(fullSyncListener, tryRordb);
				if (null == failCause) {
					return;
				} else if (FULLSYNC_PROGRESS_NOT_SUPPORTED.equals(failCause)) {
					logger.info("[fullSyncToSlave][progress not support][cancel slave]");
					redisSlave.close();
					return;
				}
			}

			try{
				RdbDumper newDumper = dumpNewRdb(tryRordb, freshRdbNeeded);
				redisSlave.waitForRdbDumping();
				if (newDumper.future().isDone() && !newDumper.future().isSuccess()) {
					logger.info("[fullSyncToSlave][new dumper fail immediatelly]");
					redisSlave.close();
				}
			}catch(AbstractRdbDumperException e){
				logger.error("[fullSyncToSlave]", e);
				if(e.isCancelSlave()){
					logger.info("[fullSyncToSlave][cancel slave]");
					redisSlave.close();
				}
			}
		}else{
			rdbDumper.get().tryFullSync(redisSlave);
		}
	}

	private synchronized boolean tryFullSyncToSlaveWithOthers(RedisSlave redisSlave) {
		if (loadingSlaves.contains(redisSlave)) return true;

		int maxConcurrentLoadingSlaves = keeperConfig.getCrossRegionMaxLoadingSlavesCnt();
		if (redisSlave.isColdStart() || maxConcurrentLoadingSlaves < 0 || loadingSlaves.size() < maxConcurrentLoadingSlaves) {
			loadingSlaves.add(redisSlave);
			return true;
		}

		return false;
	}

	@VisibleForTesting
	protected synchronized void updateLoadingSlaves() {
		Set<RedisSlave> filterSlaves = loadingSlaves.stream()
				.filter(slave -> slave.isKeeper() || !slave.isOpen()
						|| (slave.getSlaveState() == REDIS_REPL_ONLINE && slave.getAck() != null))
				.collect(Collectors.toSet());

		filterSlaves.forEach(loadingSlaves::remove);
	}

	private synchronized void clearLoadingSlaves() {
		loadingSlaves.clear();
	}

	@Override
	public synchronized void startIndexing() throws IOException {

		logger.info("[startIndexing]{}, {}", this, rdbDumper.get());

		if (indexingExecutors == null) {
			indexingExecutors = Executors.newSingleThreadExecutor(KeeperReplIdAwareThreadFactory.create(replId, "Indexing-" + threadPoolName));
		}

		isStartIndexing = true;

		FULLSYNC_FAIL_CAUSE failCause = getCurrentReplicationStore().createIndexIfPossible(indexingExecutors);

		if(rdbDumper.get() == null) {

			if (failCause != null) {
				try {
					dumpNewRdb(false);
				} catch (Throwable t) {
					logger.error("[startIndexing][dumpNewRdb] fail {}, {}", this, rdbDumper.get());
					logger.error("[startIndexing][dumpNewRdb] fail", t);
				}
			}
		}
	}

	@Override
	public boolean isStartIndexing() {
	    return isStartIndexing;
	}

	private RdbDumper dumpNewRdb(boolean tryRordb) throws CreateRdbDumperException, SetRdbDumperException {
		return dumpNewRdb(tryRordb, false);
	}

	private RdbDumper dumpNewRdb(boolean tryRordb, boolean freshRdbNeeded) throws CreateRdbDumperException, SetRdbDumperException {
		
		RdbDumper rdbDumper = keeperRedisMaster.createRdbDumper(tryRordb, freshRdbNeeded);
		setRdbDumper(rdbDumper);
		rdbDumper.execute();
		return rdbDumper;
	}

	
	public void setRdbDumper(RdbDumper rdbDumper) throws SetRdbDumperException {
		setRdbDumper(rdbDumper, false);
	}
	

	@Override
	public KeeperInstanceMeta getKeeperInstanceMeta() {
		return new KeeperInstanceMeta(replId, currentKeeperMeta);
	}
	
	public KeeperConfig getKeeperConfig() {
		return keeperConfig;
	}

	@Override
	public void destroy() throws Exception {
		this.keepersMonitorManager.remove(this);
		this.replicationStoreManager.destroy();
	}

	@Override
	public void setRdbDumper(RdbDumper newDumper, boolean force) throws SetRdbDumperException {
		
		if(newDumper == null){
			throw new IllegalArgumentException("new dumper null");
		}
		
		logger.info("[setRdbDumper]{},{}", newDumper, force);
		rdbDumpTryCount.incrementAndGet();
		AtomicLong lastDumpTime = newDumper.tryRordb() ? lastRordbDumpTime : lastRdbDumpTime;
		
		if(lastDumpTime.get() > 0 && !force && (System.currentTimeMillis() - lastDumpTime.get() < keeperConfig.getRdbDumpMinIntervalMilli())){
			logger.info("[setRdbDumper][too quick]{}", new Date(lastDumpTime.get()));
			throw new SetRdbDumperException(lastDumpTime.get(), keeperConfig.getRdbDumpMinIntervalMilli());
		}
		
		if(rdbDumper.compareAndSet(null, newDumper)){
			lastDumpTime.set(System.currentTimeMillis());
			dumpListener(newDumper);
			return;
		}
		
		RdbDumper olRdbDumper = rdbDumper.get();
		if(force){
			try {
				logger.info("[setRdbDumper][cancel old dumper]{}", olRdbDumper);
				olRdbDumper.future().cancel(true);
			} catch (Exception e) {
				logger.error("[setRdbDumper][error cancel]" + olRdbDumper, e);
			}
			rdbDumper.set(newDumper);
			lastDumpTime.set(System.currentTimeMillis());
			dumpListener(newDumper);
		}else{
			throw new SetRdbDumperException(olRdbDumper);
		}
	}

	private void dumpListener(RdbDumper newDumper) {
		
		CommandFuture<Void> future = newDumper.future();
		if(future == null){
			return;
		}
		future.addListener(new CommandFutureListener<Void>() {
			@Override
			public void operationComplete(CommandFuture<Void> commandFuture) throws Exception {
				
				if(!commandFuture.isSuccess()){
					logger.info("[operationComplete][dump fail, clear dump time]", commandFuture.cause());
					AtomicLong lastDumpTime = newDumper.tryRordb() ? lastRordbDumpTime : lastRdbDumpTime;
					lastDumpTime.set(0);
				}
			}
		});
	}

	@Override
	public void clearRdbDumper(RdbDumper oldDumper, boolean forceRdb) {
		
		logger.info("[clearRdbDumper]{}", oldDumper);
		if(!rdbDumper.compareAndSet(oldDumper, null)){
			logger.warn("[clearRdbDumper][current is not request]{}, {}", oldDumper, rdbDumper.get());
		} else {
			logger.debug("[clearRdbDumper] redump for waiting slaves if needed");
			List<RedisSlave> waitingSlaves = new ArrayList<>();
			int needRordbSlaves = 0;
			for (final RedisSlave redisSlave : slaves()) {
				if (redisSlave.getSlaveState() == SLAVE_STATE.REDIS_REPL_WAIT_RDB_DUMPING) {
					waitingSlaves.add(redisSlave);
					if (redisSlave.capaOf(CAPA.RORDB)) needRordbSlaves++;
				}
			}

			if (!waitingSlaves.isEmpty()) {
				try {
					logger.info("[clearRdbDumper][redump][rdb] waiting:{}, needRordb:{}, forceRdb:{}", waitingSlaves.size(), needRordbSlaves, forceRdb);
					if (forceRdb) {
						dumpNewRdb(false);
					} else {
						// use RORDB only if all slaves accept it
						dumpNewRdb(waitingSlaves.size() == needRordbSlaves);
					}
				} catch (Throwable th) {
					logger.info("[clearRdbDumper][redump] fail", th);
					waitingSlaves.forEach(redisSlave -> {
						try {
							logger.info("[redumpFailed][close]{}", redisSlave);
							redisSlave.close();
						} catch (Throwable t) {
							logger.error("[redumpFailed][close slave]", t);
						}
					});
				}
			}
		}
	}
	
	public int getRdbDumpTryCount() {
		return rdbDumpTryCount.get();
	}

	@Override
	public void onXFullSync(String replId, long replOff, String masterUuid, GtidSet gtidLost) {
		//alert full sync
		// TODO confirm event name
		String alert = String.format("XFULL(S)->%s[%s]", getRedisMaster().metaInfo(), getReplId());
		EventMonitor.DEFAULT.logAlertEvent(alert);
	}

	@Override
	public void onXContinue(String replId, long replOff, String masterUuid, GtidSet gtidCont) {

	}

	@Override
	public void onSwitchToXsync(String replId, long replOff, String masterUuid) {
		//TODO publish CAT event ?
		closeSlaves("switch2xsync");
	}

	@Override
	public void onSwitchToPsync(String replId, long replOff) {
		//TODO publish CAT event ?
		closeSlaves("switch2psync");
	}

	@Override
	public void onUpdateXsync() {
		closeSlaves("updateXsync");
	}

	public class ReplicationStoreManagerListener implements Observer{

		@Override
		public void update(Object args, Observable observable) {
			
			if(args instanceof NodeAdded){
				@SuppressWarnings("unchecked")
				ReplicationStore replicationStore = ((NodeAdded<ReplicationStore>) args).getNode();
				initReplicationStore(replicationStore);
			}
		}
		
	}

	public synchronized void initReplicationStore(ReplicationStore replicationStore) {
		
		logger.info("[initReplicationStore]{}", replicationStore);
		RedisKeeperServerState redisKeeperServerState = getRedisKeeperServerState();
		if(redisKeeperServerState != null){
			KeeperState keeperState = redisKeeperServerState.keeperState();
			
			try {
				if(keeperState.isActive()){
						replicationStore.getMetaStore().becomeActive();
				}else if(keeperState.isBackup()){
					replicationStore.getMetaStore().becomeBackup();
				}else{
					logger.warn("[initReplicationStore][not active and not backup]{}, {}", keeperState, replicationStore);
				}
			} catch (IOException e) {
				logger.error("[initReplicationStore]" + replicationStore, e);
			}
		}
	}

	@Override
	public KeeperMonitor getKeeperMonitor() {
		return keeperMonitor;
	}

	@Override
	public void processCommandSequentially(Runnable runnable) {
		clientExecutors.execute(new LongTimeAlertTask(runnable, DEFAULT_LONG_TIME_ALERT_TASK_MILLI));

	}

	@Override
	public RdbDumper rdbDumper() {
		return rdbDumper.get();
	}

	@Override
	public void resetDefaultReplication() {

		try {
			replicationStoreManager.create();
		} catch (IOException e) {
			throw new XpipeRuntimeException("[RedisKeeperServer][RedisMasterNewRdbDumper][RdbOffsetNotContinuous][RecreateStore]" + replicationStoreManager, e);
		}
		keeperRedisMaster.reconnect();
		closeSlaves("replication reset");
	}

	@Override
	public PsyncObserver createPsyncObserverForRdbOnlyRepl() {
	    RedisKeeperServer redisKeeperServer = this;
		return new PsyncObserver() {

			@Override
			public void onFullSync(long masterRdbOffset) {

			}

			@Override
			public void reFullSync() {

			}

			@Override
			public void beginWriteRdb(EofType eofType, String replId, long masterRdbOffset) throws IOException {

			}

			@Override
			public void readAuxEnd(RdbStore rdbStore, Map<String, String> auxMap) {
				redisKeeperServer.readAuxEnd(rdbStore, auxMap);
			}

			@Override
			public void endWriteRdb() {

			}

			@Override
			public void onContinue(String requestReplId, String responseReplId) {

			}

			@Override
			public void onKeeperContinue(String replId, long beginOffset) {

			}
		};
	}

	@VisibleForTesting
	public ReplicationStoreManager getReplicationStoreManager() {
		return replicationStoreManager;
	}

	@VisibleForTesting
	public void setReplicationStoreManager(ReplicationStoreManager replicationStoreManager) {
		this.replicationStoreManager = replicationStoreManager;
	}

}
