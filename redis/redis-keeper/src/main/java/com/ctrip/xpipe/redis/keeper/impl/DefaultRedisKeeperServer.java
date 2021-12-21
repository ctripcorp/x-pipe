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
import com.ctrip.xpipe.concurrent.LongTimeAlertTask;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.netty.NettySimpleMessageHandler;
import com.ctrip.xpipe.observer.NodeAdded;
import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.meta.MetaZkConfig;
import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.store.*;
import com.ctrip.xpipe.redis.keeper.*;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.KeeperResourceManager;
import com.ctrip.xpipe.redis.keeper.exception.RedisSlavePromotionException;
import com.ctrip.xpipe.redis.keeper.handler.CommandHandlerManager;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
import com.ctrip.xpipe.redis.keeper.monitor.KeepersMonitorManager;
import com.ctrip.xpipe.redis.keeper.netty.NettyMasterHandler;
import com.ctrip.xpipe.redis.keeper.store.DefaultFullSyncListener;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStoreManager;
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
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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

	public static String KEY_DEFAULT_KEEPER_WORKER_GROUP_THREAD_COUNT = "DEFAULT_KEEPER_WORKER_GROUP_THREAD_COUNT";
	public static int DEFAULT_KEEPER_WORKER_GROUP_THREAD_COUNT = Integer.parseInt(System.getProperty(KEY_DEFAULT_KEEPER_WORKER_GROUP_THREAD_COUNT, "5"));
	private static final int DEFAULT_LONG_TIME_ALERT_TASK_MILLI = 1000;

	/**
	 * when keeper is active, it's redis master, else it's another keeper
	 */
	private volatile RedisMaster keeperRedisMaster;
	
	private long keeperStartTime;
	
	@VisibleForTesting ReplicationStoreManager replicationStoreManager;

	private ServerSocketChannel serverSocketChannel;
	
    private EventLoopGroup bossGroup ;
    private EventLoopGroup workerGroup;
    private NioEventLoopGroup masterEventLoopGroup;
	private NioEventLoopGroup rdbOnlyEventLoopGroup;

	private final Map<Channel, RedisClient>  redisClients = new ConcurrentHashMap<Channel, RedisClient>();
	
	private ScheduledExecutorService scheduled;
	private ExecutorService clientExecutors;
	
	private final ClusterId clusterId;
	private final ShardId shardId;
	
	private volatile RedisKeeperServerState redisKeeperServerState;
	private KeeperConfig keeperConfig; 
	
	private KeeperMeta currentKeeperMeta;
	private LeaderElector leaderElector;

	private LeaderElectorManager leaderElectorManager;

	private volatile AtomicReference<RdbDumper> rdbDumper = new AtomicReference<RdbDumper>(null);
	private long lastDumpTime = -1;
	//for test
	private AtomicInteger  rdbDumpTryCount = new AtomicInteger();
	
	private KeepersMonitorManager keepersMonitorManager;
	private KeeperMonitor keeperMonitor;

	private KeeperResourceManager resourceManager;

	private AtomicInteger tryConnectMasterCnt = new AtomicInteger();

	public DefaultRedisKeeperServer(KeeperMeta currentKeeperMeta, KeeperConfig keeperConfig, File baseDir,
									LeaderElectorManager leaderElectorManager,
									KeepersMonitorManager keepersMonitorManager, KeeperResourceManager resourceManager){

		this.clusterId = ClusterId.from(currentKeeperMeta.parent().parent().getDbId());
		this.shardId = ShardId.from(currentKeeperMeta.parent().getDbId());
		this.currentKeeperMeta = currentKeeperMeta;
		this.keeperConfig = keeperConfig;
		this.keepersMonitorManager = keepersMonitorManager;
		this.keeperMonitor = keepersMonitorManager.getOrCreate(this);
		this.replicationStoreManager = new DefaultReplicationStoreManager(keeperConfig, clusterId, shardId, currentKeeperMeta.getId(), baseDir, keeperMonitor);
		replicationStoreManager.addObserver(new ReplicationStoreManagerListener());
		this.leaderElectorManager = leaderElectorManager;
		this.resourceManager = resourceManager;
	}

	private LeaderElector createLeaderElector(){
		
		String leaderElectionZKPath = MetaZkConfig.getKeeperLeaderLatchPath(clusterId, shardId);
		String leaderElectionID = MetaZkConfig.getKeeperLeaderElectionId(currentKeeperMeta);
		ElectContext ctx = new ElectContext(leaderElectionZKPath, leaderElectionID);
		return leaderElectorManager.createLeaderElector(ctx);
	}

	@Override
	protected void doInitialize() throws Exception {
		super.doInitialize();

		replicationStoreManager.initialize();
		
		String threadPoolName = String.format("keeper:%s", StringUtil.makeSimpleName(clusterId.toString(), shardId.toString()));
		logger.info("[doInitialize][keeper config]{}", keeperConfig);

		clientExecutors = Executors.newSingleThreadExecutor(ClusterShardAwareThreadFactory.create(clusterId, shardId, "RedisClient-" + threadPoolName));
		scheduled = Executors.newScheduledThreadPool(DEFAULT_SCHEDULED_CORE_POOL_SIZE , ClusterShardAwareThreadFactory.create(clusterId, shardId, "sch-" + threadPoolName));
		bossGroup = new NioEventLoopGroup(DEFAULT_BOSS_EVENT_LOOP_SIZE, ClusterShardAwareThreadFactory.create(clusterId, shardId, "boss-" + threadPoolName));
		workerGroup = new NioEventLoopGroup(DEFAULT_KEEPER_WORKER_GROUP_THREAD_COUNT, ClusterShardAwareThreadFactory.create(clusterId, shardId, "work-"+ threadPoolName));
		masterEventLoopGroup = new NioEventLoopGroup(DEFAULT_MASTER_EVENT_LOOP_SIZE, ClusterShardAwareThreadFactory.create(clusterId, shardId, "master-" + threadPoolName));
		rdbOnlyEventLoopGroup = new NioEventLoopGroup(DEFAULT_RDB_EVENT_LOOP_SIZE, ClusterShardAwareThreadFactory.create(clusterId, shardId, "rdbOnly-" + threadPoolName));


		this.resetReplAfterLongTimeDown();
		this.leaderElector = createLeaderElector();
		this.leaderElector.initialize();
	 	this.redisKeeperServerState = initKeeperServerState();
	 	logger.info("[doInitialize]{}", this.redisKeeperServerState.keeperState());

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
	}
	
	@Override
	protected void doStop() throws Exception {
		keeperMonitor.stop();
		clearClients();
		this.leaderElector.stop();
		LifecycleHelper.stopIfPossible(keeperRedisMaster);
		stopServer();
		replicationStoreManager.stop();
		super.doStop();
	}

	private void clearClients() {
		for (Entry<Channel, RedisClient> entry : redisClients.entrySet()) {
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

	@Override
	protected void doDispose() throws Exception {

		LifecycleHelper.disposeIfPossible(keeperRedisMaster);
		this.leaderElector.dispose();
		masterEventLoopGroup.shutdownGracefully();
		rdbOnlyEventLoopGroup.shutdownGracefully();
		bossGroup.shutdownGracefully();
		workerGroup.shutdownGracefully();
		replicationStoreManager.dispose();
		this.scheduled.shutdownNow();
		this.clientExecutors.shutdownNow();
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
					rdbOnlyEventLoopGroup, replicationStoreManager, scheduled, resourceManager);

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
         .handler(new LoggingHandler(LogLevel.INFO))
         .childHandler(new ChannelInitializer<SocketChannel>() {
             @Override
             public void initChannel(SocketChannel ch) throws Exception {
                 ChannelPipeline p = ch.pipeline();
                 p.addLast(new LoggingHandler(LogLevel.DEBUG));
                 p.addLast(new NettySimpleMessageHandler());
                 p.addLast(new NettyMasterHandler(DefaultRedisKeeperServer.this, new CommandHandlerManager(), keeperConfig.getTrafficReportIntervalMillis()));
             }
         });
        serverSocketChannel = (ServerSocketChannel) b.bind(currentKeeperMeta.getPort()).sync().channel();
    }
		

	@Override
	public RedisClient clientConnected(Channel channel) {
		
		RedisClient redisClient = new DefaultRedisClient(channel, this);
		redisClients.put(channel, redisClient);
		
		redisClient.addObserver(new Observer() {
			
			@Override
			public void update(Object args, Observable observable) {
				
				if(args instanceof RedisSlave){
					becomeSlave(((RedisClient)observable).channel(), (RedisSlave)args);
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

		for (Entry<Channel, RedisClient> entry : redisClients.entrySet()) {

			RedisClient redisClient = entry.getValue();
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
		String alert = String.format("FULL(S)->%s[%s,%s]", getRedisMaster().metaInfo(), getClusterId(), getShardId());
		EventMonitor.DEFAULT.logAlertEvent(alert);

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
	public ClusterId getClusterId() {
		return this.clusterId;
	}

	@Override
	public ShardId getShardId() {
		return this.shardId;
	}

	@Override
	public synchronized void setRedisKeeperServerState(final RedisKeeperServerState redisKeeperServerState){
		
		TransactionMonitor transactionMonitor = TransactionMonitor.DEFAULT;
		String name = String.format("%s,%s,%s", clusterId, shardId, redisKeeperServerState);
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
	public void fullSyncToSlave(final RedisSlave redisSlave) throws IOException {
		
		logger.info("[fullSyncToSlave]{}, {}", redisSlave, rdbDumper.get());
		if(rdbDumper.get() == null){
			logger.info("[fullSyncToSlave][dumper null]{}", redisSlave);
			FullSyncListener fullSyncListener = new DefaultFullSyncListener(redisSlave);
			if(!getCurrentReplicationStore().fullSyncIfPossible(fullSyncListener)){
				//go dump rdb
				try{
					dumpNewRdb();
					redisSlave.waitForRdbDumping();
				}catch(AbstractRdbDumperException e){
					logger.error("[fullSyncToSlave]", e);
					if(e.isCancelSlave()){
						logger.info("[fullSyncToSlave][cancel slave]");	
						redisSlave.close();
					}
				}
			}
		}else{
			rdbDumper.get().tryFullSync(redisSlave);
		}
	}
	
	private RdbDumper dumpNewRdb() throws CreateRdbDumperException, SetRdbDumperException {
		
		RdbDumper rdbDumper = keeperRedisMaster.createRdbDumper();
		setRdbDumper(rdbDumper);
		rdbDumper.execute();
		return rdbDumper;
	}

	
	public void setRdbDumper(RdbDumper rdbDumper) throws SetRdbDumperException {
		setRdbDumper(rdbDumper, false);
	}
	

	@Override
	public KeeperInstanceMeta getKeeperInstanceMeta() {
		return new KeeperInstanceMeta(clusterId, shardId, currentKeeperMeta);
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
		
		if(lastDumpTime > 0 && !force && (System.currentTimeMillis() - lastDumpTime < keeperConfig.getRdbDumpMinIntervalMilli())){
			logger.info("[setRdbDumper][too quick]{}", new Date(lastDumpTime));
			throw new SetRdbDumperException(lastDumpTime, keeperConfig.getRdbDumpMinIntervalMilli());
		}
		
		if(rdbDumper.compareAndSet(null, newDumper)){
			lastDumpTime = System.currentTimeMillis();
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
			lastDumpTime = System.currentTimeMillis();
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
					lastDumpTime = 0;
				}
			}
		});
	}

	@Override
	public void clearRdbDumper(RdbDumper oldDumper) {
		
		logger.info("[clearRdbDumper]{}", oldDumper);
		if(!rdbDumper.compareAndSet(oldDumper, null)){
			logger.warn("[clearRdbDumper][current is not request]{}, {}", oldDumper, rdbDumper.get());
		}
	}
	
	public int getRdbDumpTryCount() {
		return rdbDumpTryCount.get();
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
	public void tryConnectMaster() {
		logger.debug("[tryConnectMaster] {}", tryConnectMasterCnt.incrementAndGet());
	}

	@Override
	public int getTryConnectMasterCnt() {
		return tryConnectMasterCnt.get();
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
