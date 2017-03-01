package com.ctrip.xpipe.redis.keeper.impl;


import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.ctrip.xpipe.api.cluster.LeaderElector;
import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.monitor.Task;
import com.ctrip.xpipe.api.monitor.TransactionMonitor;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.cluster.ElectContext;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.netty.NettySimpleMessageHandler;
import com.ctrip.xpipe.netty.ChannelTrafficStatisticsHandler;
import com.ctrip.xpipe.observer.NodeAdded;
import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.meta.MetaZkConfig;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerKeeperService;
import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.store.FullSyncListener;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.KeeperRepl;
import com.ctrip.xpipe.redis.keeper.RdbDumper;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServerState;
import com.ctrip.xpipe.redis.keeper.RedisMaster;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.exception.RedisSlavePromotionException;
import com.ctrip.xpipe.redis.keeper.handler.CommandHandlerManager;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
import com.ctrip.xpipe.redis.keeper.monitor.KeepersMonitorManager;
import com.ctrip.xpipe.redis.keeper.netty.NettyMasterHandler;
import com.ctrip.xpipe.redis.keeper.store.DefaultFullSyncListener;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStoreManager;
import com.ctrip.xpipe.utils.ClusterShardAwareThreadFactory;
import com.ctrip.xpipe.utils.OsUtils;

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

/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午2:08:26
 */
public class DefaultRedisKeeperServer extends AbstractRedisServer implements RedisKeeperServer{
	
	/**
	 * when keeper is active, it's redis master, else it's another keeper
	 */
	private volatile RedisMaster keeperRedisMaster;
	
	private long keeperStartTime;
	
	private ReplicationStoreManager replicationStoreManager;

	private ServerSocketChannel serverSocketChannel;
	
    private EventLoopGroup bossGroup ;
    private EventLoopGroup workerGroup;
    
    public static String KEY_DEFAULT_KEEPER_WORKER_GROUP_THREAD_COUNT = "DEFAULT_KEEPER_WORKER_GROUP_THREAD_COUNT";
    public static int DEFAULT_KEEPER_WORKER_GROUP_THREAD_COUNT = Integer.parseInt(System.getProperty(KEY_DEFAULT_KEEPER_WORKER_GROUP_THREAD_COUNT, "5"));   

	private Map<Channel, RedisClient>  redisClients = new ConcurrentHashMap<Channel, RedisClient>();
	
	private ScheduledExecutorService scheduled;
	
	private final String clusterId, shardId;
	
	private volatile RedisKeeperServerState redisKeeperServerState;
	private KeeperConfig keeperConfig; 
	
	private KeeperMeta currentKeeperMeta;
	private LeaderElector leaderElector;

	private LeaderElectorManager leaderElectorManager;
	
	@SuppressWarnings("unused")
	private MetaServerKeeperService metaService;
	
	private volatile AtomicReference<RdbDumper> rdbDumper = new AtomicReference<RdbDumper>(null);
	private long lastDumpTime = -1;
	//for test
	private AtomicInteger  rdbDumpTryCount = new AtomicInteger();
	
	private KeepersMonitorManager keepersMonitorManager;
	private KeeperMonitor keeperMonitor;
	
	public DefaultRedisKeeperServer(KeeperMeta currentKeeperMeta, KeeperConfig keeperConfig, File baseDir, 
			MetaServerKeeperService metaService, LeaderElectorManager leaderElectorManager, KeepersMonitorManager keepersMonitorManager){
		this(currentKeeperMeta, keeperConfig, baseDir, metaService, null, leaderElectorManager, keepersMonitorManager);
	}

	public DefaultRedisKeeperServer(KeeperMeta currentKeeperMeta, KeeperConfig keeperConfig, File baseDir, 
			MetaServerKeeperService metaService, 
			ScheduledExecutorService scheduled, 
			LeaderElectorManager leaderElectorManager,
			KeepersMonitorManager keepersMonitorManager){
		this.clusterId = currentKeeperMeta.parent().parent().getId();
		this.shardId = currentKeeperMeta.parent().getId();
		this.currentKeeperMeta = currentKeeperMeta;
		this.keeperConfig = keeperConfig;
		this.keepersMonitorManager = keepersMonitorManager;
		this.keeperMonitor = keepersMonitorManager.getOrCreate(this);
		this.replicationStoreManager = new DefaultReplicationStoreManager(keeperConfig, clusterId, shardId, currentKeeperMeta.getId(), baseDir, keeperMonitor);
		replicationStoreManager.addObserver(new ReplicationStoreManagerListener());
		this.metaService = metaService;
		this.leaderElectorManager = leaderElectorManager;
		if(scheduled == null){
			scheduled = Executors.newScheduledThreadPool(OsUtils.getCpuCount(), ClusterShardAwareThreadFactory.create(clusterId, shardId, String.format("keeper:%s-%s", clusterId, shardId)));
		}
		this.scheduled = scheduled;
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
		
		String threadPoolName = String.format("keeper:%s-%s", clusterId, shardId); 
		logger.info("[doInitialize][keeper config]{}", keeperConfig);
		bossGroup = new NioEventLoopGroup(1, ClusterShardAwareThreadFactory.create(clusterId, shardId, "boss:" + threadPoolName));
		workerGroup = new NioEventLoopGroup(DEFAULT_KEEPER_WORKER_GROUP_THREAD_COUNT, ClusterShardAwareThreadFactory.create(clusterId, shardId, threadPoolName));
		this.leaderElector = createLeaderElector();
		this.leaderElector.initialize();
	 	this.redisKeeperServerState = initKeeperServerState();
	 	logger.info("[doInitialize]{}", this.redisKeeperServerState.keeperState());

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
		replicationStoreManager.start();;		
		keeperStartTime = System.currentTimeMillis();
		startServer();
		this.leaderElector.start();
	}
	
	@Override
	protected void doStop() throws Exception {
		
		LifecycleHelper.stopIfPossible(keeperRedisMaster);
		this.leaderElector.stop();
		stopServer();
		replicationStoreManager.stop();		
		super.doStop();
	}

	@Override
	protected void doDispose() throws Exception {

		this.scheduled.shutdownNow();
		LifecycleHelper.disposeIfPossible(keeperRedisMaster);
		this.leaderElector.dispose();
		bossGroup.shutdownGracefully();
		workerGroup.shutdownGracefully();
		replicationStoreManager.dispose();
		super.doDispose();
	}

	
	@Override
	public synchronized void reconnectMaster() {
		
		Endpoint target = redisKeeperServerState.getMaster();
		logger.info("[reconnectMaster]{} -> {}", this, target);

		if(keeperRedisMaster != null && target != null){
			Endpoint current = keeperRedisMaster.masterEndPoint();
			if(current != null && current.getHost().equals(target.getHost()) && current.getPort() == target.getPort()){
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

	private void initAndStartMaster(Endpoint target) {
		try {
			this.keeperRedisMaster = new DefaultRedisMaster(this, (DefaultEndPoint)target, replicationStoreManager, scheduled);
			
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
                 p.addLast(new ChannelTrafficStatisticsHandler(keeperConfig.getTrafficReportIntervalMillis(), TimeUnit.MILLISECONDS));
                 p.addLast(new NettySimpleMessageHandler());
                 p.addLast(new NettyMasterHandler(DefaultRedisKeeperServer.this, new CommandHandlerManager()));
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
					logger.info("[update][redis client become slave]" + observable);
					redisClients.put(((RedisClient)observable).channel(), (RedisSlave)args);
				}
			}
		});
		
		return redisClient;
	}

	@Override
	public void clientDisConnected(Channel channel) {
		
		redisClients.remove(channel);
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
	public void beginWriteRdb(EofType eofType, long offset) {
	}

	@Override
	public void endWriteRdb() {
		
	}

	@Override
	public void reFullSync() {

		closeSlaves("reFullSync");
	}

	@Override
	public void onFullSync() {
		
	}

	private void closeSlaves(String reason) {
		
		for(RedisSlave redisSlave : slaves()){
			try {
				logger.info("[{}][close slave]{}", reason, redisSlave);
				redisSlave.close();
			} catch (IOException e) {
				logger.error("[beginWriteRdb][close slaves]", e);
			}
		}
	}

	@Override
	public void onContinue(String requestReplId, String responseReplId) {
		
		if(!requestReplId.equals(responseReplId)){
			closeSlaves(String.format("replid changed: %s->%s", requestReplId, responseReplId));
		}
	}
	
	@Override
	public String getClusterId() {
		return this.clusterId;
	}

	@Override
	public String getShardId() {
		return this.shardId;
	}

	@Override
	public synchronized void setRedisKeeperServerState(final RedisKeeperServerState redisKeeperServerState){
		
		TransactionMonitor transactionMonitor = TransactionMonitor.DEFAULT;
		String name = String.format("%s,%s,%s", clusterId, shardId, redisKeeperServerState);
		transactionMonitor.logTransactionSwallowException("setRedisKeeperServerState", name, new Task() {
			
			@Override
			public void go() throws Throwable {
				
				RedisKeeperServerState previous = DefaultRedisKeeperServer.this.redisKeeperServerState;
				logger.info("[setRedisKeeperServerState]{}, {}->{}", this, previous, redisKeeperServerState);
				DefaultRedisKeeperServer.this.redisKeeperServerState = redisKeeperServerState;
				notifyObservers(new KeeperServerStateChanged(previous, redisKeeperServerState));
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
		}else{
			throw new SetRdbDumperException(olRdbDumper);
		}
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
	public RdbDumper rdbDumper() {
		return rdbDumper.get();
	}
}
