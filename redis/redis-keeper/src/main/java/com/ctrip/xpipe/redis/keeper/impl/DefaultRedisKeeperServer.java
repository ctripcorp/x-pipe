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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.ctrip.xpipe.api.cluster.LeaderElector;
import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.cluster.ElectContext;
import com.ctrip.xpipe.concurrent.NamedThreadFactory;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.netty.NettySimpleMessageHandler;
import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.meta.MetaZkConfig;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerKeeperService;
import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;
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
import com.ctrip.xpipe.redis.keeper.exception.RedisKeeperRuntimeException;
import com.ctrip.xpipe.redis.keeper.exception.RedisSlavePromotionException;
import com.ctrip.xpipe.redis.keeper.handler.CommandHandlerManager;
import com.ctrip.xpipe.redis.keeper.netty.NettyMasterHandler;
import com.ctrip.xpipe.redis.keeper.store.DefaultFullSyncListener;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStoreManager;
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
	
	public DefaultRedisKeeperServer(KeeperMeta currentKeeperMeta, KeeperConfig keeperConfig, File baseDir, 
			MetaServerKeeperService metaService, LeaderElectorManager leaderElectorManager){
		this(currentKeeperMeta, keeperConfig, baseDir, metaService, null, leaderElectorManager);
	}

	public DefaultRedisKeeperServer(KeeperMeta currentKeeperMeta, KeeperConfig keeperConfig, File baseDir, 
			MetaServerKeeperService metaService, 
			ScheduledExecutorService scheduled, 
			LeaderElectorManager leaderElectorManager){
		this.clusterId = currentKeeperMeta.parent().parent().getId();
		this.shardId = currentKeeperMeta.parent().getId();
		this.currentKeeperMeta = currentKeeperMeta;
		this.keeperConfig = keeperConfig;
		this.replicationStoreManager = new DefaultReplicationStoreManager(keeperConfig, clusterId, shardId, currentKeeperMeta.getId(), baseDir);
		this.metaService = metaService;
		this.leaderElectorManager = leaderElectorManager;
		if(scheduled == null){
			scheduled = Executors.newScheduledThreadPool(OsUtils.getCpuCount(), new NamedThreadFactory(clusterId + "-" + shardId));
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
		
		logger.info("[doInitialize][keeper config]{}", keeperConfig);
		bossGroup = new NioEventLoopGroup(1);
		workerGroup = new NioEventLoopGroup();
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
	protected void doDispose() throws Exception {

		LifecycleHelper.disposeIfPossible(keeperRedisMaster);

		this.leaderElector.dispose();
		bossGroup.shutdownGracefully();
		workerGroup.shutdownGracefully();
		super.doDispose();
	}

	
	@Override
	protected void doStart() throws Exception {
		super.doStart();
		
		
		keeperStartTime = System.currentTimeMillis();
		startServer();
		this.leaderElector.start();
	}
	
	@Override
	protected void doStop() throws Exception {
		
		LifecycleHelper.stopIfPossible(keeperRedisMaster);
		
		this.leaderElector.stop();
		
		stopServer();
		super.doStop();
	}
	
	@Override
	public synchronized void reconnectMaster() {
		
		Endpoint target = redisKeeperServerState.getMaster();
		logger.info("[reconnectMaster]{} -> {}", this, target);

		if(keeperRedisMaster != null && target != null){
			Endpoint current = keeperRedisMaster.masterEndPoint();
			if(current.getHost().equals(target.getHost()) && current.getPort() == target.getPort()){
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
	public void beginWriteRdb(long fileSize, long offset) {
		try {
			getReplicationStore().getMetaStore().setKeeperState(redisKeeperServerState.keeperState());
		} catch (IOException e) {
			throw new RedisKeeperRuntimeException("[setRedisKeeperServerState]" + redisKeeperServerState, e);
		}
	}

	@Override
	public void endWriteRdb() {
		
	}

	@Override
	public void reFullSync() {
		for(RedisSlave redisSlave : slaves()){
			try {
				logger.info("[reFullSync][close slave]{}", redisSlave);
				redisSlave.close();
			} catch (IOException e) {
				logger.error("[beginWriteRdb][close slaves]", e);
			}
		}
	}

	@Override
	public void onContinue() {
		try {
			getReplicationStore().getMetaStore().setKeeperState(redisKeeperServerState.keeperState());
		} catch (IOException e) {
			throw new RedisKeeperRuntimeException("[setRedisKeeperServerState]" + redisKeeperServerState, e);
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
	public synchronized void setRedisKeeperServerState(RedisKeeperServerState redisKeeperServerState){
		
		RedisKeeperServerState previous = this.redisKeeperServerState;
		logger.info("[setRedisKeeperServerState]{}, {}->{}", this, previous, redisKeeperServerState);
		this.redisKeeperServerState = redisKeeperServerState;
		notifyObservers(new KeeperServerStateChanged(previous, redisKeeperServerState));
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
		RedisPromotor promotor = new RedisPromotor(this, ip, port);
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
		this.replicationStoreManager.destroy();
	}

	@Override
	public void onFullSync() {
		
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

}
