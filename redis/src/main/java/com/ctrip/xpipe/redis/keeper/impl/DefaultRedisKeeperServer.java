package com.ctrip.xpipe.redis.keeper.impl;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.ctrip.xpipe.api.lifecycle.LifecycleState;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.exception.XpipeRuntimeException;

import com.ctrip.xpipe.netty.NettySimpleMessageHandler;
import com.ctrip.xpipe.redis.keeper.CLUSTER_ROLE;
import com.ctrip.xpipe.redis.keeper.CommandsListener;
import com.ctrip.xpipe.redis.keeper.KeeperRepl;
import com.ctrip.xpipe.redis.keeper.RdbFileListener;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisMaster;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import com.ctrip.xpipe.redis.keeper.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.ReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.ReplicationStoreMeta;
import com.ctrip.xpipe.redis.keeper.cluster.ElectContext;
import com.ctrip.xpipe.redis.keeper.cluster.LeaderElector;
import com.ctrip.xpipe.redis.keeper.config.DefaultKeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.entity.KeeperMeta;
import com.ctrip.xpipe.redis.keeper.entity.RedisMeta;
import com.ctrip.xpipe.redis.keeper.handler.CommandHandlerManager;
import com.ctrip.xpipe.redis.keeper.handler.SlaveOfCommandHandler.SlavePromotionInfo;
import com.ctrip.xpipe.redis.keeper.meta.DefaultMetaServiceManager.MetaUpdateInfo;
import com.ctrip.xpipe.redis.keeper.meta.MetaServiceManager;
import com.ctrip.xpipe.redis.keeper.netty.NettyMasterHandler;
import com.ctrip.xpipe.redis.protocal.CommandRequester;
import com.ctrip.xpipe.redis.protocal.RedisProtocol;
import com.ctrip.xpipe.redis.protocal.cmd.DefaultCommandRequester;
import com.ctrip.xpipe.thread.NamedThreadFactory;
import com.ctrip.xpipe.utils.OsUtils;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午2:08:26
 */
public class DefaultRedisKeeperServer extends AbstractRedisServer implements RedisKeeperServer, Observer{
	
	public static String BACKUP_REPLICATION_STORE_REDIS_MASTER_META_NAME = "BACKUP_REDIS_MASTER"; 
	
	private KEEPER_STATE keeperState = KEEPER_STATE.NORMAL;
	
	private CLUSTER_ROLE clusterRole = CLUSTER_ROLE.UNKNOWN;

	/**
	 * when keeper is active, it's redis master, else it's another keeper
	 */
	private RedisMaster keeperRedisMaster;
	
	private long keeperStartTime;
	
	private ReplicationStoreManager replicationStoreManager;

    private EventLoopGroup bossGroup = new NioEventLoopGroup(1);
    private EventLoopGroup workerGroup = new NioEventLoopGroup();

	private Map<Channel, RedisClient>  redisClients = new ConcurrentHashMap<Channel, RedisClient>(); 
	
	private ScheduledExecutorService scheduled;
	
	private CommandRequester commandRequester;
	
	private final String clusterId, shardId;
	
	private MetaServiceManager metaServiceManager;
	
	private KeeperMeta activeKeeperMeta, currentKeeperMeta;
	private RedisMeta  clusterRedisMasterMeta;
	
	private LeaderElector leaderElector;

	public DefaultRedisKeeperServer(String clusterId, String shardId, KeeperMeta currentKeeperMeta, ReplicationStoreManager replicationStoreManager, 
			MetaServiceManager metaServiceManager){
		this(clusterId, shardId, currentKeeperMeta, replicationStoreManager, metaServiceManager, null, null);
	}

	public DefaultRedisKeeperServer(String clusterId, String shardId, KeeperMeta currentKeeperMeta, ReplicationStoreManager replicationStoreManager, 
			MetaServiceManager metaServiceManager, 
			ScheduledExecutorService scheduled, 
			CommandRequester commandRequester){
		this.clusterId = clusterId;
		this.shardId = shardId;
		this.currentKeeperMeta = currentKeeperMeta;
		this.replicationStoreManager = replicationStoreManager;
		this.metaServiceManager = metaServiceManager;
		if(scheduled == null){
			scheduled = Executors.newScheduledThreadPool(OsUtils.getCpuCount(), new NamedThreadFactory(clusterId + "-" + shardId));
		}
		this.scheduled = scheduled;
		if(commandRequester == null){
			commandRequester = new DefaultCommandRequester(this.scheduled);
		}
		this.commandRequester = commandRequester;
		
	}
	
	private LeaderElector createLeaderElector(){
		
		KeeperConfig config = new DefaultKeeperConfig();
		String leaderElectionZKPath = String.format("%s/%s/%s", config.getZkLeaderLatchRootPath(), clusterId, shardId);
		String leaderElectionID = String.format("%s:%s:%s", currentKeeperMeta.getIp(), currentKeeperMeta.getPort(), currentKeeperMeta.getId());
		ElectContext ctx = new ElectContext(leaderElectionZKPath, leaderElectionID);
		LeaderElector elector = new LeaderElector(config, ctx);
		return elector;
	}

	@Override
	protected void doInitialize() throws Exception {
		super.doInitialize();
		
		this.leaderElector = createLeaderElector();
		this.leaderElector.initialize();
		metaServiceManager.addObserver(this);
		metaServiceManager.addShard(clusterId, shardId);
	}
	
	@Override
	protected void doDispose() throws Exception {

		if(this.keeperRedisMaster != null){
			this.keeperRedisMaster.dispose();
		}
		
		metaServiceManager.remoteObserver(this);
		metaServiceManager.removeShard(clusterId, shardId);
		
		this.leaderElector.dispose();
		super.doDispose();
	}

	
	@Override
	protected void doStart() throws Exception {
		super.doStart();
		keeperStartTime = System.currentTimeMillis();
		this.leaderElector.start();
		
		startServer();
		tryReplicationMaster();
	}
	
	@Override
	public void update(Object args, Observable observable) {
		
		if(args instanceof MetaUpdateInfo){
			
			MetaUpdateInfo metaUpdateInfo = (MetaUpdateInfo) args;
			if(!this.clusterId.equals(metaUpdateInfo.getClusterId()) || !this.shardId.equals(metaUpdateInfo.getShardId())){
				return;
			}
			
			Object change = metaUpdateInfo.getInfo();
			if(change instanceof RedisMeta){
				setRedisMasterMeta((RedisMeta)change);
			}else if(change instanceof KeeperMeta){
				setActiveKeeperMeta((KeeperMeta)change);
			}else{
				throw new IllegalStateException("unkonw info:" + change);
			}
		}
	}

	private void setActiveKeeperMeta(KeeperMeta activeKeeperMeta) {
		
		if(this.activeKeeperMeta != null && this.activeKeeperMeta.equals(activeKeeperMeta)){
			return;
		}
		
		if(this.activeKeeperMeta == null){
			logger.info("[gotActiveKeeperInfo][new activeKeeperMeta]{}", activeKeeperMeta);
		}else{
			logger.info("[gotActiveKeeperInfo][activeKeeperMeta changed]{} -> {}", this.activeKeeperMeta, activeKeeperMeta);
		}
		
		this.activeKeeperMeta = activeKeeperMeta;
		
		if(this.activeKeeperMeta.getIp().equals(currentKeeperMeta.getIp()) && this.activeKeeperMeta.getPort().equals(currentKeeperMeta.getPort())){
			setClusterRole(CLUSTER_ROLE.ACTIVE);
		}else{
			setClusterRole(CLUSTER_ROLE.BACKUP);
		}
	}
		

	private void setRedisMasterMeta(RedisMeta clusterRedisMasterMeta) {
		
		if(this.clusterRedisMasterMeta != null && this.clusterRedisMasterMeta.equals(clusterRedisMasterMeta)){
			return;
		}
		
		RedisMeta old = this.clusterRedisMasterMeta;
		this.clusterRedisMasterMeta  = clusterRedisMasterMeta;
		if(old == null){
			logger.info("[gotRedisMasterInfo][new master]{}", clusterRedisMasterMeta);
			tryReplicationMaster();
		}else{
			//TODO ask keepermaster again about redis master information
			logger.info("[gotRedisMasterInfo][master changed]{} -> {}", old, clusterRedisMasterMeta);
			
		}
	}

	private void tryReplicationMaster() {
		
		LifecycleState lifecycleState = getLifecycleState();
		if(lifecycleState.isStarting() || lifecycleState.isStarted()){
			if(clusterRole == CLUSTER_ROLE.UNKNOWN){
				logger.info("[tryReplicationMaster][role unknown]{}", this);
				return;
			}
			doReplicationMaster();
		}else{
			logger.info("[tryReplicationMaster][not started yet]{}", this);
		}
	}

	private synchronized void doReplicationMaster() {

		if(keeperRedisMaster != null && keeperRedisMaster.getLifecycleState().isStarted()){
			logger.info("[doReplicationMaster][already started]{}", this);
			return;
		}

		DefaultEndPoint endpoint = null;
		
		switch(clusterRole){
			case ACTIVE:
				if(clusterRedisMasterMeta == null){
					logger.info("[doReplicationMaster][clusterRedisMasterMeta not found]{}", this);
					return;
				}
				endpoint = new DefaultEndPoint(clusterRedisMasterMeta.getIp(), clusterRedisMasterMeta.getPort());
				break;
			case BACKUP:
				endpoint = new DefaultEndPoint(activeKeeperMeta.getIp(), activeKeeperMeta.getPort());
				break;
			case UNKNOWN:
				throw new IllegalStateException("clusterRole unknown, can not replication." + this);
		}
		
		try {
			this.keeperRedisMaster = new DefaultRedisMaster(this, endpoint, replicationStoreManager, scheduled, commandRequester);
			this.keeperRedisMaster.initialize();
			this.keeperRedisMaster.start();
		} catch (Exception e) {
			logger.error("[doReplicationMaster]" + this + "," + keeperRedisMaster, e);
		}
	}

	protected void setClusterRole(final CLUSTER_ROLE clusterRole){

		CLUSTER_ROLE previous = this.clusterRole;
		
		switch(previous){
			case ACTIVE:
				if(clusterRole == CLUSTER_ROLE.BACKUP){
					prepareFromActiveToBackup();
					this.clusterRole = clusterRole;
				}else if(clusterRole == CLUSTER_ROLE.UNKNOWN){
					throw new IllegalStateException("can not change state from backuo to unknown");
				}
				break;
			case BACKUP:
				if(clusterRole == CLUSTER_ROLE.ACTIVE){
					prepareFromBackupToActive();
					this.clusterRole = clusterRole;
					doReplicationMaster();
				}else if(clusterRole == CLUSTER_ROLE.UNKNOWN){
					throw new IllegalStateException("can not change state from backuo to unknown");
				}
				break;
			case UNKNOWN:
				this.clusterRole = clusterRole;
				tryReplicationMaster();
				break;
			default:
				throw new IllegalStateException("unknown cluster role:" + clusterRole);
		}
		
		if(previous != this.clusterRole){
			notifyObservers(new ClusterRoleChanged(previous, this.clusterRole));
		}
	}
	
	private void prepareFromBackupToActive() {
		
		try {
			
			if(keeperRedisMaster != null){
				keeperRedisMaster.stop();
			}
			logger.info("[fromBackupToActive]{}", this);
			ReplicationStore replicationStore = getCurrentReplicationStore();
			replicationStore.changeMetaTo(BACKUP_REPLICATION_STORE_REDIS_MASTER_META_NAME);
		} catch (IOException e) {
			logger.error("[fromBackupToActive][failed]", e);
		} catch (Exception e) {
			logger.error("[fromBackupToActive][failed]", e);
		}
	}
	
	private void prepareFromActiveToBackup() {
		//TODO
		logger.info("[fromActiveToBackup]{}", this);
		
	}

	@Override
	protected void doStop() throws Exception {
		
		if(keeperRedisMaster != null){
			keeperRedisMaster.stop();
		}
		stopServer();
		this.leaderElector.stop();
		
		super.doStop();
	}
	
	

	private void stopServer() {
		
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
		
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
        b.bind(currentKeeperMeta.getPort()).sync();
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
		return "master:" + this.keeperRedisMaster;
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
			ReplicationStore replicationStore = replicationStoreManager.getCurrent(); 
			if(replicationStore == null){
				replicationStore = replicationStoreManager.create();
			}
			return replicationStore;
		} catch (IOException e) {
			logger.error("[getCurrentReplicationStore]" + this, e);
			throw new XpipeRuntimeException("[getCurrentReplicationStore]" + this, e);
		}
	}


	@Override
	public void addCommandsListener(Long offset, CommandsListener commandsListener) {
		try {
			getCurrentReplicationStore().addCommandsListener(offset, commandsListener);
		} catch (IOException e) {
			logger.error("[addCommandsListener]" + offset +"," + commandsListener, e);
		}
	}

	@Override
	public void readRdbFile(RdbFileListener rdbFileListener) throws IOException {
		getCurrentReplicationStore().readRdbFile(rdbFileListener);
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

	@Override
	public CommandRequester getCommandRequester() {
		return commandRequester;
	}
	
   public ReplicationStore getReplicationStore() {
	   return getCurrentReplicationStore();
   }

	@Override
	public void setKeeperServerState(KEEPER_STATE keeperState, Object info) {
		
		@SuppressWarnings("unused")
		KEEPER_STATE oldState = this.keeperState;
		this.keeperState = keeperState;
		
		logger.info("[setKeeperServerState]{},{}" ,keeperState, info);

		switch(keeperState){
			case NORMAL:
				break;
			case BEGIN_PROMOTE_SLAVE:
				try {
					keeperRedisMaster.stop();
				} catch (Exception e) {
					logger.error("[setKeeperServerState]" + keeperState + "," + this, e);
				}
				break;
			case COMMANDS_SEND_FINISH:
				break;
			case SLAVE_PROMTED:
				try {
					SlavePromotionInfo promotionInfo = (SlavePromotionInfo) info;
					this.keeperRedisMaster = masterChanged(promotionInfo.getKeeperOffset(), promotionInfo.getNewMasterEndpoint()
							, promotionInfo.getNewMasterRunid(), promotionInfo.getNewMasterReplOffset());
					keeperRedisMaster.initialize();
					keeperRedisMaster.start();
				} catch (Exception e) {
					logger.error("[setKeeperServerState]" + keeperState + "," + this, e);
				}
				break;
			default:
				throw new IllegalStateException("unkonow state:" + keeperState);
		}
		
	}
	
	@Override
	public void setKeeperServerState(KEEPER_STATE keeperState) {
		this.setKeeperServerState(keeperState, null);
	}

	public RedisMaster masterChanged(long keeperOffset, DefaultEndPoint newMasterEndpoint, String newMasterRunid, long newMasterReplOffset) {
		
		ReplicationStore replicationStore = getCurrentReplicationStore();
		ReplicationStoreMeta meta = replicationStore.getReplicationStoreMeta();
		long delta = (meta.getKeeperBeginOffset() - meta.getBeginOffset()) + newMasterReplOffset - keeperOffset;
		replicationStore.masterChanged(newMasterEndpoint, newMasterRunid, delta);
		
		RedisMeta redisMeta = new RedisMeta();
		redisMeta.setIp(newMasterEndpoint.getHost());
		redisMeta.setPort(newMasterEndpoint.getPort());
		redisMeta.setMaster(true);
		
		this.setRedisMasterMeta(redisMeta);

		return new DefaultRedisMaster(this, newMasterEndpoint, replicationStoreManager, scheduled, commandRequester);
	}

	@Override
	public int getListeningPort() {
		return currentKeeperMeta.getPort();
	}

	@Override
	public RedisMaster getRedisMaster() {
		return keeperRedisMaster;
	}

	@Override
	public void beginWriteRdb() {
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
	public CLUSTER_ROLE getClusterRole() {
		return this.clusterRole;
	}

	@Override
	public void onContinue() {
	}
	
	public class ClusterRoleChanged{
		
		private final CLUSTER_ROLE previous, current;
		public ClusterRoleChanged(CLUSTER_ROLE previous, CLUSTER_ROLE current) {
			this.previous = previous;
			this.current = current;
		}
		public CLUSTER_ROLE getPrevious() {
			return previous;
		}
		public CLUSTER_ROLE getCurrent() {
			return current;
		}
		
		@Override
		public String toString() {
			return previous + "->" + current;
		}
	}

}
