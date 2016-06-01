package com.ctrip.xpipe.redis.keeper.impl;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.netty.NettySimpleMessageHandler;
import com.ctrip.xpipe.redis.keeper.CLUSTER_ROLE;
import com.ctrip.xpipe.redis.keeper.CommandsListener;
import com.ctrip.xpipe.redis.keeper.KeeperMeta;
import com.ctrip.xpipe.redis.keeper.KeeperRepl;
import com.ctrip.xpipe.redis.keeper.RdbFileListener;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisMaster;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import com.ctrip.xpipe.redis.keeper.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.ReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.entity.Keeper;
import com.ctrip.xpipe.redis.keeper.entity.Redis;
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
	
	private KEEPER_STATE keeperState = KEEPER_STATE.NORMAL;
	
	private CLUSTER_ROLE clusterRole = CLUSTER_ROLE.UNKNOWN;

	/**
	 * when keeper is active, it's redis master, else it's another keeper
	 */
	private RedisMaster keeperRedisMaster;
	
	private long keeperStartTime;
	
	private ReplicationStoreManager replicationStoreManager;

	private KeeperMeta keeperMeta;

    private EventLoopGroup bossGroup = new NioEventLoopGroup(1);
    private EventLoopGroup workerGroup = new NioEventLoopGroup();

	private Map<Channel, RedisClient>  redisClients = new ConcurrentHashMap<Channel, RedisClient>(); 
	
	private ScheduledExecutorService scheduled;
	
	private CommandRequester commandRequester;
	
	private final String clusterId, shardId;
	
	private MetaServiceManager metaServiceManager;
	
	private Keeper activeKeeper, currentKeeper;
	private Redis  clusterRedisMaster;
	
	public DefaultRedisKeeperServer(String clusterId, String shardId, Keeper currentKeeper, ReplicationStoreManager replicationStoreManager, 
			MetaServiceManager metaServiceManager, 
			ScheduledExecutorService scheduled, 
			CommandRequester commandRequester){
		
		this.clusterId = clusterId;
		this.shardId = shardId;
		this.currentKeeper = currentKeeper;
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

	@Override
	protected void doInitialize() throws Exception {
		super.doInitialize();
		metaServiceManager.addObserver(this);
		metaServiceManager.addShard(clusterId, shardId);
	}
	
	@Override
	protected void doDispose() throws Exception {

		metaServiceManager.remoteObserver(this);
		metaServiceManager.removeShard(clusterId, shardId);
		super.doDispose();
	}

	
	@Override
	protected void doStart() throws Exception {
		super.doStart();
		keeperStartTime = System.currentTimeMillis();
		
		startServer();
		redisMaster.startReplication();
	}
	
	@Override
	public void update(Object args, Observable observable) {
		
		if(args instanceof MetaUpdateInfo){
			
			MetaUpdateInfo metaUpdateInfo = (MetaUpdateInfo) args;
			if(!this.clusterId.equals(metaUpdateInfo.getClusterId()) || !this.shardId.equals(metaUpdateInfo.getShardId())){
				return;
			}
			
			Object change = metaUpdateInfo.getInfo();
			if(change instanceof Redis){
				gotRedisMasterInfo((Redis)change);
			}else if(change instanceof Keeper){
				gotActiveKeeperInfo((Keeper)change);
			}else{
				throw new IllegalStateException("unkonw info:" + change);
			}
		}
	}

	private void gotRedisMasterInfo(Redis clusterRedisMaster) {
		if(!clusterRedisMaster.getMaster()){
			throw new IllegalStateException("redis not master:" + clusterRedisMaster);
		}
		
		if(this.clusterRedisMaster == null){
			
			logger.info("[gotRedisMasterInfo][new master]{}", clusterRedisMaster);
			this.clusterRedisMaster  = clusterRedisMaster;
			Endpoint endpoint = new DefaultEndPoint(clusterRedisMaster.getIp(), clusterRedisMaster.getPort());
			this.keeperRedisMaster = new DefaultRedisMaster(this, endpoint, replicationStoreManager, scheduled, commandRequester);
			tryReplicationMaster();
			return;
		}
		//TODO ask keepermaster again about redis master information
		if(!this.clusterRedisMaster.equals(clusterRedisMaster)){
			
			logger.info("[gotRedisMasterInfo][master changed]{} -> {}", this.clusterRedisMaster, clusterRedisMaster);
			this.clusterRedisMaster = clusterRedisMaster;
		}
		
	}

	private void tryReplicationMaster() {
		
		
	}

	private void gotActiveKeeperInfo(Keeper activeKeeper) {
		if(!activeKeeper.isActive()){
			throw new IllegalStateException("keeper not active:" + activeKeeper);
		}
		
		if(this.activeKeeper == null){
			logger.info("[gotActiveKeeperInfo][new activeKeeper]{}", activeKeeper);
			this.activeKeeper = activeKeeper;
			tryReplicationMaster();
			return;
		}
		
		if(!this.activeKeeper.equals(activeKeeper)){
			logger.info("[gotActiveKeeperInfo][activeKeeper changed]{} -> {}", this.activeKeeper, activeKeeper);
			this.activeKeeper = activeKeeper;
		}
	}

	@Override
	protected void doStop() throws Exception {
		
		redisMaster.stopReplication();
		stopServer();
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
        b.bind(keeperMeta.getKeeperPort()).sync();
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
		return "master:" + this.redisMaster;
	}

	@Override
	public String getKeeperRunid() {
		
		return this.keeperMeta.getKeeperRunid();
	}

	
	@Override
	public KeeperRepl getKeeperRepl() {
		
		return new DefaultKeeperRepl(getCurrentReplicationStore());
	}
	
	
	protected ReplicationStore getCurrentReplicationStore(){
		
		try {
			return replicationStoreManager.getCurrent();
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
		info += "run_id:" + keeperMeta.getKeeperRunid() + RedisProtocol.CRLF;
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
				redisMaster.stopReplication();
				break;
			case COMMANDS_SEND_FINISH:
				break;
			case SLAVE_PROMTED:
				SlavePromotionInfo promotionInfo = (SlavePromotionInfo) info;
				this.redisMaster = masterChanged(promotionInfo.getKeeperOffset(), promotionInfo.getNewMasterEndpoint()
						, promotionInfo.getNewMasterRunid(), promotionInfo.getNewMasterReplOffset());
				redisMaster.startReplication();
				break;
			default:
				throw new IllegalStateException("unkonow state:" + keeperState);
		}
		
	}
	
	@Override
	public void setKeeperServerState(KEEPER_STATE keeperState) {
		this.setKeeperServerState(keeperState, null);
	}

	public RedisMaster masterChanged(long keeperOffset, Endpoint newMasterEndpoint, String newMasterRunid, long newMasterReplOffset) {
		
		getCurrentReplicationStore().masterChanged(newMasterEndpoint, newMasterRunid, newMasterReplOffset - keeperOffset);
		return new DefaultRedisMaster(this, newMasterEndpoint, replicationStoreManager, scheduled, commandRequester);
	}

	@Override
	public int getListeningPort() {
		return keeperMeta.getKeeperPort();
	}

	@Override
	public RedisMaster getRedisMaster() {
		return redisMaster;
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

}
