package com.ctrip.xpipe.redis.keeper.impl;




import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.ctrip.xpipe.api.cluster.LeaderElector;
import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.cluster.ElectContext;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.netty.NettySimpleMessageHandler;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.meta.MetaZkConfig;
import com.ctrip.xpipe.redis.core.protocal.CommandRequester;
import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;
import com.ctrip.xpipe.redis.core.protocal.cmd.DefaultCommandRequester;
import com.ctrip.xpipe.redis.core.store.RdbFileListener;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.KeeperRepl;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServerState;
import com.ctrip.xpipe.redis.keeper.RedisMaster;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import com.ctrip.xpipe.redis.keeper.exception.RedisSlavePromotionException;
import com.ctrip.xpipe.redis.keeper.handler.CommandHandlerManager;
import com.ctrip.xpipe.redis.keeper.meta.MetaServiceManager;
import com.ctrip.xpipe.redis.keeper.netty.NettyMasterHandler;
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
public class DefaultRedisKeeperServer extends AbstractRedisServer implements RedisKeeperServer{
	
	public static String BACKUP_REPLICATION_STORE_REDIS_MASTER_META_NAME = "BACKUP_REDIS_MASTER"; 
		
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
	
	private volatile RedisKeeperServerState redisKeeperServerState;
	
	private KeeperMeta currentKeeperMeta;
	private LeaderElector leaderElector;

	private LeaderElectorManager leaderElectorManager;

	public DefaultRedisKeeperServer(KeeperMeta currentKeeperMeta, ReplicationStoreManager replicationStoreManager, 
			MetaServiceManager metaServiceManager, LeaderElectorManager leaderElectorManager){
		this(currentKeeperMeta, replicationStoreManager, metaServiceManager, null, null, leaderElectorManager);
	}

	public DefaultRedisKeeperServer(KeeperMeta currentKeeperMeta, ReplicationStoreManager replicationStoreManager, 
			MetaServiceManager metaServiceManager, 
			ScheduledExecutorService scheduled, 
			CommandRequester commandRequester,
			LeaderElectorManager leaderElectorManager){
		this.clusterId = currentKeeperMeta.parent().parent().getId();
		this.shardId = currentKeeperMeta.parent().getId();
		this.currentKeeperMeta = currentKeeperMeta;
		this.replicationStoreManager = replicationStoreManager;
		this.metaServiceManager = metaServiceManager;
		this.leaderElectorManager = leaderElectorManager;
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
		
		String leaderElectionZKPath = MetaZkConfig.getKeeperLeaderLatchPath(clusterId, shardId);
		String leaderElectionID = MetaZkConfig.getKeeperLeaderElectionId(currentKeeperMeta);
		ElectContext ctx = new ElectContext(leaderElectionZKPath, leaderElectionID);
		return leaderElectorManager.createLeaderElector(ctx);
	}

	@Override
	protected void doInitialize() throws Exception {
		super.doInitialize();
		
		this.leaderElector = createLeaderElector();
		this.leaderElector.initialize();
	 	this.redisKeeperServerState = new RedisKeeperServerStateUnknown(this); 
		metaServiceManager.add(this);

	}
	
	@Override
	protected void doDispose() throws Exception {

		if(this.keeperRedisMaster != null){
			this.keeperRedisMaster.dispose();
		}
		
		metaServiceManager.remove(this);
		this.leaderElector.dispose();
		super.doDispose();
	}

	
	@Override
	protected void doStart() throws Exception {
		super.doStart();
		
		keeperStartTime = System.currentTimeMillis();
		this.leaderElector.start();
		startServer();
	}
	
	@Override
	protected void doStop() throws Exception {
		
		if(keeperRedisMaster != null){
			keeperRedisMaster.stop();
		}
		
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
			this.keeperRedisMaster = new DefaultRedisMaster(this, (DefaultEndPoint)target, replicationStoreManager, scheduled, commandRequester);
			this.keeperRedisMaster.initialize();
			this.keeperRedisMaster.start();
		} catch (Exception e) {
			logger.error("[doReplicationMaster]" + this + "," + keeperRedisMaster, e);
		}
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
	public int getListeningPort() {
		return currentKeeperMeta.getPort();
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
	public void onContinue() {
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
	public void setRedisKeeperServerState(RedisKeeperServerState redisKeeperServerState) {
		
		RedisKeeperServerState previous = this.redisKeeperServerState;
		
		this.redisKeeperServerState = redisKeeperServerState;
		
		notifyObservers(new KeeperServerStateChanged(previous, redisKeeperServerState));
	}

	@Override
	public KeeperMeta getCurrentKeeperMeta() {
		return this.currentKeeperMeta;
	}

	@Override
	public void stopAndDisposeMaster() {

		if(this.keeperRedisMaster != null){
			try {
				this.keeperRedisMaster.stop();
				this.keeperRedisMaster.dispose();
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
	public void update(Object args, Observable observable) {
		redisKeeperServerState.update(args, observable);
	}

	@Override
	public RedisMaster getRedisMaster() {
		return keeperRedisMaster;
	}

	@Override
	public void promoteSlave(String ip, int port) throws RedisSlavePromotionException {
		
		logger.info("[promoteSlave]{}:{}", ip, port);
		RedisSlavePromotor promotor = new RedisSlavePromotor(this, ip, port);
		promotor.promote();
	}

}
