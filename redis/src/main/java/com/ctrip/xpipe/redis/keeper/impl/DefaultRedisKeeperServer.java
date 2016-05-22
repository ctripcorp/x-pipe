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
import com.ctrip.xpipe.netty.NettySimpleMessageHandler;
import com.ctrip.xpipe.redis.keeper.CommandsListener;
import com.ctrip.xpipe.redis.keeper.RdbFileListener;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisMaster;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import com.ctrip.xpipe.redis.keeper.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.handler.CommandHandlerManager;
import com.ctrip.xpipe.redis.keeper.handler.SlaveOfCommandHandler.SlavePromotionInfo;
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
public class DefaultRedisKeeperServer extends AbstractRedisServer implements RedisKeeperServer{
	
	private KEEPER_STATE keeperState = KEEPER_STATE.NORMAL;

	private RedisMaster redisMaster;
	private String keeperRunid;
	
	private long keeperStartTime;
	
	private ReplicationStore replicationStore;

	private int keeperPort;

    private EventLoopGroup bossGroup = new NioEventLoopGroup(1);
    private EventLoopGroup workerGroup = new NioEventLoopGroup();

	private Map<Channel, RedisClient>  redisClients = new ConcurrentHashMap<Channel, RedisClient>(); 
	
	private ScheduledExecutorService scheduled;
	
	private CommandRequester commandRequester;
	
	public DefaultRedisKeeperServer(Endpoint masterEndpoint, ReplicationStore replicationStore, String keeperRunid, int keeperPort) {
		this(masterEndpoint, replicationStore, keeperRunid, keeperPort, null, null);
	}
	
	public DefaultRedisKeeperServer(Endpoint masterEndpoint, ReplicationStore replicationStore, String keeperRunid, int keeperPort, ScheduledExecutorService scheduled, CommandRequester commandRequester) {
		
		this.replicationStore = replicationStore;
		this.keeperRunid = keeperRunid;
		this.keeperPort = keeperPort;
		if(scheduled == null){
			scheduled = Executors.newScheduledThreadPool(OsUtils.getCpuCount(), new NamedThreadFactory(masterEndpoint.toString()));
		}
		this.scheduled = scheduled;
		
		if(commandRequester == null){
			commandRequester = new DefaultCommandRequester(this.scheduled);
		}
		this.commandRequester = commandRequester;
		this.redisMaster = new DefaultRedisMaster(this, masterEndpoint, replicationStore, this.scheduled, this.commandRequester);
	}

	
	@Override
	protected void doStart() throws Exception {
		super.doStart();
		keeperStartTime = System.currentTimeMillis();
		
		startServer();
		redisMaster.startReplication();
		
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
        b.bind(keeperPort).sync();
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
		return "master:" + this.redisMaster + ",masterRunId:" + replicationStore.getMasterRunid() + ",offset:" + replicationStore.endOffset();
	}

	@Override
	public String getKeeperRunid() {
		
		return this.keeperRunid;
	}

	@Override
	public long getBeginReploffset() {
		return replicationStore.beginOffset();
	}

	@Override
	public long getEndReploffset() {
		return replicationStore.endOffset();
	}

	@Override
	public void addCommandsListener(Long offset, CommandsListener commandsListener) {
		try {
			replicationStore.addCommandsListener(offset, commandsListener);
		} catch (IOException e) {
			logger.error("[addCommandsListener]" + offset +"," + commandsListener, e);
		}
	}

	@Override
	public void readRdbFile(RdbFileListener rdbFileListener) throws IOException {
		replicationStore.readRdbFile(rdbFileListener);
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
		info += "run_id:" + keeperRunid + RedisProtocol.CRLF;
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
	   return replicationStore;
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
		
		replicationStore.masterChanged(newMasterEndpoint, newMasterRunid, newMasterReplOffset - keeperOffset);
		return new DefaultRedisMaster(this, newMasterEndpoint, replicationStore, scheduled, commandRequester);
	}

	@Override
	public int getListeningPort() {
		return this.keeperPort;
	}

	@Override
	public RedisMaster getRedisMaster() {
		return redisMaster;
	}

}
