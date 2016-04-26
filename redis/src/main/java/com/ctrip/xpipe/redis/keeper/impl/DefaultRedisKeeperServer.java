package com.ctrip.xpipe.redis.keeper.impl;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.netty.NettySimpleMessageHandler;
import com.ctrip.xpipe.redis.keeper.CommandsListener;
import com.ctrip.xpipe.redis.keeper.RdbFile;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.handler.CommandHandlerManager;
import com.ctrip.xpipe.redis.keeper.netty.NettyMasterHandler;
import com.ctrip.xpipe.redis.keeper.netty.NettySlaveHandler;
import com.ctrip.xpipe.redis.protocal.Command;
import com.ctrip.xpipe.redis.protocal.cmd.CompositeCommand;
import com.ctrip.xpipe.redis.protocal.cmd.Psync;
import com.ctrip.xpipe.redis.protocal.cmd.Replconf;
import com.ctrip.xpipe.redis.protocal.cmd.Replconf.ReplConfType;
import com.ctrip.xpipe.thread.NamedThreadFactory;
import com.ctrip.xpipe.utils.CpuUtils;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午2:08:26
 */
public class DefaultRedisKeeperServer extends AbstractRedisServer implements RedisKeeperServer{
	
	public static final int REPLCONF_INTERVAL_MILLI = 1000;

	private Endpoint masterEndpoint;
	private String keeperRunid;
	
	private ReplicationStore replicationStore;

	private Channel slave;
	private EventLoopGroup slaveEventLoopGroup;

	private int retry = 3;
	private int keeperPort;

    EventLoopGroup bossGroup = new NioEventLoopGroup(1);
    EventLoopGroup workerGroup = new NioEventLoopGroup();

	private Map<Channel, RedisClient>  redisClients = new ConcurrentHashMap<Channel, RedisClient>(); 
	
	private ScheduledExecutorService scheduled;
	
	public DefaultRedisKeeperServer(Endpoint masterEndpoint, ReplicationStore replicationStore, String keeperRunid, int keeperPort) {
		this(masterEndpoint, replicationStore, keeperRunid, keeperPort, null, 3);
	}
	
	public DefaultRedisKeeperServer(Endpoint masterEndpoint, ReplicationStore replicationStore, String keeperRunid, int keeperPort, ScheduledExecutorService scheduled, int retry) {

		this.masterEndpoint = masterEndpoint;
		this.replicationStore = replicationStore;
		this.keeperRunid = keeperRunid;
		this.retry = retry;
		this.keeperPort = keeperPort;
		this.scheduled = scheduled;
		if(scheduled == null){
			this.scheduled = Executors.newScheduledThreadPool(CpuUtils.getCpuCount(), new NamedThreadFactory(masterEndpoint.toString()));
		}
	}

	
	@Override
	protected void doStart() throws Exception {
		super.doStart();
		startServer();
		connectMaster();
		
	}
	
	@Override
	protected void doStop() throws Exception {
		super.doStop();
		stopServer();
		disConnectWithMaster();
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
                 p.addLast(new LoggingHandler(LogLevel.INFO));
                 p.addLast(new NettySimpleMessageHandler());
                 p.addLast(new NettyMasterHandler(DefaultRedisKeeperServer.this, new CommandHandlerManager()));
             }
         });
        b.bind(keeperPort).sync();
    }
		
		
	private void connectMaster() {

        slaveEventLoopGroup = new NioEventLoopGroup();
        Bootstrap b = new Bootstrap();
        b.group(slaveEventLoopGroup)
         .channel(NioSocketChannel.class)
         .option(ChannelOption.TCP_NODELAY, true)
         .handler(new ChannelInitializer<SocketChannel>() {
             @Override
             public void initChannel(SocketChannel ch) throws Exception {
                 ChannelPipeline p = ch.pipeline();
                 p.addLast(new NettySimpleMessageHandler());
                 p.addLast(new NettySlaveHandler(DefaultRedisKeeperServer.this));
             }
         });

		for(int i=0; i < retry ; i++){
			try{
				ChannelFuture f = b.connect(masterEndpoint.getHost(), masterEndpoint.getPort());
		        f.sync();
		        break;
			}catch(Throwable th){
				logger.error("[connectMaster][fail]" + masterEndpoint, th);
			}
		}
	}

	private void disConnectWithMaster() {
		slaveEventLoopGroup.shutdownGracefully();
	}
	
	private void scheduleReplconf() {
		
		if(logger.isInfoEnabled()){
			logger.info("[scheduleReplconf]" + this);
		}
		
		scheduled.scheduleWithFixedDelay(new Runnable() {
			
			@Override
			public void run() {
				try{
					Command command = new Replconf(ReplConfType.ACK, String.valueOf(replicationStore.endOffset()), slave);
					command.request();
				}catch(Throwable th){
					logger.error("[run][send replack error]" + DefaultRedisKeeperServer.this, th);
				}
			}
		}, REPLCONF_INTERVAL_MILLI, REPLCONF_INTERVAL_MILLI, TimeUnit.MILLISECONDS);
	}

	private Command psyncCommand(){
		
		Psync psync = null;
		if(replicationStore.getMasterRunid() == null){
			psync = new Psync(slave, replicationStore);
		}else{
			psync = new Psync(slave, replicationStore.getMasterRunid(), replicationStore.endOffset() + 1, replicationStore);
		}
		psync.addPsyncObserver(this);
		return psync;
	}

	private Command listeningPortCommand(){
		
		return new Replconf(ReplConfType.LISTENING_PORT, String.valueOf(keeperPort), slave);
	}


	@Override
	public void beginWriteRdb() {
		
	}

	@Override
	public void endWriteRdb() {
		scheduleReplconf();
	}

	@Override
	public Command slaveConnected(Channel channel) {
		
		this.slave = channel;
		return new CompositeCommand(listeningPortCommand(), psyncCommand());
	}

	@Override
	public void slaveDisconntected(Channel channel) {
		connectMaster();
	}

	@Override
	public RedisClient clientConnected(Channel channel) {
		
		RedisClient redisClient = new DefaultRedisClient(channel, this);
		redisClients.put(channel, redisClient);
		return redisClient;
	}

	@Override
	public void clientDisConnected(Channel channel) {
		
		redisClients.remove(channel);
	}

	@Override
	public String toString() {
		return "master:" + this.masterEndpoint + ",masterRunId:" + replicationStore.getMasterRunid() + ",offset:" + replicationStore.endOffset();
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
	      // TODO Auto-generated catch block
	      e.printStackTrace();
      }
	}

	@Override
	public RdbFile getRdbFile() throws IOException {
		return replicationStore.getRdbFile();
	}
}
