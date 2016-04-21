package com.ctrip.xpipe.redis.keeper.impl;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.netty.NettySimpleMessageHandler;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.netty.NettySlaveHandler;
import com.ctrip.xpipe.redis.protocal.Command;
import com.ctrip.xpipe.redis.protocal.cmd.CompositeCommand;
import com.ctrip.xpipe.redis.protocal.cmd.Psync;
import com.ctrip.xpipe.redis.protocal.cmd.Replconf;
import com.ctrip.xpipe.redis.protocal.cmd.Replconf.ReplConfType;
import com.ctrip.xpipe.thread.NamedThreadFactory;
import com.ctrip.xpipe.utils.CpuUtils;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午2:08:26
 */
public class DefaultRedisKeeperServer extends AbstractRedisServer implements RedisKeeperServer{
	
	public static final int REPLCONF_INTERVAL_MILLI = 1000;

	private Endpoint masterEndpoint;
	private ReplicationStore replicationStore;

	private Channel slave;
	private EventLoopGroup slaveEventLoopGroup;

	private int retry = 3;
	private int keeperPort;

	private ScheduledExecutorService scheduled;
	
	public DefaultRedisKeeperServer(Endpoint masterEndpoint, ReplicationStore replicationStore, int keeperPort) {
		this(masterEndpoint, replicationStore, keeperPort, null, 3);
	}
	
	public DefaultRedisKeeperServer(Endpoint masterEndpoint, ReplicationStore replicationStore, int keeperPort, ScheduledExecutorService scheduled, int retry) {

		this.masterEndpoint = masterEndpoint;
		this.replicationStore = replicationStore;
		this.retry = retry;
		this.keeperPort = keeperPort;
		this.scheduled = scheduled;
		if(scheduled == null){
			this.scheduled = Executors.newScheduledThreadPool(CpuUtils.getCpuCount(), new NamedThreadFactory(masterEndpoint.toString()));
		}
	}

	
	@Override
	protected void doStart() {
		super.doStart();
		connectMaster();
	}
	
	@Override
	protected void doStop() {
		super.doStop();
		disConnectWithMaster();
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

	@Override
	public long getReploffset() {
		return replicationStore.endOffset();
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
	public void clientConnected(Channel channel) {
		
	}

	@Override
	public void clientDisConnected(Channel channel) {
		
	}

	@Override
	public String toString() {
		return "master:" + this.masterEndpoint + ",masterRunId:" + replicationStore.getMasterRunid() + ",offset:" + replicationStore.endOffset();
	}
}
