package com.ctrip.xpipe.redis.keeper.impl;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.exception.XpipeException;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.netty.NettySimpleMessageHandler;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisMaster;
import com.ctrip.xpipe.redis.keeper.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.ReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer.KEEPER_STATE;
import com.ctrip.xpipe.redis.keeper.netty.NettySlaveHandler;
import com.ctrip.xpipe.redis.protocal.Command;
import com.ctrip.xpipe.redis.protocal.CommandRequester;
import com.ctrip.xpipe.redis.protocal.cmd.Psync;
import com.ctrip.xpipe.redis.protocal.cmd.Replconf;
import com.ctrip.xpipe.redis.protocal.cmd.Replconf.ReplConfType;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;


/**
 * @author wenchao.meng
 *
 * May 22, 2016 6:36:21 PM
 */
public class DefaultRedisMaster implements RedisMaster{
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	public static final int REPLCONF_INTERVAL_MILLI = 1000;
	
	private RedisKeeperServer redisKeeperServer;
	
	private int masterConnectRetryDelaySeconds = 5;
	private int retry = 3;
	private ReplicationStoreManager replicationStoreManager;
	private Endpoint endpoint;
	private ScheduledExecutorService scheduled;
	private CommandRequester commandRequester;
	private EventLoopGroup slaveEventLoopGroup = new NioEventLoopGroup();

	private Channel masterChannel;

	private volatile boolean stop = false;
	
	public DefaultRedisMaster(RedisKeeperServer redisKeeperServer, Endpoint endpoint, ReplicationStoreManager replicationStoreManager, ScheduledExecutorService scheduled, CommandRequester commandRequester){
		
		this.redisKeeperServer = redisKeeperServer;
		this.replicationStoreManager = replicationStoreManager;
		this.endpoint = endpoint;
		this.scheduled = scheduled;
		this.commandRequester = commandRequester;
	}

	@Override
	public void startReplication() {
		
		if(this.masterChannel != null && this.masterChannel.isOpen()){
			logger.warn("[startReplication][channel alive, don't do replication]{}", this.masterChannel);
			return;
		}
		
		connectWithMaster();
		
	}

	@Override
	public void stopReplication() {
		
		this.stop = true; 
		if(masterChannel != null && masterChannel.isOpen()){
			masterChannel.disconnect();
		}
	}


	private void connectWithMaster() {
		
		if(stop){
			logger.info("[connectWithMaster][do not connect, is stopped!!]{}", endpoint);
			return;
		}
		
        Bootstrap b = new Bootstrap();
        b.group(slaveEventLoopGroup)
         .channel(NioSocketChannel.class)
         .option(ChannelOption.TCP_NODELAY, true)
         .handler(new ChannelInitializer<SocketChannel>() {
             @Override
             public void initChannel(SocketChannel ch) throws Exception {
                 ChannelPipeline p = ch.pipeline();
                 p.addLast(new LoggingHandler(LogLevel.DEBUG));
                 p.addLast(new NettySimpleMessageHandler());
                 p.addLast(new NettySlaveHandler(DefaultRedisMaster.this));
             }
         });

        int i = 0;
		for(; i < retry ; i++){
			try{
				ChannelFuture f = b.connect(endpoint.getHost(), endpoint.getPort());
		        f.sync();
		        break;
			}catch(Throwable th){
				logger.error("[connectMaster][fail]" + endpoint, th);
			}
		}
		
		if(i == retry){
			
			if(logger.isInfoEnabled()){
				logger.info("[connectMaster][fail, retry after "  + masterConnectRetryDelaySeconds + " seconds]" + endpoint);
			}
			scheduled.schedule(new Runnable() {
				@Override
				public void run() {
					connectWithMaster();
				}
			}, masterConnectRetryDelaySeconds, TimeUnit.SECONDS);
		}
		
		
	}

	@Override
	public void beginWriteRdb() {
		
		getCurrentReplicationStore().setMasterAddress(endpoint);
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
	public void endWriteRdb() {
		scheduleReplconf();
	}

	@Override
	public void handleResponse(Channel channel, ByteBuf byteBuf) throws XpipeException {
		commandRequester.handleResponse(channel, byteBuf);
	}

	@Override
	public void masterDisconntected(Channel channel) {
		
		commandRequester.connectionClosed(channel);
		connectWithMaster();
	}

	@Override
	public void masterConnected(Channel channel) {
		
		this.masterChannel = channel;
		commandRequester.request(channel, listeningPortCommand());
		commandRequester.request(channel, psyncCommand());
		
		redisKeeperServer.setKeeperServerState(KEEPER_STATE.NORMAL);
	}

	private Command psyncCommand(){
		
		Psync psync = new Psync(replicationStoreManager);
		psync.addPsyncObserver(this);
		psync.addPsyncObserver(redisKeeperServer);
		return psync;
	}

	private Command listeningPortCommand(){
		
		return new Replconf(ReplConfType.LISTENING_PORT, String.valueOf(redisKeeperServer.getListeningPort()));
	}
	
	private void scheduleReplconf() {
		
		if(logger.isInfoEnabled()){
			logger.info("[scheduleReplconf]" + this);
		}
		
		scheduled.scheduleWithFixedDelay(new Runnable() {
			
			@Override
			public void run() {
				try{
					Command command = new Replconf(ReplConfType.ACK, String.valueOf(getCurrentReplicationStore().endOffset()));
					masterChannel.writeAndFlush(command.request());
				}catch(Throwable th){
					logger.error("[run][send replack error]" + DefaultRedisMaster.this, th);
				}
			}
		}, REPLCONF_INTERVAL_MILLI, REPLCONF_INTERVAL_MILLI, TimeUnit.MILLISECONDS);
	}


	@Override
	public String toString() {
		return endpoint.toString();
	}

	@Override
	public void reFullSync() {
		
	}

}
