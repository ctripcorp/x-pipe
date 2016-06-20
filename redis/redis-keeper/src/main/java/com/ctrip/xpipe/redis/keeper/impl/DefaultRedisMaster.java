package com.ctrip.xpipe.redis.keeper.impl;


import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.server.PARTIAL_STATE;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.exception.XpipeException;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.netty.NettySimpleMessageHandler;
import com.ctrip.xpipe.payload.ByteArrayOutputStreamPayload;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisMaster;
import com.ctrip.xpipe.redis.keeper.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.ReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.ReplicationStoreMeta;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer.PROMOTION_STATE;
import com.ctrip.xpipe.redis.keeper.netty.NettySlaveHandler;
import com.ctrip.xpipe.redis.keeper.protocal.CmdContext;
import com.ctrip.xpipe.redis.keeper.protocal.Command;
import com.ctrip.xpipe.redis.keeper.protocal.CommandRequester;
import com.ctrip.xpipe.redis.keeper.protocal.RequestResponseCommandListener;
import com.ctrip.xpipe.redis.keeper.protocal.cmd.KinfoCommand;
import com.ctrip.xpipe.redis.keeper.protocal.cmd.Psync;
import com.ctrip.xpipe.redis.keeper.protocal.cmd.Replconf;
import com.ctrip.xpipe.redis.keeper.protocal.cmd.Replconf.ReplConfType;

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
public class DefaultRedisMaster extends AbstractLifecycle implements RedisMaster{
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	private volatile PARTIAL_STATE partialState = PARTIAL_STATE.UNKNOWN;
	
	public static final int REPLCONF_INTERVAL_MILLI = 1000;
	
	private RedisKeeperServer redisKeeperServer;
	
	private int masterConnectRetryDelaySeconds = 5;
	private int retry = 3;

	private ReplicationStoreManager replicationStoreManager;

	private DefaultEndPoint endpoint;
	
	private ScheduledExecutorService scheduled;
	private CommandRequester commandRequester;
	private EventLoopGroup slaveEventLoopGroup = new NioEventLoopGroup();

	private Channel masterChannel;
	private ScheduledFuture<?> replConfFuture; 
	
	public DefaultRedisMaster(RedisKeeperServer redisKeeperServer, DefaultEndPoint endpoint, ReplicationStoreManager replicationStoreManager, ScheduledExecutorService scheduled, CommandRequester commandRequester){
		
		this.redisKeeperServer = redisKeeperServer;
		this.replicationStoreManager = replicationStoreManager;
		this.endpoint = endpoint;
		this.scheduled = scheduled;
		this.commandRequester = commandRequester;
	}

	@Override
	protected void doStart() throws Exception {
		super.doStart();
		startReplication();
	}
	
	public void startReplication() {
		
		if(this.masterChannel != null && this.masterChannel.isOpen()){
			logger.warn("[startReplication][channel alive, don't do replication]{}", this.masterChannel);
			return;
		} 
		
		logger.info("[startReplication]{}", this.endpoint);
		connectWithMaster();
		
	}

	@Override
	protected void doStop() throws Exception {
		super.doStop();
		stopReplication();
	}
	

	public void stopReplication() {
		
		logger.info("[stopReplication]{}", this.endpoint);
		
		if(masterChannel != null && masterChannel.isOpen()){
			masterChannel.disconnect();
		}
		if(replConfFuture != null){
			replConfFuture.cancel(true);
			replConfFuture = null;
		}
	}


	private void connectWithMaster() {
		
		if(!(getLifecycleState().isStarting()  || getLifecycleState().isStarted())){
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
		if(redisKeeperServer.getRedisKeeperServerState().sendKinfo()){
			commandRequester.request(channel, kinfoCommand());
		}else{
			commandRequester.request(channel, psyncCommand());
		}
	}

	private Command kinfoCommand() {
		
		KinfoCommand kinfoCommand = new KinfoCommand();
		kinfoCommand.setCommandListener(new RequestResponseCommandListener() {
			
			@Override
			public void onComplete(CmdContext cmdContext, Object data, Exception e) {
				
				if(e != null){
					logger.error("[onComplete]" + data, e);
					cmdContext.schedule(TimeUnit.SECONDS, 1, kinfoCommand());
					return;
				}
				
				ByteArrayOutputStreamPayload payload = (ByteArrayOutputStreamPayload) data;
				String buff = new String(payload.getBytes(), Codec.defaultCharset);
				
				logger.info("[onComplete]{}", buff);
				
				ReplicationStoreMeta meta = null;
				try{
					meta = JSON.parseObject(buff, ReplicationStoreMeta.class);
				}catch(Exception e1){
					logger.error("[onComplete]" + cmdContext + "," + buff, e1);
				}
				
				if(meta != null && meta.getMasterRunid() != null){
					try {
						getCurrentReplicationStore().saveMeta(DefaultRedisKeeperServer.BACKUP_REPLICATION_STORE_REDIS_MASTER_META_NAME, meta);
						cmdContext.sendCommand(psyncCommand());
					} catch (IOException e1) {
						logger.error("[onComplete]" + cmdContext + "," + buff, e1);
						throw new XpipeRuntimeException("[kinfo][cmd error]" + buff, e1);
					}
					
				}else{
					cmdContext.schedule(TimeUnit.SECONDS, 1, kinfoCommand());
				}
			}
		});
		
		return kinfoCommand;
	}

	private Command psyncCommand(){
		
		Psync psync = new Psync(redisKeeperServer.getCurrentKeeperMeta(), replicationStoreManager);
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
		
		replConfFuture = scheduled.scheduleWithFixedDelay(new Runnable() {
			
			@Override
			public void run() {
				try{
					logger.debug("[run][send ack]{}", masterChannel);
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
		redisKeeperServer.getRedisKeeperServerState().setPromotionState(PROMOTION_STATE.NORMAL);
	}

	@Override
	public void beginWriteRdb() {
		
		partialState = PARTIAL_STATE.FULL;
		getCurrentReplicationStore().setMasterAddress(endpoint);
	}

	@Override
	public void endWriteRdb() {
		scheduleReplconf();
	}

	@Override
	public void onContinue() {
		partialState = PARTIAL_STATE.PARTIAL;
		redisKeeperServer.getRedisKeeperServerState().setPromotionState(PROMOTION_STATE.NORMAL);
		scheduleReplconf();
	}

	@Override
	public Endpoint masterEndPoint() {
		return this.endpoint;
	}

	@Override
	public PARTIAL_STATE partialState() {
		
		return partialState;
	}

}
