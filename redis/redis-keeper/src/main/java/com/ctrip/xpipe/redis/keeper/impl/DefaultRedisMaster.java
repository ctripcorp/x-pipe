package com.ctrip.xpipe.redis.keeper.impl;


import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.server.PARTIAL_STATE;
import com.ctrip.xpipe.command.CommandExecutionException;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.exception.XpipeException;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.netty.NettySimpleMessageHandler;
import com.ctrip.xpipe.netty.commands.DefaultNettyClient;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.FixedObjectPool;
import com.ctrip.xpipe.redis.core.protocal.cmd.KinfoCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.Psync;
import com.ctrip.xpipe.redis.core.protocal.cmd.Replconf;
import com.ctrip.xpipe.redis.core.protocal.cmd.Replconf.ReplConfType;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreMeta;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisMaster;
import com.ctrip.xpipe.redis.keeper.netty.NettySlaveHandler;

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

	private ReplicationStoreManager replicationStoreManager;

	private DefaultEndPoint endpoint;
	
	private ScheduledExecutorService scheduled;
	private EventLoopGroup slaveEventLoopGroup = new NioEventLoopGroup();

	private Channel masterChannel;
	private ScheduledFuture<?> replConfFuture;
	private long connectedTime;
	private FixedObjectPool<NettyClient>  clientPool;
	
	public DefaultRedisMaster(RedisKeeperServer redisKeeperServer, DefaultEndPoint endpoint, ReplicationStoreManager replicationStoreManager, ScheduledExecutorService scheduled){
		
		this.redisKeeperServer = redisKeeperServer;
		this.replicationStoreManager = replicationStoreManager;
		this.endpoint = endpoint;
		this.scheduled = scheduled;
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

		try{
			ChannelFuture f = b.connect(endpoint.getHost(), endpoint.getPort());
	        f.sync();
	        return;
		}catch(Throwable th){
			logger.error("[connectMaster][fail]" + endpoint, th);
		}
		
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
	public void handleResponse(Channel channel, ByteBuf byteBuf) throws XpipeException {
		clientPool.getObject().handleResponse(byteBuf);
	}

	@Override
	public void masterDisconntected(Channel channel) {
				
		long interval = System.currentTimeMillis() - connectedTime;
		long scheduleTime = masterConnectRetryDelaySeconds*1000 - interval;
		if(scheduleTime < 0){
			scheduleTime = 0;
		}
		scheduled.schedule(new Runnable() {
			
			@Override
			public void run() {
				connectWithMaster();
			}
		}, scheduleTime, TimeUnit.MILLISECONDS);
	}

	@Override
	public void masterConnected(Channel channel) {
		
		connectedTime = System.currentTimeMillis();
		this.masterChannel = channel;
		clientPool = new FixedObjectPool<NettyClient>(new DefaultNettyClient(channel));
		
		try {
			listeningPortCommand();
			if(redisKeeperServer.getRedisKeeperServerState().sendKinfo()){
				kinfoCommand();
			}else{
				psyncCommand();
			}
		} catch (CommandExecutionException e) {
			logger.error("[masterConnected]" + channel, e);
		}
	}

	private void kinfoCommand() throws CommandExecutionException {
		
		KinfoCommand kinfoCommand = new KinfoCommand(clientPool);
		
		kinfoCommand.execute().addListener(new CommandFutureListener<ReplicationStoreMeta>() {
			
			@Override
			public void operationComplete(CommandFuture<ReplicationStoreMeta> commandFuture) throws Exception {

				try{
					ReplicationStoreMeta meta = commandFuture.get();
					try{
						replicationStoreManager.getCurrent().saveMeta(ReplicationStore.BACKUP_REPLICATION_STORE_REDIS_MASTER_META_NAME, meta);
						psyncCommand();
					} catch (IOException e1) {
							logger.error("[onComplete]"+ commandFuture, e1);
					}
				}catch(Exception e){
					scheduled.schedule(new Runnable() {
						
						@Override
						public void run() {
							try {
								kinfoCommand();
							} catch (CommandExecutionException e) {
								logger.error("[run]" + clientPool.getObject(), e);
							}
						}
					}, 1, TimeUnit.SECONDS);
				}
			}
		});
	}

	private void psyncCommand() throws CommandExecutionException{
		
		Psync psync = new Psync(clientPool, endpoint, redisKeeperServer.getCurrentKeeperMeta(), replicationStoreManager);
		psync.addPsyncObserver(this);
		psync.addPsyncObserver(redisKeeperServer);
		psync.execute();
	}

	private void listeningPortCommand() throws CommandExecutionException{
		
		Replconf replconf = new Replconf(clientPool, ReplConfType.LISTENING_PORT, String.valueOf(redisKeeperServer.getListeningPort())); 
		replconf.execute();
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
					Command<Object> command = new Replconf(clientPool, ReplConfType.ACK, String.valueOf(getCurrentReplicationStore().endOffset()));
					command.execute();
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
		redisKeeperServer.getRedisKeeperServerState().initPromotionState();
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
		redisKeeperServer.getRedisKeeperServerState().initPromotionState();
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
