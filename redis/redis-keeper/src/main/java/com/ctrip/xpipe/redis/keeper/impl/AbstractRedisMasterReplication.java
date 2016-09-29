package com.ctrip.xpipe.redis.keeper.impl;



import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.command.CommandExecutionException;
import com.ctrip.xpipe.exception.XpipeException;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.netty.NettySimpleMessageHandler;
import com.ctrip.xpipe.netty.commands.DefaultNettyClient;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.FixedObjectPool;
import com.ctrip.xpipe.redis.core.protocal.cmd.KinfoCommand;
import com.ctrip.xpipe.redis.core.protocal.Psync;
import com.ctrip.xpipe.redis.core.protocal.cmd.Replconf;
import com.ctrip.xpipe.redis.core.protocal.cmd.Replconf.ReplConfType;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreMeta;
import com.ctrip.xpipe.redis.keeper.RdbDumper;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisMaster;
import com.ctrip.xpipe.redis.keeper.RedisMasterReplication;
import com.ctrip.xpipe.redis.keeper.netty.NettySlaveHandler;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
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
 * Aug 24, 2016
 */
public abstract class AbstractRedisMasterReplication extends AbstractLifecycle implements RedisMasterReplication{
	
	private AtomicReference<RdbDumper> rdbDumper = new AtomicReference<RdbDumper>(null);
	
	protected FixedObjectPool<NettyClient> clientPool;

	protected EventLoopGroup slaveEventLoopGroup;
	
	public static final int REPLCONF_INTERVAL_MILLI = 1000;

	public static final int PSYNC_RETRY_INTERVAL_MILLI = 2000;

	protected int masterConnectRetryDelaySeconds = 5;

	protected RedisMaster redisMaster;
	
	protected long connectedTime;

	protected Channel masterChannel;
	
	protected RedisKeeperServer redisKeeperServer;
	
	private ReplicationStoreMeta kinfo; 
	
	protected AtomicReference<Command<?>> currentCommand = new AtomicReference<Command<?>>(null);
		
	public AbstractRedisMasterReplication(RedisKeeperServer redisKeeperServer, RedisMaster redisMaster){
		
		this.redisKeeperServer = redisKeeperServer;
		this.redisMaster = redisMaster;
	}

	public RedisMaster getRedisMaster() {
		return redisMaster;
	}

	
	@Override
	protected void doInitialize() throws Exception {
		super.doInitialize();
		slaveEventLoopGroup = new NioEventLoopGroup();
		
	}
	
	@Override
	protected void doStart() throws Exception {
		super.doStart();
		startReplication();
	}


	public void startReplication() {

		if (this.masterChannel != null && this.masterChannel.isOpen()) {
			logger.warn("[startReplication][channel alive, don't do replication]{}", this.masterChannel);
			return;
		}

		logger.info("[startReplication]{}", redisMaster.masterEndPoint());
		connectWithMaster();

	}

	protected void connectWithMaster() {
		
		if (!(getLifecycleState().isStarting() || getLifecycleState().isStarted())) {
			logger.info("[connectWithMaster][do not connect, is stopped!!]{}", redisMaster.masterEndPoint());
			return;
		}
		
		Bootstrap b = new Bootstrap();
		b.group(slaveEventLoopGroup).channel(NioSocketChannel.class).option(ChannelOption.TCP_NODELAY, true).option(ChannelOption.ALLOCATOR, new PooledByteBufAllocator(true))
		.handler(new ChannelInitializer<SocketChannel>() {
			@Override
			public void initChannel(SocketChannel ch) throws Exception {
				ChannelPipeline p = ch.pipeline();
				p.addLast(new LoggingHandler(LogLevel.DEBUG));
				p.addLast(new NettySimpleMessageHandler());
				p.addLast(createHandler());
			}
		});

		doConnect(b);
	}

	protected abstract void doConnect(Bootstrap b);

	protected ChannelFuture tryConnect(Bootstrap b) {
		
		Endpoint endpoint = redisMaster.masterEndPoint();
		return b.connect(endpoint.getHost(), endpoint.getPort());
	}


	@Override
	public void handleResponse(Channel channel, ByteBuf byteBuf) throws XpipeException {
		clientPool.getObject().handleResponse(channel, byteBuf);
	}


	@Override
	public void masterConnected(Channel channel) {
		
		connectedTime = System.currentTimeMillis();
		this.masterChannel = channel;
		clientPool = new FixedObjectPool<NettyClient>(new DefaultNettyClient(channel));

		try {
			executeCommand(listeningPortCommand());
			sendReplicationCommand();
		} catch (CommandExecutionException e) {
			logger.error("[masterConnected]" + channel, e);
		}
	}
	
	@Override
	public void masterDisconntected(Channel channel) {
		
		dumpFail(new IOException("master closed:" + channel));
	}
	
	protected void executeCommand(Command<?> command){
		
		if(command != null){
			currentCommand.set(command);
			command.execute();
		}
	}
	
	private Replconf listeningPortCommand() throws CommandExecutionException {

		Replconf replconf = new Replconf(clientPool, ReplConfType.LISTENING_PORT, String.valueOf(redisKeeperServer.getListeningPort()));
		return replconf;
	}

	protected void sendReplicationCommand() throws CommandExecutionException {
		
		if (redisKeeperServer.getRedisKeeperServerState().sendKinfo()) {
			executeCommand(kinfoCommand());
		} else {
			executeCommand(psyncCommand());
		}
	}

	protected KinfoCommand kinfoCommand() throws CommandExecutionException {

		KinfoCommand kinfoCommand = new KinfoCommand(clientPool);

		kinfoCommand.future().addListener(new CommandFutureListener<ReplicationStoreMeta>() {

			@Override
			public void operationComplete(CommandFuture<ReplicationStoreMeta> commandFuture) throws Exception {

				try {
					kinfo = commandFuture.get();
					executeCommand(psyncCommand());
				} catch (Exception e) {
					logger.error("[operationComplete][kinfo fail]" + redisMaster, e);
					kinfoFail(e);
				}
			}
		});
		return kinfoCommand;
	}

	protected abstract void kinfoFail(Throwable e);

	protected Psync psyncCommand(){
		
		if(getLifecycleState().isStopping() || getLifecycleState().isStopped()){
			logger.info("[psyncCommand][stopped]{}", this);
			return null;
		}

		Psync psync = createPsync();
		psync.future().addListener(new CommandFutureListener<Object>() {

			@Override
			public void operationComplete(CommandFuture<Object> commandFuture) throws Exception {
				
				if(!commandFuture.isSuccess()){
					logger.error("[operationComplete][psyncCommand][fail]", commandFuture.cause());
					
					dumpFail(commandFuture.cause());
					psyncFail(commandFuture.cause());
				}
			}
		});
		return psync;
	}

	protected abstract Psync createPsync();

	protected abstract void psyncFail(Throwable cause);

	protected ChannelDuplexHandler createHandler() {
		return new NettySlaveHandler(this);
	}

	@Override
	protected void doStop() throws Exception {
		
		stopReplication();
		super.doStop();
	}

	
	protected void stopReplication() {

		logger.info("[stopReplication]{}", redisMaster.masterEndPoint());
		if (masterChannel != null && masterChannel.isOpen()) {
			masterChannel.close();
		}
	}
	
	@Override
	protected void doDispose() throws Exception {
		
		slaveEventLoopGroup.shutdownGracefully();
		super.doDispose();
	}

	@Override
	public String toString() {
		
		return String.format("%s(redisMaster:%s, %s)", getClass().getSimpleName(), redisMaster, masterChannel);
	}

	@Override
	public void onFullSync() {
		doOnFullSync();
	}
	
	protected abstract void doOnFullSync();

	@Override
	public void reFullSync(){
		doReFullSync();
	}
	
	protected abstract void doReFullSync();

	@Override
	public void beginWriteRdb(long fileSize, long masterRdbOffset) throws IOException{
		
		doBeginWriteRdb(fileSize, masterRdbOffset);
		rdbDumper.get().beginReceiveRdbData(masterRdbOffset);
	}
	
	protected abstract void doBeginWriteRdb(long fileSize, long masterRdbOffset) throws IOException;

	@Override
	public void endWriteRdb(){
		
		dumpFinished();
		doEndWriteRdb();
	}
	
	protected abstract void doEndWriteRdb();
	
	@Override
	public void onContinue(){
		doOnContinue();
	}
	
	protected abstract void doOnContinue();
	
	protected void dumpFinished(){
		logger.info("[dumpFinished]{}", this);
		
		RdbDumper dumper = rdbDumper.get();
		if(dumper != null){
			rdbDumper.set(null);
			dumper.dumpFinished();
		}
	}
	protected void dumpFail(Throwable th){
		
		RdbDumper dumper = rdbDumper.get();
		if(dumper != null){
			rdbDumper.set(null);
			dumper.dumpFail(th);
		}
	}
	
	public void setRdbDumper(RdbDumper dumper) {
		
		if(this.rdbDumper.get() != null){
			logger.info("[setRdbDumper][replace]{}", this.rdbDumper.get());
		}
		this.rdbDumper.set(dumper);
	}
	
	public RdbDumper getRdbDumper() {
		return rdbDumper.get();
	}
	
	protected ReplicationStoreMeta getKinfo() {
		return kinfo;
	}
	
	protected void updateKinfo(long rdbLastKeeperOffset){
		kinfo.setRdbLastKeeperOffset(rdbLastKeeperOffset);
	}
	
}
