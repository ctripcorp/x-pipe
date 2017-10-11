package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.command.CommandExecutionException;
import com.ctrip.xpipe.command.FailSafeCommandWrapper;
import com.ctrip.xpipe.command.SequenceCommandChain;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.exception.XpipeException;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.netty.NettySimpleMessageHandler;
import com.ctrip.xpipe.netty.commands.DefaultNettyClient;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.FixedObjectPool;
import com.ctrip.xpipe.redis.core.protocal.CAPA;
import com.ctrip.xpipe.redis.core.protocal.Psync;
import com.ctrip.xpipe.redis.core.protocal.cmd.Replconf;
import com.ctrip.xpipe.redis.core.protocal.cmd.Replconf.ReplConfType;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.keeper.RdbDumper;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisMaster;
import com.ctrip.xpipe.redis.keeper.RedisMasterReplication;
import com.ctrip.xpipe.redis.keeper.netty.NettySlaveHandler;
import com.ctrip.xpipe.utils.ChannelUtil;
import com.ctrip.xpipe.utils.ClusterShardAwareThreadFactory;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author wenchao.meng
 *
 *         Aug 24, 2016
 */
public abstract class AbstractRedisMasterReplication extends AbstractLifecycle implements RedisMasterReplication {

	public static String KEY_MASTER_CONNECT_RETRY_DELAY_SECONDS = "KEY_MASTER_CONNECT_RETRY_DELAY_SECONDS";

	public static String KEY_REPLICATION_TIMEOUT = "KEY_REPLICATION_TIMEOUT";

	public static int DEFAULT_REPLICATION_TIMEOUT = Integer.parseInt(System.getProperty(KEY_REPLICATION_TIMEOUT, "60"));

	private final int replTimeoutSeconds;

	private long repl_transfer_lastio;

	private AtomicReference<RdbDumper> rdbDumper = new AtomicReference<RdbDumper>(null);

	protected FixedObjectPool<NettyClient> clientPool;

	protected ScheduledExecutorService scheduled;

	protected EventLoopGroup slaveEventLoopGroup;

	public static final int REPLCONF_INTERVAL_MILLI = 1000;

	public static final int PSYNC_RETRY_INTERVAL_MILLI = 2000;

	protected RedisMaster redisMaster;

	protected long connectedTime;

	protected Channel masterChannel;

	protected RedisKeeperServer redisKeeperServer;

	protected AtomicReference<Command<?>> currentCommand = new AtomicReference<Command<?>>(null);

	public AbstractRedisMasterReplication(RedisKeeperServer redisKeeperServer, RedisMaster redisMaster,
			ScheduledExecutorService scheduled, int replTimeoutSeconds) {

		this.redisKeeperServer = redisKeeperServer;
		this.redisMaster = redisMaster;
		this.replTimeoutSeconds = replTimeoutSeconds;
		this.scheduled = scheduled;
	}

	public AbstractRedisMasterReplication(RedisKeeperServer redisKeeperServer, RedisMaster redisMaster,
			ScheduledExecutorService scheduled) {
		this(redisKeeperServer, redisMaster, scheduled, DEFAULT_REPLICATION_TIMEOUT);
	}

	public RedisMaster getRedisMaster() {
		return redisMaster;
	}

	@Override
	protected void doInitialize() throws Exception {
		super.doInitialize();
		String threadPoolName = String.format("%s:(%s:%d)", getSimpleName(), redisMaster.masterEndPoint().getHost(), redisMaster.masterEndPoint().getPort()); 
		slaveEventLoopGroup = new NioEventLoopGroup(1, ClusterShardAwareThreadFactory.create(redisKeeperServer.getClusterId(), redisKeeperServer.getShardId(), threadPoolName));

	}

	protected abstract String getSimpleName();

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
		b.group(slaveEventLoopGroup).channel(NioSocketChannel.class).option(ChannelOption.TCP_NODELAY, true)
				.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
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

		repl_transfer_lastio = System.currentTimeMillis();
		clientPool.getObject().handleResponse(channel, byteBuf);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void masterConnected(Channel channel) {

		connectedTime = System.currentTimeMillis();
		this.masterChannel = channel;
		clientPool = new FixedObjectPool<NettyClient>(new DefaultNettyClient(channel));

		checkTimeout(channel);
		
		checkKeeper();

		SequenceCommandChain chain = new SequenceCommandChain(false);
		chain.add(listeningPortCommand());
		chain.add(new FailSafeCommandWrapper<>(new Replconf(clientPool, ReplConfType.CAPA, scheduled, CAPA.EOF.toString(), CAPA.PSYNC2.toString())));
		
		try {
			executeCommand(chain).addListener(new CommandFutureListener() {

				@Override
				public void operationComplete(CommandFuture commandFuture) throws Exception {
					if(commandFuture.isSuccess()){
						sendReplicationCommand();
					}else{
						logger.error("[operationComplete][listeningPortCommand]", commandFuture.cause());
					}
				}
			});
		} catch (Exception e) {
			logger.error("[masterConnected]" + channel, e);
		}
	}

	private void checkKeeper() {
		executeCommand(new Replconf(clientPool, ReplConfType.KEEPER, scheduled)).addListener(new CommandFutureListener<Object>() {

			@Override
			public void operationComplete(CommandFuture<Object> commandFuture) throws Exception {
				if(commandFuture.isSuccess()){
					redisMaster.setKeeper();
				}
			}
		});
	}

	private void checkTimeout(final Channel channel) {

		logger.info("[checkTimeout]{}s, {}", replTimeoutSeconds, ChannelUtil.getDesc(channel));
		final ScheduledFuture<?> repliTimeoutCheckFuture = scheduled.scheduleAtFixedRate(new AbstractExceptionLogTask() {

					@Override
					protected void doRun() throws Exception {

						long current = System.currentTimeMillis();
						if ((current - repl_transfer_lastio) >= replTimeoutSeconds * 1000) {
							logger.info("[doRun][no action with master for a long time, close connection]{}, {}", channel, AbstractRedisMasterReplication.this);
							channel.close();
						}
					}
		}, replTimeoutSeconds, replTimeoutSeconds, TimeUnit.SECONDS);
		
		channel.closeFuture().addListener(new ChannelFutureListener() {
			
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				
				logger.info("[cancelTimeout]{}s, {}", replTimeoutSeconds, channel);
				repliTimeoutCheckFuture.cancel(true);
			}
		});
	}

	@Override
	public void masterDisconntected(Channel channel) {

		logger.info("[masterDisconntected]{}", channel);
		dumpFail(new IOException("master closed:" + channel));
	}

	protected <V> CommandFuture<V> executeCommand(Command<V> command) {

		if (command != null) {
			currentCommand.set(command);
			return command.execute();
		}
		return null;
	}

	private Replconf listeningPortCommand() {

		Replconf replconf = new Replconf(clientPool, ReplConfType.LISTENING_PORT, scheduled,
				String.valueOf(redisKeeperServer.getListeningPort()));
		return replconf;
	}

	protected void sendReplicationCommand() throws CommandExecutionException {
		executeCommand(psyncCommand());
	}

	protected Psync psyncCommand() {

		if (getLifecycleState().isStopping() || getLifecycleState().isStopped()) {
			logger.info("[psyncCommand][stopped]{}", this);
			return null;
		}

		Psync psync = createPsync();
		psync.future().addListener(new CommandFutureListener<Object>() {

			@Override
			public void operationComplete(CommandFuture<Object> commandFuture) throws Exception {

				if (!commandFuture.isSuccess()) {
					logger.error("[operationComplete][psyncCommand][fail]" + AbstractRedisMasterReplication.this, commandFuture.cause());

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
		return new NettySlaveHandler(this, redisKeeperServer, redisKeeperServer.getKeeperConfig().getTrafficReportIntervalMillis());
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

		return String.format("%s(redisMaster:%s, %s)", getClass().getSimpleName(), redisMaster, ChannelUtil.getDesc(masterChannel));
	}

	@Override
	public void onFullSync() {
		doOnFullSync();
	}

	protected abstract void doOnFullSync();

	@Override
	public void reFullSync() {
		doReFullSync();
	}

	protected abstract void doReFullSync();

	@Override
	public void beginWriteRdb(EofType eofType, long masterRdbOffset) throws IOException {

		doBeginWriteRdb(eofType, masterRdbOffset);
		rdbDumper.get().beginReceiveRdbData(masterRdbOffset);
	}

	protected abstract void doBeginWriteRdb(EofType eofType, long masterRdbOffset) throws IOException;

	@Override
	public void endWriteRdb() {

		dumpFinished();
		doEndWriteRdb();
	}

	protected abstract void doEndWriteRdb();

	@Override
	public void onContinue(String requestReplId, String responseReplId) {
		doOnContinue();
	}

	protected abstract void doOnContinue();

	protected void dumpFinished() {
		logger.info("[dumpFinished]{}", this);

		RdbDumper dumper = rdbDumper.get();
		if (dumper != null) {
			rdbDumper.set(null);
			dumper.dumpFinished();
		}
	}

	protected void dumpFail(Throwable th) {

		RdbDumper dumper = rdbDumper.get();
		if (dumper != null) {
			rdbDumper.set(null);
			dumper.dumpFail(th);
		}
	}

	public void setRdbDumper(RdbDumper dumper) {

		if (this.rdbDumper.get() != null) {
			logger.info("[setRdbDumper][replace]{}", this.rdbDumper.get());
		}
		this.rdbDumper.set(dumper);
	}

	public RdbDumper getRdbDumper() {
		return rdbDumper.get();
	}
	
	@Override
	public RedisMaster redisMaster() {
		return redisMaster;
	}
}
