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
import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.netty.NettySimpleMessageHandler;
import com.ctrip.xpipe.netty.commands.DefaultNettyClient;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.FixedObjectPool;
import com.ctrip.xpipe.redis.core.protocal.CAPA;
import com.ctrip.xpipe.redis.core.protocal.Psync;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractRedisCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.Replconf;
import com.ctrip.xpipe.redis.core.protocal.cmd.Replconf.ReplConfType;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.proxy.ProxyEnabled;
import com.ctrip.xpipe.redis.core.proxy.ProxyProtocol;
import com.ctrip.xpipe.redis.core.proxy.ProxyResourceManager;
import com.ctrip.xpipe.redis.core.proxy.endpoint.*;
import com.ctrip.xpipe.redis.keeper.RdbDumper;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisMaster;
import com.ctrip.xpipe.redis.keeper.RedisMasterReplication;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.netty.NettySlaveHandler;
import com.ctrip.xpipe.utils.ChannelUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
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

	public static String KEY_REPLICATION_TIMEOUT = "KEY_REPLICATION_TIMEOUT_MILLI";

	public static int DEFAULT_REPLICATION_TIMEOUT_MILLI = Integer.parseInt(System.getProperty(KEY_REPLICATION_TIMEOUT, "60000"));

	protected int masterConnectRetryDelaySeconds = Integer.parseInt(System.getProperty(KEY_MASTER_CONNECT_RETRY_DELAY_SECONDS, "2"));

	protected static int PROXYED_REPLICATION_COMMAND_TIMEOUT_MILLI = 15 * 1000;

	private final int replTimeoutMilli;

	private long repl_transfer_lastio;

	private AtomicReference<RdbDumper> rdbDumper = new AtomicReference<RdbDumper>(null);

	protected FixedObjectPool<NettyClient> clientPool;

	protected ScheduledExecutorService scheduled;

	public static final int REPLCONF_INTERVAL_MILLI = 1000;

	public static final int PSYNC_RETRY_INTERVAL_MILLI = 2000;

	protected RedisMaster redisMaster;

	protected long connectedTime;

	protected Channel masterChannel;

	private NioEventLoopGroup nioEventLoopGroup;

	protected RedisKeeperServer redisKeeperServer;

	protected AtomicReference<Command<?>> currentCommand = new AtomicReference<Command<?>>(null);

	private ProxyEndpointSelector selector;

	private int commandTimeoutMilli;

	private ProxyResourceManager proxyResourceManager;

	public AbstractRedisMasterReplication(RedisKeeperServer redisKeeperServer, RedisMaster redisMaster, NioEventLoopGroup nioEventLoopGroup,
										  ScheduledExecutorService scheduled, int replTimeoutMilli, ProxyResourceManager proxyResourceManager) {

		this.redisKeeperServer = redisKeeperServer;
		this.redisMaster = redisMaster;
		this.nioEventLoopGroup = nioEventLoopGroup;
		this.replTimeoutMilli = replTimeoutMilli;
		this.scheduled = scheduled;
		this.proxyResourceManager = proxyResourceManager;
		this.commandTimeoutMilli = initCommandTimeoutMilli();
	}

	public AbstractRedisMasterReplication(RedisKeeperServer redisKeeperServer, RedisMaster redisMaster, NioEventLoopGroup nioEventLoopGroup,
										  ScheduledExecutorService scheduled, ProxyResourceManager proxyResourceManager) {
		this(redisKeeperServer, redisMaster, nioEventLoopGroup, scheduled, DEFAULT_REPLICATION_TIMEOUT_MILLI, proxyResourceManager);
	}

	public RedisMaster getRedisMaster() {
		return redisMaster;
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
		Endpoint endpoint = redisMaster.masterEndPoint();
		String masterInfo = endpoint.toString();
		if(isMasterConnectThroughProxy()) {
			masterInfo = String.format("endpoint: %s, proxy info: %s", endpoint.toString(),
					((ProxyEnabled)endpoint).getProxyProtocol());
		}
		logger.info("[startReplication]{}", masterInfo);
		connectWithMaster();

	}

	protected void connectWithMaster() {

		if (!(getLifecycleState().isStarting() || getLifecycleState().isStarted())) {
			logger.info("[connectWithMaster][do not connect, is stopped!!]{}", redisMaster.masterEndPoint());
			return;
		}

		Bootstrap b = new Bootstrap();
		b.group(nioEventLoopGroup).channel(NioSocketChannel.class).option(ChannelOption.TCP_NODELAY, true)
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

		doConnect0(b);
	}

	private void doConnect0(Bootstrap b) {
		try {
			doConnect(b);
		} catch (Exception e) {
			logger.error("[doConnect0] ", e);

			scheduled.schedule(new Runnable() {
				@Override
				public void run() {
					try{
						connectWithMaster();
					}catch(Throwable th){
						logger.error("[doConnect0][connectUntilConnected]" + AbstractRedisMasterReplication.this, th);
					}
				}
			}, masterConnectRetryDelaySeconds, TimeUnit.SECONDS);
		}
	}

	protected abstract void doConnect(Bootstrap b);

	protected ChannelFuture tryConnect(Bootstrap b) {
		if(isMasterConnectThroughProxy()) {
			return tryConnectThroughProxy(b);
		} else {
			Endpoint endpoint = redisMaster.masterEndPoint();
			logger.info("[tryConnect][begin]{}", endpoint);
			return b.connect(endpoint.getHost(), endpoint.getPort());
		}
	}

	@VisibleForTesting
	protected boolean isMasterConnectThroughProxy() {
		return redisMaster.masterEndPoint() instanceof ProxyEnabled;
	}

	private ChannelFuture tryConnectThroughProxy(Bootstrap b) {
		ProxyEnabledEndpoint endpoint = (ProxyEnabledEndpoint) redisMaster.masterEndPoint();
		ProxyProtocol protocol = endpoint.getProxyProtocol();
		if(selector == null) {
			selector = proxyResourceManager.createProxyEndpointSelector(protocol);
		}
		ProxyEndpoint nextHop = selector.nextHop();
		logger.info("[tryConnectThroughProxy] connect endpoint: {}", nextHop.getUri());

		return b.connect(nextHop.getHost(), nextHop.getPort());
	}

	@Override
	public void handleResponse(Channel channel, ByteBuf byteBuf) throws XpipeException {

		if(!(getLifecycleState().isStarted() || getLifecycleState().isStarting())){
			throw new RedisMasterReplicationStateException(this,
					String.format("not stated: %s, do not receive message:%d, %s", getLifecycleState().getPhaseName(), byteBuf.readableBytes(), ByteBufUtils.readToString(byteBuf)));
		}

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

		if(isMasterConnectThroughProxy()) {
			ProxyEnabledEndpoint endpoint = (ProxyEnabledEndpoint) redisMaster.masterEndPoint();
			channel.writeAndFlush(endpoint.getProxyProtocol().output());
		}
		
		checkKeeper();

		SequenceCommandChain chain = new SequenceCommandChain(false);
		chain.add(listeningPortCommand());

		// for proxy connect init time
		Replconf capa = new Replconf(clientPool, ReplConfType.CAPA, scheduled, commandTimeoutMilli,
				CAPA.EOF.toString(), CAPA.PSYNC2.toString());
		chain.add(new FailSafeCommandWrapper<>(capa));
		
		try {
			executeCommand(chain).addListener(new CommandFutureListener() {

				@Override
				public void operationComplete(CommandFuture commandFuture) throws Exception {
					if(commandFuture.isSuccess()){
						sendReplicationCommand();
					}else{
						logger.error("[operationComplete][listeningPortCommand] close channel{} and wait for reconnect",
                                ChannelUtil.getDesc(channel), commandFuture.cause());
						channel.close();
					}
				}
			});
		} catch (Exception e) {
			logger.error("[masterConnected]" + channel, e);
		}
	}

	private void checkKeeper() {
		Replconf replconf = new Replconf(clientPool, ReplConfType.KEEPER, scheduled, commandTimeoutMilli);
		executeCommand(replconf).addListener(new CommandFutureListener<Object>() {

			@Override
			public void operationComplete(CommandFuture<Object> commandFuture) throws Exception {
				if(commandFuture.isSuccess()){
					redisMaster.setKeeper();
				}
			}
		});
	}

	private void checkTimeout(final Channel channel) {

		logger.info("[checkTimeout]{} ms, {}", replTimeoutMilli, ChannelUtil.getDesc(channel));
		final ScheduledFuture<?> repliTimeoutCheckFuture = scheduled.scheduleAtFixedRate(new AbstractExceptionLogTask() {

					@Override
					protected void doRun() throws Exception {

						long current = System.currentTimeMillis();
						if ((current - repl_transfer_lastio) >= replTimeoutMilli) {
							logger.info("[doRun][no action with master for a long time, close connection]{}, {}", channel, AbstractRedisMasterReplication.this);
							channel.close();
						}
					}
		}, replTimeoutMilli, replTimeoutMilli, TimeUnit.MILLISECONDS);
		
		channel.closeFuture().addListener(new ChannelFutureListener() {
			
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				
				logger.info("[cancelTimeout]{}ms, {}", replTimeoutMilli, channel);
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

		Replconf replconf = new Replconf(clientPool, ReplConfType.LISTENING_PORT, scheduled, commandTimeoutMilli,
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

		KeeperConfig keeperConfig = redisKeeperServer.getKeeperConfig();
		return new NettySlaveHandler(this, redisKeeperServer, keeperConfig != null? keeperConfig.getTrafficReportIntervalMillis() : KeeperConfig.DEFAULT_TRAFFIC_REPORT_INTERVAL_MILLIS);
	}

	@Override
	protected void doStop() throws Exception {

		stopReplication();
		super.doStop();
	}

	protected void stopReplication() {

		logger.info("[stopReplication]{}", redisMaster.masterEndPoint());
		if (masterChannel != null && masterChannel.isOpen()) {
			logger.info("[stopReplication][doStop]{}", masterChannel);
			masterChannel.close();
		}
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

	private int initCommandTimeoutMilli() {
		if(isMasterConnectThroughProxy()) {
			return AbstractRedisMasterReplication.PROXYED_REPLICATION_COMMAND_TIMEOUT_MILLI;
		}
		return AbstractRedisCommand.DEFAULT_REDIS_COMMAND_TIME_OUT_MILLI;
	}

	@VisibleForTesting
	protected int commandTimeoutMilli() {
		return commandTimeoutMilli;
	}

}
