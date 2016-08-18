package com.ctrip.xpipe.redis.keeper.impl;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

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
import com.ctrip.xpipe.netty.AbstractNettyHandler;
import com.ctrip.xpipe.netty.ByteBufReadAction;
import com.ctrip.xpipe.netty.NettySimpleMessageHandler;
import com.ctrip.xpipe.netty.commands.DefaultNettyClient;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.FixedObjectPool;
import com.ctrip.xpipe.redis.core.protocal.PsyncObserver;
import com.ctrip.xpipe.redis.core.protocal.cmd.KinfoCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.Psync;
import com.ctrip.xpipe.redis.core.protocal.cmd.RdbOnlyPsync;
import com.ctrip.xpipe.redis.core.protocal.cmd.Replconf;
import com.ctrip.xpipe.redis.core.protocal.cmd.Replconf.ReplConfType;
import com.ctrip.xpipe.redis.core.store.FullSyncListener;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreMeta;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisMaster;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import com.ctrip.xpipe.redis.keeper.netty.NettySlaveHandler;
import com.ctrip.xpipe.redis.keeper.store.DefaultFullSyncListener;
import com.ctrip.xpipe.redis.keeper.store.RdbOnlyReplicationStore;
import com.google.common.util.concurrent.SettableFuture;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelOutboundHandler;
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
 *         May 22, 2016 6:36:21 PM
 */
public class DefaultRedisMaster extends AbstractLifecycle implements RedisMaster {

	protected Logger logger = LoggerFactory.getLogger(getClass());

	private volatile PARTIAL_STATE partialState = PARTIAL_STATE.UNKNOWN;

	public static final int REPLCONF_INTERVAL_MILLI = 1000;

	public static final int PSYNC_RETRY_INTERVAL_MILLI = 2000;

	// to avoid catch exception, \n should be identical on target platforms
	private static byte[] NEW_LINE = "\n".getBytes();

	private RedisKeeperServer redisKeeperServer;

	private int masterConnectRetryDelaySeconds = 5;

	private ReplicationStoreManager replicationStoreManager;

	private DefaultEndPoint endpoint;

	private ScheduledExecutorService scheduled;
	private EventLoopGroup slaveEventLoopGroup = new NioEventLoopGroup();

	private Channel masterChannel;
	private ScheduledFuture<?> replConfFuture;
	private long connectedTime;
	private FixedObjectPool<NettyClient> clientPool;

	public DefaultRedisMaster(RedisKeeperServer redisKeeperServer, DefaultEndPoint endpoint, ReplicationStoreManager replicationStoreManager,
			ScheduledExecutorService scheduled) {

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

		if (this.masterChannel != null && this.masterChannel.isOpen()) {
			logger.warn("[startReplication][channel alive, don't do replication]{}", this.masterChannel);
			return;
		}

		logger.info("[startReplication]{}", this.endpoint);
		connectWithMaster(new NettySlaveHandler(DefaultRedisMaster.this), true);

	}

	@Override
	protected void doStop() throws Exception {
		super.doStop();
		stopReplication();
	}

	public void stopReplication() {

		logger.info("[stopReplication]{}", this.endpoint);

		if (masterChannel != null && masterChannel.isOpen()) {
			masterChannel.disconnect();
		}
		if (replConfFuture != null) {
			replConfFuture.cancel(true);
			replConfFuture = null;
		}
	}

	private void connectWithMaster(final ChannelOutboundHandler handler, final boolean asyncReconnect) {
		Bootstrap b = new Bootstrap();
		b.group(slaveEventLoopGroup).channel(NioSocketChannel.class).option(ChannelOption.TCP_NODELAY, true).handler(new ChannelInitializer<SocketChannel>() {
			@Override
			public void initChannel(SocketChannel ch) throws Exception {
				ChannelPipeline p = ch.pipeline();
				p.addLast(new LoggingHandler(LogLevel.DEBUG));
				p.addLast(new NettySimpleMessageHandler());
				p.addLast(handler);
			}
		});

		connectUntilConnected(b, asyncReconnect);
		// TODO close Bootstrap
	}

	private void connectUntilConnected(final Bootstrap b, final boolean asyncReconnect) {
		if (tryConnect(b)) {
			return;
		}

		if (asyncReconnect) {
			scheduled.schedule(new Runnable() {
				@Override
				public void run() {
					connectUntilConnected(b, asyncReconnect);
				}
			}, masterConnectRetryDelaySeconds, TimeUnit.SECONDS);
		} else {
			while (true) {
				try {
					TimeUnit.SECONDS.sleep(masterConnectRetryDelaySeconds);
				} catch (InterruptedException e) {
				}
				if (tryConnect(b)) {
					break;
				}
			}
		}
	}

	private boolean tryConnect(Bootstrap b) {
		if (!(getLifecycleState().isStarting() || getLifecycleState().isStarted())) {
			logger.info("[connectWithMaster][do not connect, is stopped!!]{}", endpoint);
			return true;
		}

		try {
			ChannelFuture f = b.connect(endpoint.getHost(), endpoint.getPort());
			f.sync();
			return true;
		} catch (Throwable th) {
			logger.error("[connectMaster][fail]" + endpoint, th);
		}

		if (logger.isInfoEnabled()) {
			logger.info("[connectMaster][fail, retry after " + masterConnectRetryDelaySeconds + " seconds]" + endpoint);
		}
		return false;
	}

	protected ReplicationStore getCurrentReplicationStore() {

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
		clientPool.getObject().handleResponse(channel, byteBuf);
	}

	@Override
	public void masterDisconntected(Channel channel) {

		long interval = System.currentTimeMillis() - connectedTime;
		long scheduleTime = masterConnectRetryDelaySeconds * 1000 - interval;
		if (scheduleTime < 0) {
			scheduleTime = 0;
		}
		scheduled.schedule(new Runnable() {

			@Override
			public void run() {
				connectWithMaster(new NettySlaveHandler(DefaultRedisMaster.this), true);
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
			sendReplicationCommand();
		} catch (CommandExecutionException e) {
			logger.error("[masterConnected]" + channel, e);
		}
	}

	private void sendReplicationCommand() throws CommandExecutionException {
		
		if (redisKeeperServer.getRedisKeeperServerState().sendKinfo()) {
			kinfoCommand();
		} else {
			psyncCommand();
		}
	}

	private void kinfoCommand() throws CommandExecutionException {

		KinfoCommand kinfoCommand = new KinfoCommand(clientPool);

		// TODO execute in this thread?
		kinfoCommand.execute().addListener(new CommandFutureListener<ReplicationStoreMeta>() {

			@Override
			public void operationComplete(CommandFuture<ReplicationStoreMeta> commandFuture) throws Exception {

				try {
					ReplicationStoreMeta meta = commandFuture.get();
					try {
						logger.info("[operationComplete][meta got, save]{}", meta);
						getCurrentReplicationStore().getMetaStore().saveMeta(ReplicationStore.BACKUP_REPLICATION_STORE_REDIS_MASTER_META_NAME, meta);
						psyncCommand();
					} catch (IOException e1) {
						logger.error("[onComplete]" + commandFuture, e1);
					}
				} catch (Exception e) {
					logger.error("[operationComplete][retry kinfo]" + DefaultRedisMaster.this, e);
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

	private void psyncCommand(){
		
		if(getLifecycleState().isStopping() || getLifecycleState().isStopped()){
			logger.info("[psyncCommand][stopped]{}", this);
			return;
		}

		Psync psync = new Psync(clientPool, endpoint, replicationStoreManager);
		psync.addPsyncObserver(this);
		psync.addPsyncObserver(redisKeeperServer);
		psync.execute().addListener(new CommandFutureListener<Object>() {

			@Override
			public void operationComplete(CommandFuture<Object> commandFuture) throws Exception {
				
				if(!commandFuture.isSuccess()){
					logger.error("[operationComplete][psyncCommand][fail, retry]", commandFuture.cause());
					scheduled.schedule(new Runnable() {
						
						@Override
						public void run() {
							try {
								//if backup , send kinfo
								sendReplicationCommand();
							} catch (CommandExecutionException e) {
								logger.error("[run]" + DefaultRedisMaster.this, e);
							}
						}
					}, PSYNC_RETRY_INTERVAL_MILLI, TimeUnit.MILLISECONDS);
				}
			}
		});;
		
	}

	private void listeningPortCommand() throws CommandExecutionException {

		Replconf replconf = new Replconf(clientPool, ReplConfType.LISTENING_PORT, String.valueOf(redisKeeperServer.getListeningPort()));
		replconf.execute();
	}

	private void scheduleReplconf() {

		if (logger.isInfoEnabled()) {
			logger.info("[scheduleReplconf]" + this);
		}

		replConfFuture = scheduled.scheduleWithFixedDelay(new Runnable() {

			@Override
			public void run() {
				try {

					logger.debug("[run][send ack]{}", masterChannel);
					Command<Object> command = new Replconf(clientPool, ReplConfType.ACK, String.valueOf(getCurrentReplicationStore().getEndOffset()));
					command.execute();
				} catch (Throwable th) {
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
	public void beginWriteRdb() throws IOException {

		partialState = PARTIAL_STATE.FULL;
		getCurrentReplicationStore().getMetaStore().setMasterAddress(endpoint);
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

	private ReentrantLock fullSyncLock = new ReentrantLock();
	private AtomicLong lastRdbUpdateTime = new AtomicLong(0);

	@Override
	public void fullSyncToSlave(final RedisSlave redisSlave) throws IOException {
		final ReplicationStore currentStore = replicationStoreManager.createIfNotExist();
		FullSyncListener rdbListener = new DefaultFullSyncListener(redisSlave);

		boolean fullSyncPossible = currentStore.fullSyncIfPossible(rdbListener);

		if (!fullSyncPossible) {
			fullSyncLock.lock();

			rdbListener = lockAware(rdbListener, fullSyncLock);

			try {
				while (!Thread.interrupted()) {
					if (currentStore.fullSyncIfPossible(rdbListener)) {
						break;
					}

					// TODO config
					if (System.currentTimeMillis() - lastRdbUpdateTime.get() > 5000) {

						logger.info("[fullSyncToSlave]update rdb to full sync");
						File rdbFile = currentStore.prepareNewRdbFile();
						logger.info("[fullSyncToSlave]Create file {}", rdbFile);

						SettableFuture<Boolean> endWriteRdbFuture = SettableFuture.create();

						AtomicReference<RdbOnlyPsync> rdbOnlyPsyncRef = new AtomicReference<RdbOnlyPsync>();
						RdbOnlyReplicationStore rdbOnlyReplicationStore = new RdbOnlyReplicationStore(rdbFile);

						lastRdbUpdateTime.set(System.currentTimeMillis());
						connectWithMaster(new RdbOnlyPsyncNettyHandler(rdbOnlyReplicationStore, rdbOnlyPsyncRef, endWriteRdbFuture), false);
						lastRdbUpdateTime.set(System.currentTimeMillis());

						waitUntilPsyncDone(rdbOnlyPsyncRef, rdbOnlyReplicationStore, redisSlave, currentStore, endWriteRdbFuture);
					} else {
						try {
							sendNewLineToRedisSlave(redisSlave);
							// TODO config
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							Thread.currentThread().interrupt();
							break;
						}
					}
				}
			} finally {
				if (fullSyncLock.isHeldByCurrentThread()) {
					fullSyncLock.unlock();
				}
			}
		}
	}

	private void waitUntilPsyncDone(AtomicReference<RdbOnlyPsync> rdbOnlyPsyncRef, RdbOnlyReplicationStore rdbOnlyReplicationStore, RedisSlave redisSlave,
			ReplicationStore currentStore, SettableFuture<Boolean> endWriteRdbFuture) {
		
		logger.info("[waitUntilPsyncDone]{}", redisSlave);
		
		while (!Thread.interrupted()) {
			boolean psyncFinished = false;

			RdbOnlyPsync rdbOnlyPsync = rdbOnlyPsyncRef.get();
			if (rdbOnlyPsync != null) {
				if (rdbOnlyPsync.future().isDone()) {
					logger.info("[waitUntilPsyncDone][done]{}", redisSlave);
					psyncFinished = true;

					File rdbFile = rdbOnlyReplicationStore.getRdbFile();
					if (rdbOnlyPsync.future().isSuccess()) {
						logger.info("[waitUntilPsyncDone][success]{}", redisSlave);
						/**
						 * isDone() means endWriteRdb() is called. no exception
						 * will ever in endWriteRdbFuture
						 */
						if (endWriteRdbFuture.isDone()) {
							try {
								currentStore.rdbUpdated(rdbFile.getName(), rdbOnlyReplicationStore.getMasterOffset());
								return;
							} catch (Exception e) {
								logger.error("[updateRdb] error update rdb to ReplicationStore", e);
								rdbFile.delete();
								return;
							}
						}
					} else {
						logger.info("[waitUntilPsyncDone][fail]{}", redisSlave);
						logger.error("[waitUntilPsyncDone]", rdbOnlyPsync.future().cause());
						rdbFile.delete();
						return;
					}
				}
			}

			if (!psyncFinished) {
				try {
					sendNewLineToRedisSlave(redisSlave);
					// TODO config
					Thread.sleep(100);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					return;
				}
			}

		}
	}

	private void sendNewLineToRedisSlave(RedisSlave redisSlave) {
		redisSlave.sendMessage(NEW_LINE);
	}

	private FullSyncListener lockAware(final FullSyncListener fullSyncListener, final ReentrantLock fullSyncLock) {
		return new FullSyncListener() {

			public void setRdbFileInfo(long rdbFileSize, long rdbFileKeeperOffset) {
				fullSyncListener.setRdbFileInfo(rdbFileSize, rdbFileKeeperOffset);
			}

			public void onFileData(FileChannel fileChannel, long pos, long len) throws IOException {
				fullSyncListener.onFileData(fileChannel, pos, len);
			}

			public boolean isOpen() {
				return fullSyncListener.isOpen();
			}

			public void exception(Exception e) {
				fullSyncListener.exception(e);
			}

			@Override
			public void onCommand(ByteBuf byteBuf) {
				fullSyncListener.onCommand(byteBuf);
			}

			@Override
			public void beforeFileData() {
				fullSyncLock.unlock();
			}

			@Override
			public void beforeCommand() {
				fullSyncListener.beforeCommand();
			}
		};
	}

	static class RdbOnlyPsyncNettyHandler extends AbstractNettyHandler {
		private FixedObjectPool<NettyClient> clientPool;
		RdbOnlyReplicationStore rdbOnlyReplicationStore;
		private SettableFuture<Boolean> endWriteRdbFuture;
		private AtomicReference<RdbOnlyPsync> rdbOnlyPsyncRef;

		public RdbOnlyPsyncNettyHandler(RdbOnlyReplicationStore rdbOnlyReplicationStore, AtomicReference<RdbOnlyPsync> rdbOnlyPsyncRef,
				SettableFuture<Boolean> endWriteRdbFuture) {
			this.rdbOnlyReplicationStore = rdbOnlyReplicationStore;
			this.rdbOnlyPsyncRef = rdbOnlyPsyncRef;
			this.endWriteRdbFuture = endWriteRdbFuture;
		}

		@Override
		public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

			ByteBuf byteBuf = (ByteBuf) msg;
			byteBufReadPolicy.read(ctx.channel(), byteBuf, new ByteBufReadAction() {
				@Override
				public void read(Channel channel, ByteBuf byteBuf) throws XpipeException {
					clientPool.getObject().handleResponse(channel, byteBuf);
				}
			});
			super.channelRead(ctx, msg);
		}

		@Override
		public void channelActive(ChannelHandlerContext ctx) throws Exception {
			clientPool = new FixedObjectPool<NettyClient>(new DefaultNettyClient(ctx.channel()));
			RdbOnlyPsync rdbOnlyPsync = new RdbOnlyPsync(clientPool, rdbOnlyReplicationStore);
			rdbOnlyPsyncRef.set(rdbOnlyPsync);
			rdbOnlyPsync.addPsyncObserver(new PsyncObserver() {

				@Override
				public void reFullSync() {
				}

				@Override
				public void onContinue() {
				}

				@Override
				public void endWriteRdb() {
					endWriteRdbFuture.set(true);
				}

				@Override
				public void beginWriteRdb() {
				}
			});
			rdbOnlyPsync.execute();

			super.channelActive(ctx);
		}
	}

}
