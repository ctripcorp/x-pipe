package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.api.server.PARTIAL_STATE;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.netty.filechannel.ReferenceFileRegion;
import com.ctrip.xpipe.redis.core.protocal.CAPA;
import com.ctrip.xpipe.redis.core.protocal.cmd.DefaultPsync;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.protocal.protocal.SimpleStringParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.store.*;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import com.ctrip.xpipe.redis.keeper.SLAVE_STATE;
import com.ctrip.xpipe.redis.keeper.exception.RedisKeeperRuntimeException;
import com.ctrip.xpipe.utils.*;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author wenchao.meng
 *
 * May 20, 2016 4:34:09 PM
 */
public class DefaultRedisSlave implements RedisSlave {
	
	private final static Logger logger = LoggerFactory.getLogger(DefaultRedisSlave.class);
	
	public static final String KEY_RDB_DUMP_MAX_WAIT_MILLI = "rdbDumpMaxWaitMilli";

	private Long replAckOff;
	
	private Long replAckTime = System.currentTimeMillis();

	private SLAVE_STATE  slaveState;
	
	private PARTIAL_STATE partialState = PARTIAL_STATE.UNKNOWN;
	
	private Long rdbFileOffset;
	private ReplicationProgress<?> progressAfterRdb;
	private EofType eofType;
		
	private ScheduledExecutorService scheduled;
	private ScheduledFuture<?> 		  pingFuture, waitTimeoutFuture;

	private static final int pingIntervalMilli = 1000;

	private int rdbDumpMaxWaitMilli = Integer.parseInt(System.getProperty(KEY_RDB_DUMP_MAX_WAIT_MILLI, "1800000"));//half an hour
	private int waitForPsyncProcessedTimeoutMilli = 10000;
	
	private volatile boolean putOnLineOnAck = false; 

	private ExecutorService psyncExecutor;

	private RedisClient<RedisKeeperServer> redisClient;

	private AtomicBoolean writingCommands = new AtomicBoolean(false);

	private ChannelFutureListener writeExceptionListener = new ChannelFutureListener() {

		private AtomicLong atomicLong = new AtomicLong(0);

		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			
			if(!future.isSuccess()){
				long failCount = atomicLong.incrementAndGet();
				//avoid write too much error msg
				if((failCount & (failCount -1)) == 0){
					getLogger().error("[operationComplete][write fail]" +failCount + "," + DefaultRedisSlave.this, future.cause());
				}
			}
		}
	};

	private CloseState closeState = new CloseState();
	private SettableFuture<Boolean> psyncProcessed = SettableFuture.create();

	public DefaultRedisSlave(RedisClient<RedisKeeperServer> redisClient){
		this.redisClient = redisClient;
		this.setSlaveListeningPort(redisClient.getSlaveListeningPort());
		this.redisClient.addChannelCloseReleaseResources(this);
		initExecutor(((DefaultRedisClient)redisClient).channel);
	}

	private void initExecutor(Channel channel) {
		
		String threadPrefix = buildThreadPrefix(channel);
		ClusterId clusterId = redisClient.getRedisServer().getClusterId();
		ShardId shardId = redisClient.getRedisServer().getShardId();
		psyncExecutor = Executors.newSingleThreadExecutor(ClusterShardAwareThreadFactory.create(clusterId, shardId, threadPrefix));
		scheduled = Executors.newScheduledThreadPool(1, ClusterShardAwareThreadFactory.create(clusterId, shardId, threadPrefix));
	}

	protected String buildThreadPrefix(Channel channel) {
		String getRemoteIpLocalPort = ChannelUtil.getRemoteAddr(channel);
		return  "RedisClientPsync-" + getRemoteIpLocalPort;
	}

	@Override
	public void waitForRdbDumping() {
		
		if(this.slaveState == SLAVE_STATE.REDIS_REPL_WAIT_RDB_DUMPING){
			getLogger().info("[waitForRdbDumping][already waiting]{}", this);
			return;
		}
		
		this.slaveState = SLAVE_STATE.REDIS_REPL_WAIT_RDB_DUMPING;
		this.waitForRdb();
	}

	@Override
	public void waitForGtidParse() {

		if(this.slaveState == SLAVE_STATE.REDIS_REPL_WAIT_RDB_GTIDSET){
			getLogger().info("[waitForGtidParse][already waiting]{}", this);
			return;
		}

		this.slaveState = SLAVE_STATE.REDIS_REPL_WAIT_RDB_GTIDSET;

		if (null == pingFuture || pingFuture.isDone()) {
			waitForRdb();
		} else {
			getLogger().info("[waitForGtidParse][already start wait]{}", this);
		}
	}

	private void waitForRdb() {
		getLogger().info("[waitForRdb][begin ping]{}", this);
		pingFuture = scheduled.scheduleAtFixedRate(new Runnable() {

			@Override
			public void run() {
				try{
					sendMessage("\n".getBytes());
				}catch(Exception e){
					getLogger().error("[run][sendPing]" + redisClient, e);
				}
			}
		}, pingIntervalMilli, pingIntervalMilli, TimeUnit.MILLISECONDS);

		waitTimeoutFuture = scheduled.schedule(new AbstractExceptionLogTask() {

			@Override
			protected void doRun() throws IOException {
				getLogger().info("[waitForRdb][timeout][close slave]{}", DefaultRedisSlave.this);
				close();
			}
		}, rdbDumpMaxWaitMilli, TimeUnit.MILLISECONDS);
	}

	@Override
	public SLAVE_STATE getSlaveState() {
		return this.slaveState;
	}

	@Override
	public void ack(Long ackOff) {
		
		if(getLogger().isDebugEnabled()){
			getLogger().debug("[ack]{}, {}", this , ackOff);
		}
		
		if(putOnLineOnAck){
			
			putOnLineOnAck = false;
			getLogger().info("[ack][put slave online]{}", this);
			sendCommandForFullSync();
		}
		
		this.replAckOff = ackOff;
		this.replAckTime = System.currentTimeMillis();
	}

	@Override
	public ChannelFuture writeFile(ReferenceFileRegion referenceFileRegion) {
		
		return doWriteFile(referenceFileRegion);
	}

	private ChannelFuture doWriteFile(ReferenceFileRegion referenceFileRegion) {

		closeState.makeSureNotClosed();

		ChannelFuture future = channel().writeAndFlush(referenceFileRegion);
		future.addListener(writeExceptionListener);
		return future;
	}

	@Override
	public Long processedOffset() {
		return getAck();
	}

	@Override
	public Long getAck() {
		return this.replAckOff;
	}

	@Override
	public Long getAckTime() {
		return this.replAckTime;
	}

	protected String buildMarkBeforeFsync(ReplicationProgress<?> rdbProgress) {
		return StringUtil.join(" ", DefaultPsync.FULL_SYNC, getRedisServer().getKeeperRepl().replId(),
				rdbProgress.getProgress().toString());
	}
	
	@Override
	public void beginWriteRdb(EofType eofType, ReplicationProgress<?> rdbProgress) {
		getLogger().info("[beginWriteRdb]{}, {}", eofType, rdbProgress);
		closeState.makeSureOpen();

		SimpleStringParser simpleStringParser = new SimpleStringParser(buildMarkBeforeFsync(rdbProgress));

		getLogger().info("[setRdbFileInfo]{},{}", simpleStringParser.getPayload(), this);
		sendMessage(simpleStringParser.format());

		if(!eofType.support(getCapas())){
			getLogger().warn("[beginWriteRdb][eoftype not supported]{}, {}, {}", this, eofType, getCapas());
		}

		partialState = PARTIAL_STATE.FULL;
		slaveState = SLAVE_STATE.REDIS_REPL_SEND_BULK;

		this.eofType = eofType;
		if (rdbProgress instanceof OffsetReplicationProgress) {
			this.progressAfterRdb = new OffsetReplicationProgress(((OffsetReplicationProgress) rdbProgress).getProgress() + 1);
		} else {
			this.progressAfterRdb = rdbProgress;
		}

		putOnLineOnAck = eofType.putOnLineOnAck();

		cancelWaitRdb();

		channel().writeAndFlush(eofType.getStart());
	}

	
	@Override
	public void rdbWriteComplete() {
		
		getLogger().info("[rdbWriteComplete]{}", this);
		
		ByteBuf end = eofType.getEnd();
		if(end != null){
			channel().writeAndFlush(end);
		}
		

		if(slaveState == SLAVE_STATE.REDIS_REPL_SEND_BULK){
			if(getLogger().isInfoEnabled()){
				getLogger().info("[writeComplete][rdbWriteComplete]" + this);
			}
		}
		this.slaveState = SLAVE_STATE.REDIS_REPL_ONLINE;
		
		if(!putOnLineOnAck){
			sendCommandForFullSync();
		}
	}

	
	private void cancelWaitRdb() {
		
		if(pingFuture != null){
			getLogger().info("[cancelWaitRdb][cancel ping]{}", this);
			pingFuture.cancel(true);
		}
		if(waitTimeoutFuture != null){
			getLogger().info("[cancelWaitRdb][cancel wait dump rdb]{}", this);
			waitTimeoutFuture.cancel(true);
		}
	}

	@Override
	public void beginWriteCommands(ReplicationProgress<?> progress) {

		closeState.makeSureOpen();

		try {

			if (writingCommands.compareAndSet(false, true)) {
				if(partialState == PARTIAL_STATE.UNKNOWN){
					partialState = PARTIAL_STATE.PARTIAL;
				}
				getLogger().info("[beginWriteCommands]{}, {}", this, progress);
				slaveState = SLAVE_STATE.REDIS_REPL_ONLINE;
				getRedisServer().getReplicationStore().addCommandsListener(progress, this);
			} else {
				getLogger().warn("[beginWriteCommands][already writing]{}, {}", this, progress);
			}
		} catch (IOException e) {
			throw new RedisKeeperRuntimeException("[beginWriteCommands]" + progress + "," + this, e);
		}
	}

	protected void sendCommandForFullSync() {
		
		getLogger().info("[sendCommandForFullSync]{}, {}", this, progressAfterRdb);
		
		processPsyncSequentially(new AbstractExceptionLogTask() {
			
			@Override
			protected void doRun() throws Exception {
				try {
					beginWriteCommands(progressAfterRdb);
				} catch (Throwable th) {
					getLogger().error("[sendCommandForFullSync][failed]", th);
					if (DefaultRedisSlave.this.isOpen()) {
						getLogger().error("[sendCommandForFullSync] close slave");
						DefaultRedisSlave.this.close();
					}
				}
			}
		});
	}

	@Override
	public ChannelFuture onCommand(CommandFile currentFile, long filePosition, Object cmd) {
		closeState.makeSureOpen();
		getLogger().debug("[onCommand]{}, {}", this, cmd);

		Object command = cmd;

		if (cmd instanceof RedisOp) {
			if (shouldFilter((RedisOp) cmd)) {
				DefaultChannelPromise result = new DefaultChannelPromise(channel());
			    result.setSuccess();
			    return result;
			}
		    command = ((RedisOp) cmd).buildRESP();
		}

		ChannelFuture future = channel().writeAndFlush(command);
		future.addListener(writeExceptionListener);
		return future;
	}

	@VisibleForTesting
	protected boolean shouldFilter(RedisOp redisOp) {
	 	if (RedisOpType.PUBLISH.equals(redisOp.getOpType())) {
			int length = redisOp.buildRawOpArgs().length;
			if (length < 5) {
				logger.warn("publish command length={} < 5, filtered", length);
				return true;
			}
			String channel = new String(redisOp.buildRawOpArgs()[4]);
			if (!channel.startsWith("xpipe-hetero-")) {
				logger.debug("publish channel: [{}] filtered", channel);
				return true;
            }
        }
		return false;
    }

	@Override
	public String info() {
		
		String info = "";
		long lag = System.currentTimeMillis() - replAckTime;
		info = String.format(
				"ip=%s,port=%d,state=%s,offset=%d,lag=%d,remotePort=%d" ,
				getClientIpAddress() == null ? ip() : getClientIpAddress(),
				getSlaveListeningPort(),
				slaveState != null ? slaveState.getDesc() : "null",
				replAckOff, lag/1000, remotePort());
		return info;
	}

	@Override
	public String ip() {
		return redisClient.ip();
	}

	@Override
	public PARTIAL_STATE partialState() {
		return partialState;
	}

	@Override
	public void partialSync() {
		partialState = PARTIAL_STATE.PARTIAL;
	}

	@Override
	public void processPsyncSequentially(Runnable runnable) {
		closeState.makeSureNotClosed();
		psyncExecutor.execute(runnable);
	}

	@Override
	public void markPsyncProcessed() {

		getLogger().info("[markPsyncProcessed]{}", this);
		psyncProcessed.set(true);
	}

	@Override
	public String metaInfo() {
		return String.format("%s(%s:%d)", roleDesc(), ip(), getSlaveListeningPort());
	}

	@Override
	public boolean supportProgress(Class<? extends ReplicationProgress<?>> clazz) {
		return clazz.equals(OffsetReplicationProgress.class);
	}

	private int remotePort() {
		Channel channel = channel();
		return channel == null? 0: ((InetSocketAddress)channel.remoteAddress()).getPort();
	}

	@Override
	public boolean isOpen() {
		return closeState.isOpen();
	}

	@Override
	public void close() {
		close(0);
	}

	@VisibleForTesting
	/**
	 * testSleepMilli is for test
	 */
	protected void close(int testSleepMilli) {
		getLogger().info("[close]{}", this);
		if(closeState.isClosed()){
			getLogger().info("[close][already closed]{}", this);
			return;
		}

		closeState.setClosing();
		psyncProcessed.addListener(new AbstractExceptionLogTask() {
			@Override
			protected void doRun() throws Exception {
				doRealClose();
			}
		}, MoreExecutors.directExecutor());


		synchronized (closeState) {
			if (closeState.isClosing()) {
				//for unit test
				if (testSleepMilli > 0) {
					try {
						TimeUnit.MILLISECONDS.sleep(testSleepMilli);
					} catch (InterruptedException e) {
					}
				}
				scheduled.schedule(new AbstractExceptionLogTask() {
					@Override
					protected void doRun() throws Exception {
						getLogger().info("[wait for psync processed timeout close slave]{}", DefaultRedisSlave.this);
						doRealClose();
					}
				}, waitForPsyncProcessedTimeoutMilli, TimeUnit.MILLISECONDS);
			}
		}
	}

	protected void doRealClose() throws IOException {

		synchronized (closeState) {
			getLogger().info("[doRealClose]{}", this);
			closeState.setClosed();
			redisClient.close();
			psyncExecutor.shutdownNow();
			scheduled.shutdownNow();
		}
	}
	
	@Override
	public void beforeCommand() {
	}
	
	
	// delegate methods start
	public void addObserver(Observer observer) {
		redisClient.addObserver(observer);
	}

	public void removeObserver(Observer observer) {
		redisClient.removeObserver(observer);
	}

	public RedisSlave becomeSlave() {
		return redisClient.becomeSlave();
	}

	@Override
	public RedisSlave becomeXSlave() {
		return redisClient.becomeSlave();
	}

	public RedisKeeperServer getRedisServer() {
		return redisClient.getRedisServer();
	}

	public void setSlaveListeningPort(int port) {
		redisClient.setSlaveListeningPort(port);
	}

	public int getSlaveListeningPort() {
		return redisClient.getSlaveListeningPort();
	}

	@Override
	public void setClientIpAddress(String host) {
		redisClient.setClientIpAddress(host);
	}

	@Override
	public String getClientIpAddress() {
		return redisClient.getClientIpAddress();
	}

	public void capa(CAPA capa) {
		redisClient.capa(capa);
	}
	
	@Override
	public Set<CAPA> getCapas() {
		return redisClient.getCapas();
	}
	
	public String[] readCommands(ByteBuf byteBuf) {
		return redisClient.readCommands(byteBuf);
	}

	public Channel channel() {
		return redisClient.channel();
	}

	public void sendMessage(ByteBuf byteBuf) {
		closeState.makeSureNotClosed();
		redisClient.sendMessage(byteBuf);
	}

	public void sendMessage(byte[] bytes) {
		closeState.makeSureNotClosed();
		redisClient.sendMessage(bytes);
	}

	public void addChannelCloseReleaseResources(Releasable releasable) {
		redisClient.addChannelCloseReleaseResources(releasable);
	}
	
	protected Logger getLogger() {
		return logger;
	}

	@Override
	public void setClientEndpoint(Endpoint endpoint) {
		redisClient.setClientEndpoint(endpoint);
	}

	@Override
	public Endpoint getClientEndpoint() {
		return redisClient.getClientEndpoint();
	}

	@Override
	public String toString() {
		return this.redisClient.toString();
	}

	@Override
	public void release() throws Exception {
		getLogger().info("[release]{}", this);
		close();
	}

	@Override
	public boolean capaOf(CAPA capa) {
		return redisClient.capaOf(capa);
	}

	@Override
	public boolean isKeeper() {
		return redisClient.isKeeper();
	}

	@Override
	public void setKeeper() {
		redisClient.setKeeper();
	}

	@VisibleForTesting
	protected void setRdbDumpMaxWaitMilli(int rdbDumpMaxWaitMilli) {
		this.rdbDumpMaxWaitMilli = rdbDumpMaxWaitMilli;
	}

	@VisibleForTesting
	protected void setWaitForPsyncProcessedTimeoutMilli(int waitForPsyncProcessedTimeoutMilli) {
		this.waitForPsyncProcessedTimeoutMilli = waitForPsyncProcessedTimeoutMilli;
	}

	@VisibleForTesting
	protected CloseState getCloseState() {
		return closeState;
	}
}
