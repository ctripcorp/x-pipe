package com.ctrip.xpipe.redis.keeper.impl;


import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.api.monitor.DelayMonitor;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.api.server.PARTIAL_STATE;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.monitor.DefaultDelayMonitor;
import com.ctrip.xpipe.netty.NotClosableFileRegion;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import com.ctrip.xpipe.redis.keeper.SLAVE_STATE;
import com.ctrip.xpipe.redis.keeper.exception.RedisKeeperRuntimeException;
import com.ctrip.xpipe.redis.keeper.netty.ChannelUtil;
import com.ctrip.xpipe.utils.IpUtils;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.XpipeThreadFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

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
	
	@SuppressWarnings("unused")
	private Long rdbFileOffset;
	
	private DelayMonitor delayMonitor = new DefaultDelayMonitor("CREATE_NETTY", 5000);
	private boolean debugDelay = Boolean.parseBoolean(System.getProperty("DEBUG_DELAY"));
	
	private ScheduledExecutorService scheduled;
	private ScheduledFuture<?> 		  pingFuture, waitDumpTimeoutFuture;
	private final int pingIntervalMilli = 1000;
	private final int rdbDumpMaxWaitMilli = Integer.parseInt(System.getProperty(KEY_RDB_DUMP_MAX_WAIT_MILLI, "1800000"));//half an hour

	private ExecutorService psyncExecutor;

	private RedisClient redisClient;
	
	private AtomicBoolean closed = new AtomicBoolean(false);
	
	public DefaultRedisSlave(RedisClient redisClient){
		this.redisClient = redisClient;
		this.setSlaveListeningPort(redisClient.getSlaveListeningPort());
		delayMonitor.setDelayInfo(redisClient.channel().remoteAddress().toString());
		this.redisClient.addChannelCloseReleaseResources(this);
		initExecutor(((DefaultRedisClient)redisClient).channel);
	}

	private void initExecutor(Channel channel) {
		
		String getRemoteIpLocalPort = ChannelUtil.getRemoteAddr(channel);
		String threadPrefix = "RedisClientPsync-" + getRemoteIpLocalPort;
		psyncExecutor = Executors.newSingleThreadExecutor(XpipeThreadFactory.create(threadPrefix));
		scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create(threadPrefix));
	}

	@Override
	public void waitForRdbDumping() {
		
		if(this.slaveState == SLAVE_STATE.REDIS_REPL_WAIT_RDB_DUMPING){
			logger.info("[waitForRdbDumping][already waiting]{}", this);
			return;
		}
		
		this.slaveState = SLAVE_STATE.REDIS_REPL_WAIT_RDB_DUMPING;
		
		logger.info("[waitForRdbDumping][begin ping]{}", this);
		pingFuture = scheduled.scheduleAtFixedRate(new Runnable() {
			
			@Override
			public void run() {
				try{
					sendMessage("\n".getBytes());
				}catch(Exception e){
					logger.error("[run][sendPing]" + redisClient, e);
				}
			}
		}, pingIntervalMilli, pingIntervalMilli, TimeUnit.MILLISECONDS);
		
		waitDumpTimeoutFuture = scheduled.schedule(new AbstractExceptionLogTask() {
			
			@Override
			protected void doRun() throws IOException {
				logger.info("[waitForRdbDumping][timeout][close slave]{}", DefaultRedisSlave.this);
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
		
		if(logger.isDebugEnabled()){
			logger.debug("[ack]{}, {}", this , ackOff);
		}
		
		this.replAckOff = ackOff;
		this.replAckTime = System.currentTimeMillis();
	}

	@Override
	public ChannelFuture writeFile(FileChannel fileChannel, long pos, long len) {
		
		return channel().writeAndFlush(new NotClosableFileRegion(fileChannel, pos, len));
	}

	@Override
	public Long getAck() {
		return this.replAckOff;
	}

	@Override
	public Long getAckTime() {
		return this.replAckTime;
	}
	
	@Override
	public void beginWriteRdb(long rdbFileSize, long rdbFileOffset) {
		
		logger.info("[beginWriteRdb]{}, {}", rdbFileSize, rdbFileOffset);
		
		partialState = PARTIAL_STATE.FULL;
		slaveState = SLAVE_STATE.REDIS_REPL_SEND_BULK;
		this.rdbFileOffset = rdbFileOffset;
		
		cancelWaitRdb();
		
    	RequestStringParser parser = new RequestStringParser(String.valueOf((char)RedisClientProtocol.DOLLAR_BYTE)
    			+String.valueOf(rdbFileSize)); 
    	channel().writeAndFlush(parser.format());
	}

	private void cancelWaitRdb() {
		
		if(pingFuture != null){
			logger.info("[cancelWaitRdb][cancel ping]{}", this);
			pingFuture.cancel(true);
		}
		if(waitDumpTimeoutFuture != null){
			logger.info("[cancelWaitRdb][cancel wait dump rdb]{}", this);
			waitDumpTimeoutFuture.cancel(true);
		}
	}

	@Override
	public void beginWriteCommands(long beginOffset) {
		
		try {
			logger.info("[beginWriteCommands]{}, {}", this, beginOffset);
			slaveState = SLAVE_STATE.REDIS_REPL_ONLINE;
			getRedisKeeperServer().getReplicationStore().addCommandsListener(beginOffset, this);
		} catch (IOException e) {
			throw new RedisKeeperRuntimeException("[beginWriteCommands]" + beginOffset + "," + this, e);
		}
	}

	@Override
	public void rdbWriteComplete() {
		
		logger.info("[rdbWriteComplete]{}", this);

		if(slaveState == SLAVE_STATE.REDIS_REPL_SEND_BULK){
			if(logger.isInfoEnabled()){
				logger.info("[writeComplete][rdbWriteComplete]" + this);
			}
		}
		this.slaveState = SLAVE_STATE.REDIS_REPL_ONLINE;
	}

	
	@Override
	public void onCommand(ByteBuf byteBuf) {
		
		ByteBuf b2 = byteBuf.duplicate();
		logger.debug("[onCommand]{}", this);
		if(debugDelay){
			long createTime = getTime(b2);
			delayMonitor.addData(createTime);
		}
		channel().writeAndFlush(byteBuf);
	}

	/**
	 * can only support key or value with currentTimeMillis
	 * @param b2
	 * @return
	 */
	public static long getTime(ByteBuf byteBuf) {
		
		String data = new String(byteBuf.array(), byteBuf.arrayOffset() + byteBuf.readerIndex(), byteBuf.readableBytes());
		
		long time = -1;
		String []parts = data.split("\r\n");
		for(String part : parts){
				time = OsUtils.getCorrentTime(part);
				if(time > 0){
					break;
				}
		}
		return time;
	}

	@Override
	public String info() {
		
		String info = "";
		long lag = System.currentTimeMillis() - replAckTime;
		info = String.format(
				"ip=%s,port=%d,state=%s,offset=%d,lag=%d,remotePort=%d" ,
				IpUtils.getIp(channel().remoteAddress()), getSlaveListeningPort(), 
				slaveState != null ? slaveState.getDesc() : "null",
				replAckOff, lag/1000, ((InetSocketAddress)channel().remoteAddress()).getPort());
		return info;
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
		psyncExecutor.execute(runnable);
	}

	@Override
	public boolean isOpen() {
		return !closed.get();
	}
	
	public void close() throws IOException {
		
		logger.info("[close]{}", this);
		closed.set(true);
		redisClient.close();
		psyncExecutor.shutdownNow();
		scheduled.shutdownNow();
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

	public RedisKeeperServer getRedisKeeperServer() {
		return redisClient.getRedisKeeperServer();
	}

	public void setSlaveListeningPort(int port) {
		redisClient.setSlaveListeningPort(port);
	}

	public int getSlaveListeningPort() {
		return redisClient.getSlaveListeningPort();
	}

	public void capa(CAPA capa) {
		redisClient.capa(capa);
	}

	public String[] readCommands(ByteBuf byteBuf) {
		return redisClient.readCommands(byteBuf);
	}

	public Channel channel() {
		return redisClient.channel();
	}

	public void sendMessage(ByteBuf byteBuf) {
		redisClient.sendMessage(byteBuf);
	}

	public void sendMessage(byte[] bytes) {
		redisClient.sendMessage(bytes);
	}

	public void addChannelCloseReleaseResources(Releasable releasable) {
		redisClient.addChannelCloseReleaseResources(releasable);
	}

	public void processCommandSequentially(Runnable runnable) {
		redisClient.processCommandSequentially(runnable);
	}
	// delegate methods end
	
	@Override
	public String toString() {
		return this.redisClient.toString();
	}

	@Override
	public void release() throws Exception {
		logger.info("[release]{}", this);
		psyncExecutor.shutdownNow();
		closed.set(true);
	}
	
}
