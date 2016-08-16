package com.ctrip.xpipe.redis.keeper.impl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.api.monitor.DelayMonitor;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.api.server.PARTIAL_STATE;
import com.ctrip.xpipe.monitor.DefaultDelayMonitor;
import com.ctrip.xpipe.netty.NotClosableFileRegion;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import com.ctrip.xpipe.redis.keeper.netty.ChannelUtil;
import com.ctrip.xpipe.utils.IpUtils;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.XpipeThreadFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * @author wenchao.meng
 *
 * May 20, 2016 4:34:09 PM
 */
public class DefaultRedisSlave implements RedisSlave {
	
	private final static Logger logger = LoggerFactory.getLogger(DefaultRedisSlave.class);
	
	private Long replAckOff;
	
	private Long replAckTime = System.currentTimeMillis();

	private SLAVE_STATE  slaveState;
	
	private PARTIAL_STATE partialState = PARTIAL_STATE.UNKNOWN;
	
	private Long rdbFileOffset;
	
	private DelayMonitor delayMonitor = new DefaultDelayMonitor("CREATE_NETTY", 5000);
	private boolean debugDelay = Boolean.parseBoolean(System.getProperty("DEBUG_DELAY"));

	private ExecutorService psyncExecutor;

	private RedisClient redisClient;
	
	private AtomicBoolean closed = new AtomicBoolean(false);
	
	public DefaultRedisSlave(RedisClient redisClient){
		this.redisClient = redisClient;
		this.setSlaveListeningPort(redisClient.getSlaveListeningPort());
		delayMonitor.setDelayInfo(redisClient.channel().remoteAddress().toString());
		initPsyncExecutor(((DefaultRedisClient)redisClient).channel);
	}

	private void initPsyncExecutor(Channel channel) {
		String getRemoteIpLocalPort = ChannelUtil.getRemoteIpLocalPort(channel);
		psyncExecutor = Executors.newSingleThreadExecutor(XpipeThreadFactory.create("RedisClientPsync-" + getRemoteIpLocalPort));
	}

	@Override
	public void setSlaveState(SLAVE_STATE slaveState) {
		this.slaveState = slaveState;
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
	public void writeFile(FileChannel fileChannel, long pos, long len) {
		
		channel().writeAndFlush(new NotClosableFileRegion(fileChannel, pos, len));
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
		
		partialState = PARTIAL_STATE.FULL;
		slaveState = SLAVE_STATE.REDIS_REPL_SEND_BULK;
		this.rdbFileOffset = rdbFileOffset;
		
    	RequestStringParser parser = new RequestStringParser(String.valueOf((char)RedisClientProtocol.DOLLAR_BYTE)
    			+String.valueOf(rdbFileSize)); 
    	channel().writeAndFlush(parser.format());
	}

	@Override
	public void beginWriteCommands(long beginOffset) {
		
		logger.info("[beginWriteCommands]{}, {}", this, beginOffset);
		slaveState = SLAVE_STATE.REDIS_REPL_ONLINE;
	}

	@Override
	public void rdbWriteComplete() {
		
		if(slaveState == SLAVE_STATE.REDIS_REPL_SEND_BULK){
			
			if(logger.isInfoEnabled()){
				logger.info("[writeComplete][rdbWriteComplete]" + this);
			}
			beginWriteCommands(rdbFileOffset + 1);
		}
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
	
}
