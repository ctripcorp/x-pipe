package com.ctrip.xpipe.redis.keeper.impl;

import java.net.InetSocketAddress;
import java.nio.channels.FileChannel;

import com.ctrip.xpipe.api.monitor.DelayMonitor;
import com.ctrip.xpipe.api.server.PARTIAL_STATE;
import com.ctrip.xpipe.monitor.DefaultDelayMonitor;
import com.ctrip.xpipe.netty.NotClosableFileRegion;
import com.ctrip.xpipe.redis.keeper.CommandsListener;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import com.ctrip.xpipe.redis.keeper.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.keeper.protocal.protocal.RequestStringParser;
import com.ctrip.xpipe.utils.IpUtils;
import com.ctrip.xpipe.utils.OsUtils;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * @author wenchao.meng
 *
 * May 20, 2016 4:34:09 PM
 */
public class DefaultRedisSlave extends DefaultRedisClient implements RedisSlave, CommandsListener{
	
	private Long replAckOff;
	
	private Long replAckTime = System.currentTimeMillis();

	private SLAVE_STATE  slaveState;
	
	private PARTIAL_STATE partialState = PARTIAL_STATE.UNKNOWN;
	
	private Long rdbFileOffset;
	
	private DelayMonitor delayMonitor = new DefaultDelayMonitor("CREATE_NETTY", 5000);
	private boolean debugDelay = Boolean.parseBoolean(System.getProperty("DEBUG_DELAY"));

	
	public DefaultRedisSlave(RedisClient redisClient){
		super((DefaultRedisClient)redisClient);
		this.setSlaveListeningPort(redisClient.getSlaveListeningPort());
		delayMonitor.setDelayInfo(redisClient.channel().remoteAddress().toString());
		
	}

	public DefaultRedisSlave(Channel channel, RedisKeeperServer redisKeeperServer) {
		super(channel, redisKeeperServer);
		delayMonitor.setDelayInfo(channel.remoteAddress().toString());
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
			logger.debug("[ack]" + this + "," + ackOff);
		}
		
		this.replAckOff = ackOff;
		this.replAckTime = System.currentTimeMillis();
	}

	@Override
	public void writeFile(FileChannel fileChannel, long pos, long len) {
		
		channel.writeAndFlush(new NotClosableFileRegion(fileChannel, pos, len));
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
    	channel.writeAndFlush(parser.format());
	}

	@Override
	public void beginWriteCommands(long beginOffset) {
		
		slaveState = SLAVE_STATE.REDIS_REPL_ONLINE;
		redisKeeperServer.getKeeperRepl().addCommandsListener(beginOffset, this);
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
		
		if(debugDelay){
			long createTime = getTime(b2);
			delayMonitor.addData(createTime);
		}
		channel.writeAndFlush(byteBuf);
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
				IpUtils.getIp(channel.remoteAddress()), getSlaveListeningPort(), 
				slaveState != null ? slaveState.getDesc() : "null",
				replAckOff, lag/1000, ((InetSocketAddress)channel.remoteAddress()).getPort());
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
	
}
