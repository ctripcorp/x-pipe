package com.ctrip.xpipe.redis.keeper.impl;

import java.net.InetSocketAddress;
import java.nio.channels.FileChannel;

import com.ctrip.xpipe.netty.NotClosableFileRegion;
import com.ctrip.xpipe.redis.keeper.CommandsListener;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import com.ctrip.xpipe.redis.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.protocal.protocal.RequestStringParser;
import com.ctrip.xpipe.utils.IpUtils;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * @author wenchao.meng
 *
 * May 20, 2016 4:34:09 PM
 */
public class DefaultRedisSlave extends DefaultRedisClient implements RedisSlave, CommandsListener{
	
	private Long replAckOff;
	
	private Long replAckTime;

	private SLAVE_STATE  slaveState;
	
	private Long rdbFileOffset;

	
	public DefaultRedisSlave(RedisClient redisClient){
		super((DefaultRedisClient)redisClient);
		this.setSlaveListeningPort(redisClient.getSlaveListeningPort());
		
	}

	public DefaultRedisSlave(Channel channel, RedisKeeperServer redisKeeperServer) {
		super(channel, redisKeeperServer);
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
		
		slaveState = SLAVE_STATE.REDIS_REPL_SEND_BULK;
		this.rdbFileOffset = rdbFileOffset;
		
    	RequestStringParser parser = new RequestStringParser(String.valueOf((char)RedisClientProtocol.DOLLAR_BYTE)
    			+String.valueOf(rdbFileSize)); 
    	channel.writeAndFlush(parser.format());
	}

	@Override
	public void beginWriteCommands(long beginOffset) {
		
		redisKeeperServer.addCommandsListener(beginOffset, this);
	}

	@Override
	public void rdbWriteComplete() {
		
		if(slaveState == SLAVE_STATE.REDIS_REPL_SEND_BULK){
			
			if(logger.isInfoEnabled()){
				logger.info("[writeComplete][rdbWriteComplete]" + this);
			}
			slaveState = SLAVE_STATE.REDIS_REPL_ONLINE;
			beginWriteCommands(rdbFileOffset + 1);
		}
	}

	@Override
	public void onCommand(ByteBuf byteBuf) {
		
		channel.writeAndFlush(byteBuf);
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
}
