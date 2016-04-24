package com.ctrip.xpipe.redis.keeper.impl;


import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ctrip.xpipe.redis.keeper.CommandsListener;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * @author wenchao.meng
 *
 * 2016年4月22日 上午11:30:49
 */
public class DefaultRedisClient implements RedisClient, CommandsListener{
	
	private static Logger logger = LogManager.getLogger(DefaultRedisClient.class);
	
	private Set<CAPA>  capas = new HashSet<CAPA>(); 

	private int slaveListeningPort;
	
	private Channel channel;
	
	private Long replAckOff;
	
	private Long replAckTime;

	private RedisKeeperServer redisKeeperServer;
	
	private CLIENT_ROLE clientRole = CLIENT_ROLE.SLAVE;
	
	private SLAVE_STATE  slaveState;
	
	private Long rdbBeginOffset;
	
	public DefaultRedisClient(Channel channel, RedisKeeperServer redisKeeperServer) {

		this.channel = channel;
		this.redisKeeperServer = redisKeeperServer;
	}

	@Override
	public CLIENT_ROLE getClientRole() {
		return clientRole;
	}

	@Override
	public RedisKeeperServer getRedisKeeperServer() {
		return redisKeeperServer;
	}

	@Override
	public void setClientRole(CLIENT_ROLE clientRole) {
		this.clientRole = clientRole;
	}

	@Override
	public void setSlaveListeningPort(int port) {
		this.slaveListeningPort = port;
	}

	@Override
	public void capa(CAPA capa) {
		capas.add(capa);
	}
	
	@Override
	public int getSlaveListeningPort() {
		return this.slaveListeningPort;
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
		this.replAckOff = ackOff;
		this.replAckTime = System.currentTimeMillis();
	}

	@Override
	public void sendMessage(byte[] message) {
		
		if(clientRole == CLIENT_ROLE.SLAVE){
			return;
		}
		channel.write(message);
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
	public String toString() {
		return channel.toString();
	}

	@Override
	public void writeRdb(long rdbBeginOffset) {
		slaveState = SLAVE_STATE.REDIS_REPL_SEND_BULK;
		this.rdbBeginOffset = rdbBeginOffset;
	}

	@Override
	public void beginWriteCommands(long beginOffset) {
		
		redisKeeperServer.addCommandsListener(beginOffset, this);
	}

	@Override
	public void writeComplete() {
		
		if(slaveState == SLAVE_STATE.REDIS_REPL_SEND_BULK){
			
			if(logger.isInfoEnabled()){
				logger.info("[writeComplete][rdbWriteComplete]" + this);
			}
			slaveState = SLAVE_STATE.REDIS_REPL_ONLINE;
			beginWriteCommands(rdbBeginOffset);
		}
	}

	@Override
	public void onCommand(ByteBuf byteBuf) {
		
		channel.write(byteBuf);
	}
}
