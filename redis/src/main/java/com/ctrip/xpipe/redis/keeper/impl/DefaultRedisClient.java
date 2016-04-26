package com.ctrip.xpipe.redis.keeper.impl;



import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.payload.ByteArrayOutputStreamPayload;
import com.ctrip.xpipe.redis.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.keeper.CommandsListener;
import com.ctrip.xpipe.redis.keeper.RdbFile;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.protocal.protocal.ArrayParser;
import com.ctrip.xpipe.redis.protocal.protocal.RequestStringParser;
import com.ctrip.xpipe.redis.protocal.protocal.SimpleStringParser;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.DefaultChannelProgressivePromise;
import io.netty.channel.DefaultFileRegion;

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
		if(logger.isInfoEnabled()){
			logger.info("[setSlaveListeningPort]" + this + "," + port);
		}
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
		
		if(logger.isDebugEnabled()){
			logger.debug("[ack]" + this + "," + ackOff);
		}
		
		this.replAckOff = ackOff;
		this.replAckTime = System.currentTimeMillis();
	}

	@Override
	public void sendMessage(byte[] message) {
		
		if(clientRole == CLIENT_ROLE.SLAVE && slaveState == SLAVE_STATE.REDIS_REPL_ONLINE){
			return;
		}
		channel.writeAndFlush(message);
	}

	@Override
	public Long getAck() {
		return this.replAckOff;
	}

	@Override
	public Long getAckTime() {
		return this.replAckTime;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void writeRdb(RdbFile rdbFile) {
		
		try {
			
			slaveState = SLAVE_STATE.REDIS_REPL_SEND_BULK;
			this.rdbBeginOffset = rdbFile.getRdboffset();
	    	DefaultChannelProgressivePromise promise = new DefaultChannelProgressivePromise(channel); 
	    	promise.addListeners(new ChannelFutureListener() {
				
				public void operationComplete(ChannelFuture future) throws Exception {
					if(future.isSuccess()){
						writeComplete();
					}else{
						logger.error("[operationComplete][write fail]" + channel, future.cause());
					}
				}
			});
	    	
	    	RequestStringParser parser = new RequestStringParser(String.valueOf((char)RedisClientProtocol.DOLLAR_BYTE)
	    			+String.valueOf(rdbFile.getRdbFile().size())); 
	    	channel.writeAndFlush(parser.format());
			channel.writeAndFlush(new DefaultFileRegion(rdbFile.getRdbFile(), 0, rdbFile.getRdbFile().size()), promise);
		} catch (IOException e) {
			logger.error("[writeRdb]" + rdbFile, e);
		}
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
			beginWriteCommands(rdbBeginOffset + 1);
		}
	}

	@Override
	public void onCommand(ByteBuf byteBuf) {
		
		channel.writeAndFlush(byteBuf);
	}
	
	@Override
	public String toString() {
		return channel.toString();
	}


	public static enum COMMAND_STATE{
		READ_SIGN,
		READ_COMMANDS
	}
	
	private COMMAND_STATE commandState = COMMAND_STATE.READ_SIGN;
	private RedisClientProtocol<?>  redisClientProtocol;

	
	@Override
	public String[] readCommands(ByteBuf byteBuf) {
	
		while(true){

			switch(commandState){
				case READ_SIGN:
					if(!hasDataRead(byteBuf)){
						return null;
					}
					int readIndex = byteBuf.readerIndex();
					byte sign = byteBuf.getByte(readIndex);
					if(sign == RedisClientProtocol.ASTERISK_BYTE){
						redisClientProtocol = new ArrayParser();
					}else if(sign == '\n'){
						byteBuf.readByte();
						return new String[]{"\n"};
					}else{
						redisClientProtocol = new SimpleStringParser();
					}
					commandState = COMMAND_STATE.READ_COMMANDS;
				case READ_COMMANDS:
					RedisClientProtocol<?> resultParser = redisClientProtocol.read(byteBuf);
					if(resultParser == null){
						return null;
					}
					
					Object result = resultParser.getPayload();
					if(result == null){
						return new String[0];
					}
					
					commandState = COMMAND_STATE.READ_SIGN ;
					String []ret = null;
					if(result instanceof String){
						ret = handleString((String)result);
					}else if(result instanceof Object[]){
						ret = handleArray((Object[])result);
					}else{
						throw new IllegalStateException("unkonw result array:" + result);
					}
					return ret;
				default:
					throw new IllegalStateException("unkonwn state:" + commandState);
			}
		}
	}
	

	private String[] handleArray(Object[] result) {

		String []strArray = new String[result.length];
		int index = 0;
		for(Object param : result){
			
			if(param instanceof String){
				strArray[index] = (String) param;
			}else if(param instanceof ByteArrayOutputStreamPayload){
				
				byte [] bytes = ((ByteArrayOutputStreamPayload)param).getBytes();
				strArray[index] = new String(bytes, Codec.defaultCharset);
			}else{
				throw new RedisRuntimeException("request unkonwn, can not be transformed to string!");
			}
			index++;
		}
		return strArray;
	}

	private String[] handleString(String result) {
		
		return result.trim().split("\\s+");
	}

	private boolean hasDataRead(ByteBuf byteBuf) {
		
		if(byteBuf.readableBytes() > 0){
			return true;
		}
		return false;
	}

}
