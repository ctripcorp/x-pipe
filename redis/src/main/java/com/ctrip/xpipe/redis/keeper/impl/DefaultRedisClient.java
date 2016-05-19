package com.ctrip.xpipe.redis.keeper.impl;


import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.FileChannel;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.netty.NotClosableFileRegion;
import com.ctrip.xpipe.payload.ByteArrayOutputStreamPayload;
import com.ctrip.xpipe.redis.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.keeper.CommandsListener;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.protocal.protocal.ArrayParser;
import com.ctrip.xpipe.redis.protocal.protocal.RequestStringParser;
import com.ctrip.xpipe.redis.protocal.protocal.SimpleStringParser;
import com.ctrip.xpipe.utils.IpUtils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;

/**
 * @author wenchao.meng
 *
 * 2016年4月22日 上午11:30:49
 */
public class DefaultRedisClient implements RedisClient, CommandsListener{
	
	private static Logger logger = LoggerFactory.getLogger(DefaultRedisClient.class);
	
	private Set<CAPA>  capas = new HashSet<CAPA>(); 

	private int slaveListeningPort;
	
	private Channel channel;
	
	private Long replAckOff;
	
	private Long replAckTime;

	private RedisKeeperServer redisKeeperServer;
	
	private CLIENT_ROLE clientRole = CLIENT_ROLE.NORMAL;
	
	private SLAVE_STATE  slaveState;
	
	private Long rdbFileOffset;
	
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
	public void sendMessage(ByteBuf byteBuf) {
		
		if(clientRole == CLIENT_ROLE.SLAVE && slaveState == SLAVE_STATE.REDIS_REPL_ONLINE){
			return;
		}
		channel.writeAndFlush(byteBuf);
	}

	@Override
	public void sendMessage(byte[] bytes) {
		
		sendMessage(Unpooled.wrappedBuffer(bytes));
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

	@Override
	public String info() {
		

		String info = "";
		switch(clientRole){
			case NORMAL:
				break;
			case SLAVE:
				long lag = System.currentTimeMillis() - replAckTime;
				info = String.format(
						"ip=%s,port=%d,state=%s,offset=%d,lag=%d" ,
						IpUtils.getIp(channel.remoteAddress()), ((InetSocketAddress)channel.remoteAddress()).getPort(), 
						slaveState != null ? slaveState.getDesc() : "null",
						replAckOff, lag/1000);
			default:
				break;
		}
		return info;
	}

	@Override
	public void writeFile(FileChannel fileChannel, long pos, long len) {
		
		channel.writeAndFlush(new NotClosableFileRegion(fileChannel, pos, len));
	}

	@Override
	public void close() throws IOException {
		channel.close();
	}
}
