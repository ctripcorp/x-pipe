package com.ctrip.xpipe.redis.keeper.impl;



import java.util.HashSet;
import java.util.Set;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.observer.AbstractObservable;
import com.ctrip.xpipe.payload.ByteArrayOutputStreamPayload;
import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.ArrayParser;
import com.ctrip.xpipe.redis.core.protocal.protocal.SimpleStringParser;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisSlave;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

/**
 * @author wenchao.meng
 *
 * 2016年4月22日 上午11:30:49
 */
public class DefaultRedisClient extends AbstractObservable implements RedisClient{
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	private Set<CAPA>  capas = new HashSet<CAPA>(); 

	private int slaveListeningPort;
	
	protected Channel channel;
	
	protected RedisKeeperServer redisKeeperServer;
	
	private CLIENT_ROLE clientRole = CLIENT_ROLE.NORMAL;
	
	public DefaultRedisClient(Channel channel, RedisKeeperServer redisKeeperServer) {

		this.channel = channel;
		this.redisKeeperServer = redisKeeperServer;
	}

	public DefaultRedisClient(DefaultRedisClient redisClient) {
		this.channel = redisClient.channel;
		this.redisKeeperServer = redisClient.redisKeeperServer;
	}

	@Override
	public RedisKeeperServer getRedisKeeperServer() {
		return redisKeeperServer;
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
		return "";
	}

	@Override
	public void close() {
		channel.close();
	}
	
	@Override
	public RedisSlave becomeSlave() {
		
		RedisSlave redisSlave = null;
		switch(clientRole){
			case NORMAL:
				logger.info("[becomeSlave]" + this);
				redisSlave = new DefaultRedisSlave(this); 
				notifyObservers(redisSlave);
				break;
			case SLAVE:
				logger.info("[becomeSlave][already slave]" + this);
				break;
			default:
				throw new IllegalStateException("unknown state:" + clientRole);
		}
		return redisSlave;
	}

	@Override
	public Channel channel() {
		return channel;
	}

	@Override
	public void sendMessage(ByteBuf byteBuf) {
		
		channel.writeAndFlush(byteBuf);
	}
	
	@Override
	public void sendMessage(byte[] bytes) {
		
		sendMessage(Unpooled.wrappedBuffer(bytes));
	}
	
	public void addChannelCloseReleaseResources(final Releasable releasable){
		
		channel.closeFuture().addListener(new ChannelFutureListener() {
			
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				
				logger.info("[channel close][release resource]{}", releasable);
				releasable.release();
			}
		});
	}
}
