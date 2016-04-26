package com.ctrip.xpipe.redis.keeper;

import io.netty.buffer.ByteBuf;

/**
 * @author wenchao.meng
 *
 * 2016年4月22日 上午11:25:07
 */
public interface RedisClient {
	
	public static enum CLIENT_ROLE{
		NORMAL,
		SLAVE
	}
	
	public static enum CAPA{
		
		EOF;
		
		public static CAPA of(String capaString){
			
			if("eof".equalsIgnoreCase(capaString)){
				return EOF;
			}
			throw new IllegalArgumentException("unsupported capa type:" + capaString);
		}
	}
	
	public static enum SLAVE_STATE{
		REDIS_REPL_SEND_BULK,
		REDIS_REPL_ONLINE
		
	}
	
	CLIENT_ROLE getClientRole();
	
	RedisKeeperServer getRedisKeeperServer();

	void setClientRole(CLIENT_ROLE clientState);
	
	void setSlaveListeningPort(int port);

	int getSlaveListeningPort();

	void capa(CAPA capa);
	
	void setSlaveState(SLAVE_STATE slaveState);
	
	SLAVE_STATE getSlaveState();

	void ack(Long valueOf);
	
	Long getAck();
	
	Long getAckTime();
	
	void sendMessage(byte []message);
	
	void writeRdb(RdbFile rdbFile);
	
	void beginWriteCommands(long beginOffset);

	void writeComplete(RdbFile rdbFile);
	
	String []readCommands(ByteBuf byteBuf);
	
}
