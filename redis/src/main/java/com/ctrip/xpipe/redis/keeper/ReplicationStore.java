package com.ctrip.xpipe.redis.keeper;

import java.io.Closeable;

import io.netty.buffer.ByteBuf;
import io.netty.channel.FileRegion;

/**
 * @author wenchao.meng
 *
 * 2016年4月19日 下午3:43:56
 */
public interface ReplicationStore extends Closeable{

	void beginRdb(String masterRunid, long masterOffset);
	
	void writeRdb(ByteBuf byteBuffer);
	
	void endRdb();
	
	String getMasterRunid();
	
	/**
	 * Zero copy support
	 * @return
	 */
	FileRegion getRdbFile();
	

	void appendCommands(ByteBuf byteBuf);
	
	
	void addCommandsListener(long offset, CommandsListener commandsListener);
	
	void removeCommandsListener(CommandsListener commandsListener);
	
	
	/**
	 * the first byte offset,
	 * @return
	 */
	long beginOffset();
	
	/**
	 * the last byte offset
	 * @return
	 */
	long endOffset();
	
	
	
	/**
	 * delete all the data saved!
	 */
	void delete();

}
