package com.ctrip.xpipe.redis.keeper;

import java.io.Closeable;
import java.io.IOException;

import io.netty.buffer.ByteBuf;
import io.netty.channel.FileRegion;

/**
 * @author wenchao.meng
 *
 * 2016年4月19日 下午3:43:56
 */
public interface ReplicationStore extends Closeable{

	void beginRdb(String masterRunid, long masterOffset);
	
	int writeRdb(ByteBuf byteBuffer) throws IOException;
	
	void endRdb();
	
	String getMasterRunid();
	
	/**
	 * Zero copy support
	 * @return
	 */
	FileRegion getRdbFile();
	

	int appendCommands(ByteBuf byteBuf) throws IOException;
	
	
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
