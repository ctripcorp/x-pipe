package com.ctrip.xpipe.redis.keeper;

import java.io.Closeable;
import java.io.IOException;

import com.ctrip.xpipe.api.endpoint.Endpoint;

import io.netty.buffer.ByteBuf;

/**
 * @author wenchao.meng
 *
 *         2016年4月19日 下午3:43:56
 */
public interface ReplicationStore extends Closeable {

	void beginRdb(String masterRunid, long masterOffset, long rdbFileSize) throws IOException;

	int writeRdb(ByteBuf byteBuffer) throws IOException;

	void endRdb() throws IOException;

	String getMasterRunid();

	/**
	 * @param rdbFileListener
	 * @return 	masterOffset
	 * @throws IOException
	 */
	void readRdbFile(RdbFileListener rdbFileListener) throws IOException;
	
	int appendCommands(ByteBuf byteBuf) throws IOException;

	void addCommandsListener(long offset, CommandsListener commandsListener) throws IOException;

	void removeCommandsListener(CommandsListener commandsListener);

	/**
	 * the first byte offset,
	 * 
	 * @return
	 */
	long beginOffset();

	/**
	 * the last byte offset
	 * 
	 * @return
	 */
	long endOffset();

	/**
	 * delete all the data saved!
	 */
	void delete();

	/**
	 * @param newMasterEndPoint
	 * @param newMasterId
	 * @param offsetdelta  newBeginOffset = beginoffset + delta
	 */
	void masterChanged(Endpoint newMasterEndpoint, String newMasterRunid, long offsetdelta);
	
	void setMasterAddress(Endpoint endpoint);
	
	Endpoint getMasterAddress();
}
