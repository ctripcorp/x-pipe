package com.ctrip.xpipe.redis.keeper;

import java.io.Closeable;
import java.io.IOException;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;

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
	void masterChanged(DefaultEndPoint newMasterEndpoint, String newMasterRunid, long offsetdelta);
	
	void setMasterAddress(DefaultEndPoint endpoint);
	
	DefaultEndPoint getMasterAddress();
	
	void setKeeperBeginOffset(long offset);
	
	long getKeeperBeginOffset();
	
	void setActive(boolean active);
	
	boolean isActive();
	
	ReplicationStoreMeta getReplicationStoreMeta();
	
	ReplicationStoreMeta getReplicationStoreMeta(String name) throws IOException;
	
	void saveMeta(String name, ReplicationStoreMeta replicationStoreMeta) throws IOException;
	
	void changeMetaTo(String name) throws IOException;
	
	/**
	 * check if this store is newly borned
	 * @return
	 */
	boolean isFresh();
}
