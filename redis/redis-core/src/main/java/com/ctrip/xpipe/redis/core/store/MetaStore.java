/**
 * 
 */
package com.ctrip.xpipe.redis.core.store;

import java.io.IOException;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;

/**
 * @author marsqing
 *
 * Jul 26, 2016 11:21:27 AM
 */
public interface MetaStore {

	String getMasterRunid();
	
	/**
	 * the first byte offset,
	 * 
	 * @return
	 */
	Long beginOffset();
	
	void setMasterAddress(DefaultEndPoint endpoint) throws IOException;
	
	DefaultEndPoint getMasterAddress();
	
	long getKeeperBeginOffset();
	
	ReplicationStoreMeta dupReplicationStoreMeta();
	
	void loadMeta() throws IOException;
	
	void saveMeta(String name, ReplicationStoreMeta replicationStoreMeta) throws IOException;

	void psyncBegun(String masterRunid, long offset) throws IOException;
	
	/**
	 * keeper backup -> active
	 * @param name
	 * @throws IOException
	 */
	void becomeActive() throws IOException;
	
	/**
	 * keeper active -> backup
	 * @throws IOException 
	 */
	void becomeBackup() throws IOException;

	ReplicationStoreMeta rdbBegun(String masterRunid, long beginOffset, String rdbFile, long rdbFileSize, String cmdFilePrefix) throws IOException;

	/**
	 * redis failover
	 * @param newMasterEndPoint
	 * @param newMasterId
	 * @param offsetdelta  newBeginOffset = beginoffset + delta
	 * @throws IOException 
	 */
	void masterChanged(long keeperOffset, DefaultEndPoint newMasterEndpoint, String newMasterRunid, long newMasterReplOffset) throws IOException;

	ReplicationStoreMeta rdbUpdated(String rdbFile, long rdbFileSize, long masterOffset) throws IOException;
	
	long redisOffsetToKeeperOffset(long redisOffset);
	
	void updateKeeperRunid(String keeperRunid) throws IOException;
}
