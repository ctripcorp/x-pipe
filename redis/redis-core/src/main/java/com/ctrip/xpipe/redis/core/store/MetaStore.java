package com.ctrip.xpipe.redis.core.store;

import java.io.IOException;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;

/**
 * @author marsqing
 *
 * Jul 26, 2016 11:21:27 AM
 */
public interface MetaStore {

	public static final String META_FILE = "meta.json";
	
	public static final String METHOD_BECOME_ACTIVE = "becomeActive";
	
	public static final String METHOD_BECOME_BACKUP = "becomeBackup";

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
	
	void saveKinfo(ReplicationStoreMeta replicationStoreMeta) throws IOException;
	
	void psyncBegun(String masterRunid, long keeperBeginOffset) throws IOException;
	
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
	
	void setKeeperState(String keeperRunid, KeeperState keeperState) throws IOException;

	ReplicationStoreMeta rdbBegun(String masterRunid, long beginOffset, String rdbFile, EofType eofType, String cmdFilePrefix) throws IOException;

	void setRdbFileSize(long rdbFileSize) throws IOException;
	
	/**
	 * redis failover
	 * @param newMasterEndPoint
	 * @param newMasterId
	 * @param offsetdelta  newBeginOffset = beginoffset + delta
	 * @throws IOException 
	 */
	void masterChanged(long keeperOffset, DefaultEndPoint newMasterEndpoint, String newMasterRunid, long newMasterReplOffset) throws IOException;

	ReplicationStoreMeta rdbUpdated(String rdbFile, EofType eofType, long masterOffset) throws IOException;
	
	long redisOffsetToKeeperOffset(long redisOffset);
	
	void updateKeeperRunid(String keeperRunid) throws IOException;

	boolean isFresh();
}
