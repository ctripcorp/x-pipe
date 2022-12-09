package com.ctrip.xpipe.redis.core.store;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;

import java.io.IOException;

/**
 * @author marsqing
 *
 * Jul 26, 2016 11:21:27 AM
 */
public interface MetaStore {

	public static final String META_FILE = "meta.json";
	
	public static final String METHOD_BECOME_ACTIVE = "becomeActive";
	
	public static final String METHOD_BECOME_BACKUP = "becomeBackup";

	String getReplId();
	
	String getReplId2();
	
	Long getSecondReplIdOffset();
	
	ReplicationStoreMeta shiftReplicationId(String newReplId, Long currentOffset) throws IOException;
	
	/**
	 * the first byte offset,
	 * 
	 * @return
	 */
	Long beginOffset();
	
	void setMasterAddress(DefaultEndPoint endpoint) throws IOException;
	
	DefaultEndPoint getMasterAddress();
	
	ReplicationStoreMeta dupReplicationStoreMeta();
	
	void loadMeta() throws IOException;
		
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
	
	ReplicationStoreMeta rdbBegun(String replId, long beginOffset, String rdbFile, EofType eofType, String cmdFilePrefix) throws IOException;

	boolean attachRdbGtidSet(String rdbFile, String gtidSet) throws IOException;

	ReplicationStoreMeta continueFromOffset(String replId, long beginOffset, String cmdFilePrefix) throws IOException;

	void setRdbFileSize(long rdbFileSize) throws IOException;

	@Deprecated
	void masterChanged(long keeperOffset, DefaultEndPoint newMasterEndpoint, String newMasterRunid, long newMasterReplOffset) throws IOException;

	ReplicationStoreMeta checkReplIdAndUpdateRdbInfo(String rdbFile, EofType eofType, long rdbOffset, String expectedReplId) throws IOException;
	
	void updateKeeperRunid(String keeperRunid) throws IOException;

	boolean isFresh();

	void releaseRdbFile(String rdbFile) throws IOException ;
}
