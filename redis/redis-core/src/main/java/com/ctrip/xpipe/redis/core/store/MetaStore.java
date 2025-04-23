package com.ctrip.xpipe.redis.core.store;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.gtid.GtidSet;
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

	ReplStage getPreReplStage();

	ReplStage getCurrentReplStage();

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

	ReplicationStoreMeta rdbConfirm(String replId, long beginOffset, String gtidSet, String rdbFile, RdbStore.Type type, EofType eofType, String cmdFilePrefix) throws IOException;

	ReplicationStoreMeta rdbBegun(String replId, long beginOffset, String rdbFile, EofType eofType, String cmdFilePrefix) throws IOException;

	boolean attachRdbGtidSet(String rdbFile, String gtidSet) throws IOException;

	ReplicationStoreMeta continueFromOffset(String replId, long beginOffset, String cmdFilePrefix) throws IOException;

	void setRdbFileSize(long rdbFileSize) throws IOException;

	void setRordbFileSize(long rordbFileSize) throws IOException;

	@Deprecated
	void masterChanged(long keeperOffset, DefaultEndPoint newMasterEndpoint, String newMasterRunid, long newMasterReplOffset) throws IOException;

	ReplicationStoreMeta checkReplIdAndUpdateRdbInfo(String rdbFile, RdbStore.Type type, EofType eofType, long rdbOffset, String gtidSet, String expectedReplId) throws IOException;

	ReplicationStoreMeta checkReplIdAndUpdateRdbInfo(String rdbFile, EofType eofType, long rdbOffset, String expectedReplId) throws IOException;
	
	void updateKeeperRunid(String keeperRunid) throws IOException;

	boolean isFresh();

	void releaseRdbFile(String rdbFile) throws IOException ;

	String getCurReplStageReplId();

	ReplicationStoreMeta rdbConfirmPsync(String replId, long beginReplOffset, long backlogOff, String rdbFile, RdbStore.Type type, EofType eofType, String cmdFilePrefix) throws IOException;

	ReplicationStoreMeta psyncContinue(String newReplId, long backlogOff) throws IOException;

	ReplicationStoreMeta switchToPsync(String replId, long beginReplOffset, long backlogOff) throws IOException;

	ReplicationStoreMeta rdbConfirmXsync(String replId, long beginReplOffset, long backlogOff, String masterUuid, GtidSet gtidLost, GtidSet gtidExecuted, String rdbFile, RdbStore.Type type, EofType eofType, String cmdFilePrefix) throws IOException;

	boolean xsyncContinue(String replId, long beginReplOffset, long backlogOff, String masterUuid, GtidSet gtidCont, GtidSet gtidIndexed) throws IOException;

	ReplicationStoreMeta switchToXsync(String replId, long beginReplOffset, long backlogOff, String masterUuid, GtidSet gtidCont) throws IOException;

	UPDATE_RDB_RESULT checkReplIdAndUpdateRdbInfoPsync(String rdbFile, RdbStore.Type type, EofType eofType, long rdbOffset, long backlogBeginOffset, long backlogEndOffset, String expectedReplId) throws IOException;

	UPDATE_RDB_RESULT checkReplIdAndUpdateRdbInfoXsync(String rdbFile, RdbStore.Type type, EofType eofType, long rdbOffset, long rdbBacklogOffset, long backlogBeginOffset, long backlogEndOffset, String gtidSet, String expectedReplId, String exepectedMasterUuid) throws IOException;
}
