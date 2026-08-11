package com.ctrip.xpipe.redis.core.store;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.tuple.Pair;

import java.io.IOException;

/**
 * @author marsqing
 *
 * Jul 26, 2016 11:21:27 AM
 */
public interface MetaStore {

	public static final String META_V1_FILE = "meta.json";

	public static final String META_V2_FILE = "meta.v2.json";

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

	/**
	 * Build rdbConfirm meta without persisting. Used by Cmd-first confirm (Phase H2.A1).
	 *
	 * @return Pair(expectedCurrent, preparedFuture). {@code expectedCurrent} is the live
	 *         {@code metaRef} identity for subsequent {@link #saveMeta(ReplicationStoreMeta, ReplicationStoreMeta)} CAS;
	 *         {@code preparedFuture} is a mutable dup to persist after Cmd is ready.
	 */
	Pair<ReplicationStoreMeta, ReplicationStoreMeta> prepareRdbConfirm(String replId, long beginOffset, String gtidSet, String rdbFile, RdbStore.Type type, EofType eofType, String cmdFilePrefix);

	/**
	 * CAS persist: succeed only if the in-memory meta identity still equals {@code expectedOld}.
	 * Disk first, then memory. On mismatch returns {@code false} without writing.
	 * Unconditional overwrite is not part of this interface (internal to MetaStore impl only).
	 */
	boolean saveMeta(ReplicationStoreMeta expectedOld, ReplicationStoreMeta newMeta) throws IOException;

	ReplicationStoreMeta rdbBegun(String replId, long beginOffset, String rdbFile, EofType eofType, String cmdFilePrefix) throws IOException;

	boolean attachRdbGtidSet(String rdbFile, String gtidSet) throws IOException;

	ReplicationStoreMeta continueFromOffset(String replId, long beginOffset, String cmdFilePrefix) throws IOException;

	/**
	 * Build continueFromOffset meta without persisting. Used by Cmd-first continue (Phase H2.A2).
	 *
	 * @return Pair(expectedCurrent, preparedFuture); see {@link #prepareRdbConfirm}.
	 */
	Pair<ReplicationStoreMeta, ReplicationStoreMeta> prepareContinueFromOffset(String replId, long beginOffset, String cmdFilePrefix);

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

	Long backlogOffsetToReplOffset(Long backlogOffset);

	Long replOffsetToBacklogOffset(Long replOff);

	ReplicationStoreMeta rdbConfirmPsync(String replId, long beginReplOffset, long backlogOff, String rdbFile, RdbStore.Type type, EofType eofType, String cmdFilePrefix) throws IOException;

	/**
	 * Build rdbConfirmPsync meta without persisting. Used by Cmd-first confirm (Phase H2.A1).
	 *
	 * @return Pair(expectedCurrent, preparedFuture); see {@link #prepareRdbConfirm}.
	 */
	Pair<ReplicationStoreMeta, ReplicationStoreMeta> prepareRdbConfirmPsync(String replId, long beginReplOffset, long backlogOff, String rdbFile, RdbStore.Type type, EofType eofType, String cmdFilePrefix);

	ReplicationStoreMeta psyncContinueFrom(String replId, long beginReplOffset, long backlogOff, String cmdFilePrefix) throws IOException;

	/**
	 * Build psyncContinueFrom meta without persisting. Used by Cmd-first continue (Phase H2.A2).
	 *
	 * @return Pair(expectedCurrent, preparedFuture); see {@link #prepareRdbConfirm}.
	 */
	Pair<ReplicationStoreMeta, ReplicationStoreMeta> preparePsyncContinueFrom(String replId, long beginReplOffset, long backlogOff, String cmdFilePrefix);

	ReplicationStoreMeta psyncContinue(String newReplId, long backlogOff) throws IOException;

	/**
	 * Build psyncContinue meta without persisting. Used by Cmd-first protocol update (Phase H2.B2).
	 *
	 * @return Pair(expectedCurrent, preparedFuture), or {@code null} when newReplId equals current (no-op)
	 */
	Pair<ReplicationStoreMeta, ReplicationStoreMeta> preparePsyncContinue(String newReplId, long backlogOff);

	ReplicationStoreMeta switchToPsync(String replId, long beginReplOffset, long backlogOff) throws IOException;

	/**
	 * Build switchToPsync meta without persisting. Used by Cmd-first protocol switch (Phase H2.B2).
	 *
	 * @return Pair(expectedCurrent, preparedFuture); see {@link #prepareRdbConfirm}.
	 */
	Pair<ReplicationStoreMeta, ReplicationStoreMeta> prepareSwitchToPsync(String replId, long beginReplOffset, long backlogOff);

	ReplicationStoreMeta rdbConfirmXsync(String replId, long beginReplOffset, long backlogOff, String masterUuid, GtidSet gtidLost, GtidSet gtidExecuted, String rdbFile, RdbStore.Type type, EofType eofType, String cmdFilePrefix) throws IOException;

	/**
	 * Build rdbConfirmXsync meta without persisting. Used by Cmd-first confirm (Phase H2.A1).
	 *
	 * @return Pair(expectedCurrent, preparedFuture); see {@link #prepareRdbConfirm}.
	 */
	Pair<ReplicationStoreMeta, ReplicationStoreMeta> prepareRdbConfirmXsync(String replId, long beginReplOffset, long backlogOff, String masterUuid, GtidSet gtidLost, GtidSet gtidExecuted, String rdbFile, RdbStore.Type type, EofType eofType, String cmdFilePrefix);

	ReplicationStoreMeta xsyncContinueFrom(String replId, long beginReplOffset, long backlogOff, String masterUuid, GtidSet gtidLost, GtidSet gtidExecuted, String cmdFilePrefix) throws IOException;

	/**
	 * Build xsyncContinueFrom meta without persisting. Used by Cmd-first continue (Phase H2.A3).
	 *
	 * @return Pair(expectedCurrent, preparedFuture); see {@link #prepareRdbConfirm}.
	 */
	Pair<ReplicationStoreMeta, ReplicationStoreMeta> prepareXsyncContinueFrom(String replId, long beginReplOffset, long backlogOff, String masterUuid, GtidSet gtidLost, GtidSet gtidExecuted, String cmdFilePrefix);

	boolean increaseLost(GtidSet lost) throws IOException;

	long removeLost(GtidSet gtidSet) throws IOException;

	long increaseExecuted(GtidSet gtidSet) throws IOException;

	boolean xsyncContinue(String replId, long beginReplOffset, long backlogOff, String masterUuid, GtidSet gtidCont, GtidSet gtidIndexed) throws IOException;

	/**
	 * Build switchToXsync meta without persisting. Used by Cmd-first protocol switch (Phase H2.B1).
	 *
	 * @return Pair(expectedCurrent, preparedFuture); see {@link #prepareRdbConfirm}.
	 */
	Pair<ReplicationStoreMeta, ReplicationStoreMeta> prepareSwitchToXsync(String replId, long beginReplOffset, long backlogOff,
																		  String masterUuid, GtidSet gtidCont, GtidSet gtidLost);

	ReplicationStoreMeta switchToXsync(String replId, long beginReplOffset, long backlogOff, String masterUuid, GtidSet gtidCont, GtidSet gtidLost) throws IOException;

	UPDATE_RDB_RESULT checkReplIdAndUpdateRdbInfoPsync(String rdbFile, RdbStore.Type type, EofType eofType, long rdbOffset, String rdbReplId, long backlogBeginOffset, long backlogEndOffset) throws IOException;

	UPDATE_RDB_RESULT checkReplIdAndUpdateRdbInfoXsync(String rdbFile, RdbStore.Type type, EofType eofType, long rdbOffset, String rdbReplId, String rdbMasterUuid, GtidSet rdbGtidExecuted, GtidSet rdbGtidLost, long backlogBeginOffset, long backlogEndOffset, long indexedOffsetBacklog, GtidSet indexedGtidSet) throws IOException;

	GtidCmdFilter generateGtidCmdFilter();

	/**
	 * Release async file handles held by this MetaStore.
	 * Default no-op for implementations that do not hold resources.
	 */
	default void close() throws IOException {
	}

	/**
	 * Close handles and delete persisted meta files.
	 * Default implementation only releases handles.
	 */
	default void destroy() throws Exception {
		close();
	}
}
