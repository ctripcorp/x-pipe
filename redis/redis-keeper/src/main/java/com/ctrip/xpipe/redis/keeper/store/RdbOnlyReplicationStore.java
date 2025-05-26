package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.store.*;
import com.ctrip.xpipe.tuple.Pair;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * @author marsqing
 *
 *         Jul 22, 2016 10:40:00 AM
 */
public class RdbOnlyReplicationStore implements ReplicationStore {

	private DumpedRdbStore dumpedRdbStore;
	private String replId;
	private long rdbOffset;
	private MetaStore metaStore;

	public RdbOnlyReplicationStore(DumpedRdbStore dumpedRdbStore) {
		this.dumpedRdbStore = dumpedRdbStore;
		metaStore = new MetaStore() {

			@Override
			public ReplStage getPreReplStage() {
				throw new UnsupportedOperationException();
			}

			@Override
			public ReplStage getCurrentReplStage() {
				return null;
			}

			@Override
			public ReplicationStoreMeta rdbConfirm(String replId, long beginOffset, String gtidSet, String rdbFile,
												   RdbStore.Type type, EofType eofType, String cmdFilePrefix) throws IOException {
				throw new UnsupportedOperationException();
			}

			@Override
			public ReplicationStoreMeta checkReplIdAndUpdateRdbInfo(String rdbFile, RdbStore.Type type, EofType eofType,
																	long rdbOffset, String gtidSet, String expectedReplId) throws IOException {
				throw new UnsupportedOperationException();
			}

			@Override
			public void setMasterAddress(DefaultEndPoint endpoint) {
				throw new UnsupportedOperationException();
			}

			@Override
			public void loadMeta() throws IOException {
				throw new UnsupportedOperationException();
			}

			@Override
			public String getReplId() {
				return replId;
			}
			
			@Override
			public String getReplId2() {
				throw new UnsupportedOperationException();
			}
			
			@Override
			public Long getSecondReplIdOffset() {
				throw new UnsupportedOperationException();
			}
			
			@Override
			public ReplicationStoreMeta shiftReplicationId(String newReplId, Long currentOffset) throws IOException {
				throw new UnsupportedOperationException();
			}

			@Override
			public DefaultEndPoint getMasterAddress() {
				throw new UnsupportedOperationException();
			}

			@Override
			public ReplicationStoreMeta dupReplicationStoreMeta() {
				throw new UnsupportedOperationException();
			}

			@Override
			public Long beginOffset() {
				throw new UnsupportedOperationException();
			}

			@Override
			public void masterChanged(long keeperOffset, DefaultEndPoint newMasterEndpoint, String newMasterRunid, long newMasterReplOffset)
					throws IOException {
				throw new UnsupportedOperationException();
			}

			@Override
			public void becomeBackup() throws IOException {
				throw new UnsupportedOperationException();
			}

			@Override
			public void becomeActive() throws IOException {
				throw new UnsupportedOperationException();
			}

			@Override
			public void updateKeeperRunid(String keeperRunid) throws IOException {
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean isFresh() {
				return true;
			}

			@Override
			public ReplicationStoreMeta rdbBegun(String masterRunid, long beginOffset, String rdbFile, EofType eofType,
					String cmdFilePrefix) throws IOException {
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean attachRdbGtidSet(String rdbFile, String gtidSet) throws IOException {
				throw new UnsupportedOperationException();
			}

			@Override
			public ReplicationStoreMeta continueFromOffset(String replId, long beginOffset, String cmdFilePrefix) throws IOException {
				throw new UnsupportedOperationException();
			}

			@Override
			public ReplicationStoreMeta checkReplIdAndUpdateRdbInfo(String rdbFile, EofType eofType, long masterOffset, String expectedReplId)
					throws IOException {
				throw new UnsupportedOperationException();
			}

			
			@Override
			public void setRdbFileSize(long rdbFileSize) throws IOException {
			}

			@Override
			public void setRordbFileSize(long rordbFileSize) throws IOException {
			}

			@Override
			public void releaseRdbFile(String rdbFile) throws IOException {
				throw new UnsupportedOperationException();
			}

			@Override
			public String getCurReplStageReplId() {
				throw new UnsupportedOperationException();
			}

			@Override
			public Long backlogOffsetToReplOffset(Long backlogOffset) {
				throw new UnsupportedOperationException();
			}

			@Override
			public Long replOffsetToBacklogOffset(Long replOff) {
				throw new UnsupportedOperationException();
			}

			@Override
			public ReplicationStoreMeta rdbConfirmPsync(String replId, long replOff, long backlogOff, String rdbFile, RdbStore.Type type, EofType eofType, String cmdFilePrefix) throws IOException {
				throw new UnsupportedOperationException();
			}

			@Override
			public ReplicationStoreMeta psyncContinueFrom(String replId, long beginOffset, long backlogOff, String cmdFilePrefix) throws IOException {
				throw new UnsupportedOperationException();
			}

			@Override
			public ReplicationStoreMeta psyncContinue(String newReplId, long backlogOff) throws IOException {
				throw new UnsupportedOperationException();
			}

			@Override
			public ReplicationStoreMeta switchToPsync(String replId, long replOff, long backlogOff) throws IOException {
				throw new UnsupportedOperationException();
			}

			@Override
			public ReplicationStoreMeta rdbConfirmXsync(String replId, long replOff, long backlogOff, String masterUuid, GtidSet gtidLost, GtidSet gtidExecuted, String rdbFile, RdbStore.Type type, EofType eofType, String cmdFilePrefix) throws IOException {
				throw new UnsupportedOperationException();
			}

			@Override
			public ReplicationStoreMeta xsyncContinueFrom(String replId, long beginReplOffset, long backlogOff, String masterUuid, GtidSet gtidLost, GtidSet gtidExecuted, String cmdFilePrefix) throws IOException {
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean xsyncContinue(String replId, long replOff, long backlogOff, String masterUuid, GtidSet gtidCont, GtidSet gtidIndexed) throws IOException {
				throw new UnsupportedOperationException();
			}

			@Override
			public ReplicationStoreMeta switchToXsync(String replId, long replOff, long backlogOff, String masterUuid, GtidSet gtidCont) throws IOException {
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean increaseLost(GtidSet lost) throws IOException {
				throw new UnsupportedOperationException();
			}

			@Override
			public UPDATE_RDB_RESULT checkReplIdAndUpdateRdbInfoPsync(String rdbFile, RdbStore.Type type, EofType eofType, long rdbOffset, String rdbReplId, long backlogBeginOffset, long backlogEndOffset) throws IOException {
				throw new UnsupportedOperationException();
			}

			@Override
			public UPDATE_RDB_RESULT checkReplIdAndUpdateRdbInfoXsync(String rdbFile, RdbStore.Type type, EofType eofType, long rdbOffset, String rdbReplId, String rdbMasterUuid,  GtidSet rdbGtidExecuted, GtidSet rdbGtidLost, long backlogBeginOffset, long backlogEndOffset, long indexedOffsetBacklog, GtidSet indexedGtidSet) throws IOException {
				throw new UnsupportedOperationException();
			}
		};
	}

	@Override
	public XSyncContinue locateContinueGtidSet(GtidSet gtidSet) {
		throw new UnsupportedOperationException();
	}

	@Override
	public RdbStore prepareRdb(String replId, long rdbOffset, EofType eofType, ReplStage.ReplProto replProto,
							   GtidSet gtidLost, String masterUuid) throws IOException {
		prepareRdb(replId, rdbOffset, eofType);
		dumpedRdbStore.setReplProto(replProto);
		dumpedRdbStore.setGtidLost(gtidLost == null ? null : gtidLost.toString());
		dumpedRdbStore.setMasterUuid(masterUuid);
		return dumpedRdbStore;
	}

	@Override
	public void confirmRdbGapAllowed(RdbStore rdbStore) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void psyncContinueFrom(String replId, long continueOffset) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public UPDATE_RDB_RESULT checkReplIdAndUpdateRdbGapAllowed(RdbStore rdbStore) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void switchToPSync(String replId, long offset) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void psyncContinue(String replId) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void xsyncContinueFrom(String replId, long replOff, String masterUuid, GtidSet gtidCont, GtidSet gtidLost) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void switchToXSync(String replId, long replOff, String masterUuid, GtidSet gtidSet) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean xsyncContinue(String replId, long replOff, String masterUuid, GtidSet gtidCont) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean increaseLost(GtidSet lost) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public long getCurReplStageReplOff() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Pair<GtidSet, GtidSet> getGtidSet() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void checkReplId(String expectReplId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void confirmRdb(RdbStore rdbStore) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void checkReplIdAndUpdateRdb(RdbStore rdbStore) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public FULLSYNC_FAIL_CAUSE fullSyncIfPossible(FullSyncListener fullSyncListener, boolean masterSupportRordb) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void close() throws IOException {
		dumpedRdbStore.close();
	}

	@Override
	public void destroy() throws Exception {
		dumpedRdbStore.destroy();
	}

	@Override
	public RdbStore prepareRdb(String replId, long rdbOffset, EofType eofType) throws IOException {
		this.replId = replId;
		this.rdbOffset = rdbOffset;
		dumpedRdbStore.setReplId(replId);
		dumpedRdbStore.setRdbOffset(this.rdbOffset);
		dumpedRdbStore.setEofType(eofType);
		return dumpedRdbStore;
	}

	@Override
	public long getEndOffset() {
		return -1L;
	}
	
	@Override
	public long firstAvailableOffset() {
		throw new UnsupportedOperationException();
	}

	@Override
	public long backlogBeginOffset() {
		throw new UnsupportedOperationException();
	}

	@Override
	public long backlogEndOffset() {
		throw new UnsupportedOperationException();
	}

	@Override
	public MetaStore getMetaStore() {
		return metaStore;
	}

	@Override
	public boolean gc() {
		return true;
	}

	@Override
	public FULLSYNC_FAIL_CAUSE fullSyncIfPossible(FullSyncListener fullSyncListener) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isFresh() {
		return true;
	}

	@Override
	public int appendCommands(ByteBuf byteBuf) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean awaitCommandsOffset(long offset, int timeMilli) throws InterruptedException {
		throw new UnsupportedOperationException();
	}

	@Override
	public GtidSet getBeginGtidSet() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public GtidSet getEndGtidSet() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean supportGtidSet() {
		throw new UnsupportedOperationException();
	}

	@Override
	public DumpedRdbStore prepareNewRdb() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void addCommandsListener(ReplicationProgress<?> progress, CommandsListener commandsListener) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean checkOk() {
		return dumpedRdbStore.checkOk();
	}

	@Override
	public long beginOffsetWhenCreated() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void shiftReplicationId(String newReplId) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void continueFromOffset(String replId, long continueOffset) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public long lastReplDataUpdatedAt() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void releaseRdb() throws IOException {

	}
}
