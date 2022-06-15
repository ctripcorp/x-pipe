package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.store.*;
import io.netty.buffer.ByteBuf;

import java.io.IOException;

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
			public void releaseRdbFile(String rdbFile) throws IOException {
				throw new UnsupportedOperationException();
			}
		};
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public RdbStore beginRdb(String replId, long rdbOffset, EofType eofType) throws IOException {
		this.replId = replId;
		this.rdbOffset = rdbOffset;
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
	public void destroy() {
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
	public DumpedRdbStore prepareNewRdb() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void checkReplIdAndUpdateRdb(DumpedRdbStore dumpedRdbStore, String expectedReplId) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void checkAndUpdateRdbGtidSet(RdbStore rdbStore, String rdbGtidSet) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void addCommandsListener(ReplicationProgress<?,?> progress, CommandsListener commandsListener) throws IOException {
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
}
