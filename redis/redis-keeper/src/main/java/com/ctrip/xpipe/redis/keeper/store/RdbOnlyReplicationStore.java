package com.ctrip.xpipe.redis.keeper.store;

import java.io.IOException;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.store.CommandsListener;
import com.ctrip.xpipe.redis.core.store.DumpedRdbStore;
import com.ctrip.xpipe.redis.core.store.FullSyncListener;
import com.ctrip.xpipe.redis.core.store.MetaStore;
import com.ctrip.xpipe.redis.core.store.RdbStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreMeta;

import io.netty.buffer.ByteBuf;

/**
 * @author marsqing
 *
 *         Jul 22, 2016 10:40:00 AM
 */
public class RdbOnlyReplicationStore implements ReplicationStore {

	private DumpedRdbStore dumpedRdbStore;
	private DefaultRdbStore rdbStore;
	private String masterRunid;
	private long masterOffset;
	private MetaStore metaStore;

	public RdbOnlyReplicationStore(DumpedRdbStore dumpedRdbStore) {
		this.dumpedRdbStore = dumpedRdbStore;
		metaStore = new MetaStore() {

			@Override
			public void setMasterAddress(DefaultEndPoint endpoint) {
				throw new UnsupportedOperationException();
			}

			@Override
			public void saveKinfo(ReplicationStoreMeta replicationStoreMeta) throws IOException {
				throw new UnsupportedOperationException();
			}

			@Override
			public void loadMeta() throws IOException {
				throw new UnsupportedOperationException();
			}

			@Override
			public String getMasterRunid() {
				return masterRunid;
			}

			@Override
			public DefaultEndPoint getMasterAddress() {
				throw new UnsupportedOperationException();
			}

			@Override
			public long getKeeperBeginOffset() {
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
			public ReplicationStoreMeta rdbUpdated(String rdbFile, long rdbFileSize, long masterOffset) throws IOException {
				throw new UnsupportedOperationException();
			}

			@Override
			public ReplicationStoreMeta rdbBegun(String masterRunid, long beginOffset, String rdbFile, long rdbFileSize, String cmdFilePrefix)
					throws IOException {
				return null;
			}

			@Override
			public long redisOffsetToKeeperOffset(long redisOffset) {
				return 0;
			}

			@Override
			public void becomeActive() throws IOException {
				throw new UnsupportedOperationException();
			}

			@Override
			public void psyncBegun(String keeperRunid, long offset) throws IOException {
				throw new UnsupportedOperationException();
			}

			@Override
			public void updateKeeperRunid(String keeperRunid) throws IOException {
				throw new UnsupportedOperationException();
			}

			@Override
			public void setKeeperState(String keeperRunid, KeeperState keeperState) throws IOException {
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean isFresh() {
				return true;
			}
		};
	}

	public long getMasterOffset() {
		return masterOffset;
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public RdbStore beginRdb(String masterRunid, long masterOffset, long rdbFileSize) throws IOException {
		this.masterRunid = masterRunid;
		this.masterOffset = masterOffset;
		dumpedRdbStore.setMasterOffset(masterOffset);
		dumpedRdbStore.setRdbFileSize(rdbFileSize);
		return dumpedRdbStore;
	}

	@Override
	public long getEndOffset() {
		return -1L;
	}

	@Override
	public void delete() {
		throw new UnsupportedOperationException();
	}

	@Override
	public MetaStore getMetaStore() {
		return metaStore;
	}

	@Override
	public boolean gc() {
		return rdbStore.delete();
	}

	@Override
	public boolean fullSyncIfPossible(FullSyncListener fullSyncListener) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isFresh() {
		return true;
	}

	@Override
	public long getKeeperEndOffset() {
		throw new UnsupportedOperationException();
	}

	@Override
	public long nextNonOverlappingKeeperBeginOffset() {
		throw new UnsupportedOperationException();
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
	public void addCommandsListener(long offset, CommandsListener commandsListener) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public DumpedRdbStore prepareNewRdb() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void rdbUpdated(DumpedRdbStore dumpedRdbStore) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean checkOk() {
		return dumpedRdbStore.checkOk();
	}

}
