/**
 * 
 */
package com.ctrip.xpipe.redis.keeper.store;

import java.io.File;
import java.io.IOException;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.store.CommandsListener;
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

	private File rdbFile;
	private DefaultRdbStore rdbStore;
	private String masterRunid;
	private long masterOffset;
	private MetaStore metaStore;

	public RdbOnlyReplicationStore(File rdbFile) {
		this.rdbFile = rdbFile;
		metaStore = new MetaStore() {

			@Override
			public void setMasterAddress(DefaultEndPoint endpoint) {
			}

			@Override
			public void saveMeta(String name, ReplicationStoreMeta replicationStoreMeta) throws IOException {
			}

			@Override
			public void loadMeta() throws IOException {
			}

			@Override
			public String getMasterRunid() {
				return masterRunid;
			}

			@Override
			public DefaultEndPoint getMasterAddress() {
				return null;
			}

			@Override
			public long getKeeperBeginOffset() {
				return 0;
			}

			@Override
			public ReplicationStoreMeta dupReplicationStoreMeta() {
				return null;
			}

			@Override
			public Long beginOffset() {
				return 0L;
			}

			@Override
			public void masterChanged(long keeperOffset, DefaultEndPoint newMasterEndpoint, String newMasterRunid, long newMasterReplOffset)
					throws IOException {

			}

			@Override
			public void becomeBackup() throws IOException {
			}

			@Override
			public ReplicationStoreMeta rdbUpdated(String rdbFile, long rdbFileSize, long masterOffset) throws IOException {
				return null;
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
			}

			@Override
			public void psyncBegun(String keeperRunid, long offset) throws IOException {
			}
		};
	}

	public File getRdbFile() {
		return rdbFile;
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
		rdbStore = new DefaultRdbStore(rdbFile, masterOffset, rdbFileSize);
		return rdbStore;
	}

	@Override
	public long getEndOffset() {
		return 0;
	}

	@Override
	public void delete() {
	}

	@Override
	public void rdbUpdated(String rdbFile, long masterOffset) {
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
	public File prepareNewRdbFile() {
		return null;
	}

	@Override
	public boolean fullSyncIfPossible(FullSyncListener fullSyncListener) throws IOException {
		return false;
	}

	@Override
	public boolean isFresh() {
		return true;
	}

	@Override
	public long getKeeperEndOffset() {
		return 0;
	}

	@Override
	public long nextNonOverlappingKeeperBeginOffset() {
		return 0;
	}

	@Override
	public int appendCommands(ByteBuf byteBuf) throws IOException {
		return 0;
	}

	@Override
	public boolean awaitCommandsOffset(long offset, int timeMilli) throws InterruptedException {
		return false;
	}

	@Override
	public void addCommandsListener(long offset, CommandsListener commandsListener) throws IOException {
		throw new UnsupportedOperationException();
	}

}
