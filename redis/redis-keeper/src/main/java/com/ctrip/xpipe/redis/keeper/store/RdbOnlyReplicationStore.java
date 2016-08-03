/**
 * 
 */
package com.ctrip.xpipe.redis.keeper.store;

import java.io.File;
import java.io.IOException;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.store.CommandStore;
import com.ctrip.xpipe.redis.core.store.MetaStore;
import com.ctrip.xpipe.redis.core.store.RdbFileListener;
import com.ctrip.xpipe.redis.core.store.RdbStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreMeta;

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
				// TODO Auto-generated method stub

			}

			@Override
			public void saveMeta(String name, ReplicationStoreMeta replicationStoreMeta) throws IOException {
				// TODO Auto-generated method stub

			}

			@Override
			public void loadMeta() throws IOException {
				// TODO Auto-generated method stub

			}

			@Override
			public String getMasterRunid() {
				return masterRunid;
			}

			@Override
			public DefaultEndPoint getMasterAddress() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public long getKeeperBeginOffset() {
				// TODO Auto-generated method stub
				return 0;
			}

			@Override
			public ReplicationStoreMeta dupReplicationStoreMeta() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public long beginOffset() {
				// TODO Auto-generated method stub
				return 0;
			}

			@Override
			public void masterChanged(long keeperOffset, DefaultEndPoint newMasterEndpoint, String newMasterRunid, long newMasterReplOffset)
					throws IOException {
				// TODO Auto-generated method stub

			}

			@Override
			public void becomeBackup() throws IOException {
				// TODO Auto-generated method stub

			}

			@Override
			public ReplicationStoreMeta rdbUpdated(String rdbFile, long rdbFileSize, long masterOffset) throws IOException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public ReplicationStoreMeta rdbBegun(String masterRunid, long beginOffset, String rdbFile, long rdbFileSize, String cmdFilePrefix)
					throws IOException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public long redisOffsetToKeeperOffset(long redisOffset) {
				// TODO Auto-generated method stub
				return 0;
			}

			@Override
			public void becomeActive() throws IOException {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void psyncBegun(String keeperRunid, long offset) throws IOException {
				// TODO Auto-generated method stub
				
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
		// TODO Auto-generated method stub

	}

	@Override
	public RdbStore beginRdb(String masterRunid, long masterOffset, long rdbFileSize) throws IOException {
		this.masterRunid = masterRunid;
		this.masterOffset = masterOffset;
		rdbStore = new DefaultRdbStore(rdbFile, masterOffset, rdbFileSize);
		return rdbStore;
	}

	@Override
	public long endOffset() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void delete() {
		// TODO Auto-generated method stub

	}

	@Override
	public void rdbUpdated(String rdbFile, long masterOffset) {
		// TODO Auto-generated method stub

	}

	@Override
	public CommandStore getCommandStore() {
		return null;
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
	public File newRdbFile() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean fullSyncIfPossible(RdbFileListener defaultRdbFileListener) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isFresh() {
		// TODO Auto-generated method stub
		return true;
	}

}
