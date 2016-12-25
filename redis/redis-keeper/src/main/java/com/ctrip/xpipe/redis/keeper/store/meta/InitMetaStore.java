package com.ctrip.xpipe.redis.keeper.store.meta;

import java.io.File;
import java.io.IOException;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.store.MetaStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreMeta;

/**
 * @author wenchao.meng
 *
 * Dec 4, 2016
 */
@SuppressWarnings("unused")
public class InitMetaStore implements MetaStore{
	
	private File baseDir;
	
	private String keeperRunid;	

	public InitMetaStore(File baseDir, String keeperRunid) {
		this.baseDir = baseDir;
		this.keeperRunid = keeperRunid;
	}

	@Override
	public String getMasterRunid() {
		return null;
	}

	@Override
	public Long beginOffset() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setMasterAddress(DefaultEndPoint endpoint) throws IOException {
		throw new UnsupportedOperationException();
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
		return null;
	}

	@Override
	public void loadMeta() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void psyncBegun(String masterRunid, long offset) throws IOException {
		throw new UnsupportedOperationException();
		
	}

	@Override
	public void becomeActive() throws IOException {
		throw new UnsupportedOperationException();
		
	}

	@Override
	public void becomeBackup() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public ReplicationStoreMeta rdbBegun(String masterRunid, long beginOffset, String rdbFile, EofType eofType,
			String cmdFilePrefix) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void masterChanged(long keeperOffset, DefaultEndPoint newMasterEndpoint, String newMasterRunid,
			long newMasterReplOffset) throws IOException {
		throw new UnsupportedOperationException();
		
	}

	@Override
	public ReplicationStoreMeta rdbUpdated(String rdbFile, EofType eofType, long masterOffset) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public long redisOffsetToKeeperOffset(long redisOffset) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateKeeperRunid(String keeperRunid) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void saveKinfo(ReplicationStoreMeta replicationStoreMeta) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setKeeperState(String keeperRunid, KeeperState keeperState) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setRdbFileSize(long rdbFileSize) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isFresh() {
		return true;
	}

}
