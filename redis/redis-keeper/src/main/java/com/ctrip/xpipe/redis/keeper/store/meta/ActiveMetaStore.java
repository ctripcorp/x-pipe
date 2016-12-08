package com.ctrip.xpipe.redis.keeper.store.meta;

import java.io.File;
import java.io.IOException;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreMeta;

/**
 * @author wenchao.meng
 *
 * Dec 4, 2016
 */
public class ActiveMetaStore extends AbstractMetaStore{

	public ActiveMetaStore(File baseDir, String keeperRunid) {
		super(baseDir, keeperRunid);
	}

	@Override
	public String getMasterRunid() {
		return getMeta().getMasterRunid();
	}

	@Override
	public Long beginOffset() {
		return getMeta().getBeginOffset();
	}

	@Override
	protected void doSetMasterAddress(ReplicationStoreMeta metaDup, DefaultEndPoint endpoint) {
		metaDup.setMasterAddress(endpoint);
	}

	
	@Override
	public DefaultEndPoint getMasterAddress() {
		
		return getMeta().getMasterAddress();
	}

	@Override
	public void psyncBegun(String masterRunid, long keeperBeginOffset) throws IOException {
		synchronized (metaRef) {
			ReplicationStoreMeta metaDup = dupReplicationStoreMeta();

			metaDup.setMasterRunid(masterRunid);
			metaDup.setKeeperBeginOffset(keeperBeginOffset);

			saveMeta(metaDup);
		}		
	}

	@Override
	public ReplicationStoreMeta rdbBegun(String masterRunid, long beginOffset, String rdbFile, long rdbFileSize,
			String cmdFilePrefix) throws IOException {
		synchronized (metaRef) {
			ReplicationStoreMeta metaDup = dupReplicationStoreMeta();

			metaDup.setMasterRunid(masterRunid);
			metaDup.setBeginOffset(beginOffset);
			metaDup.setRdbFile(rdbFile);
			metaDup.setRdbFileSize(rdbFileSize);
			metaDup.setCmdFilePrefix(cmdFilePrefix);
			metaDup.setRdbLastKeeperOffset(metaDup.getKeeperBeginOffset() - 1);

			saveMeta(metaDup);
			return metaDup;
		}
	}

	@Override
	public void masterChanged(long keeperOffset, DefaultEndPoint newMasterEndpoint, String newMasterRunid,
			long newMasterReplOffset) throws IOException {
		
		synchronized (metaRef) {
			ReplicationStoreMeta metaDup = dupReplicationStoreMeta();
			long newBeginOffset = metaDup.getKeeperBeginOffset() + newMasterReplOffset - keeperOffset;

			metaDup.setMasterAddress(newMasterEndpoint);
			metaDup.setMasterRunid(newMasterRunid);
			metaDup.setBeginOffset(newBeginOffset);

			saveMeta(metaDup);

			logger.info("[masterChanged]newMasterEndpoint: {},  newMasterRunid: {}, keeperOffset: {}, newMasterReplOffset: {}, newBeginOffset: {}", //
					newMasterEndpoint, newMasterRunid, keeperOffset, newMasterReplOffset, newBeginOffset);
		}
	}

	@Override
	public void saveKinfo(ReplicationStoreMeta replicationStoreMeta) throws IOException {
		throw new UnsupportedOperationException();
	}

}
