package com.ctrip.xpipe.redis.keeper.store.meta;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreMeta;
import com.ctrip.xpipe.utils.ObjectUtils;

import java.io.File;
import java.io.IOException;

/**
 * @author wenchao.meng
 *
 * Dec 4, 2016
 */
public class DefaultMetaStore extends AbstractMetaStore{

	public DefaultMetaStore(File baseDir, String keeperRunid) {
		super(baseDir, keeperRunid);
	}

	@Override
	public String getReplId() {
		return getMeta().getReplId();
	}
	
	@Override
	public String getReplId2() {
		return getMeta().getReplId2();
	}
	
	@Override
	public Long getSecondReplIdOffset() {
		return getMeta().getSecondReplIdOffset();
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
	public ReplicationStoreMeta rdbBegun(String replId, long beginOffset, String rdbFile, EofType eofType,
			String cmdFilePrefix) throws IOException {
		synchronized (metaRef) {
			ReplicationStoreMeta metaDup = dupReplicationStoreMeta();

			metaDup.setReplId(replId);
			metaDup.setBeginOffset(beginOffset);
			metaDup.setRdbFile(rdbFile);
			setRdbFileInfo(metaDup, eofType);
			metaDup.setCmdFilePrefix(cmdFilePrefix);
			metaDup.setRdbLastOffset(beginOffset - 1);
			
			clearReplicationId2(metaDup);
			
			saveMeta(metaDup);
			return metaDup;
		}
	}

	@Override
	public ReplicationStoreMeta continueFromOffset(String replId, long beginOffset, String cmdFilePrefix) throws IOException {
		synchronized (metaRef) {
			ReplicationStoreMeta metaDup = dupReplicationStoreMeta();

			metaDup.setReplId(replId);
			metaDup.setBeginOffset(beginOffset);
			metaDup.setCmdFilePrefix(cmdFilePrefix);
			clearRdb(metaDup);

			saveMeta(metaDup);
			return metaDup;
		}
	}

	@Override
	public void masterChanged(long keeperOffset, DefaultEndPoint newMasterEndpoint, String newMasterRunid,
			long newMasterReplOffset) throws IOException {
		throw new UnsupportedOperationException();
	}
	
	protected void clearReplicationId2(ReplicationStoreMeta meta){
		
		logger.info("[clearReplicationId2]");
		meta.setReplId2(ReplicationStoreMeta.EMPTY_REPL_ID);
		meta.setSecondReplIdOffset(ReplicationStoreMeta.DEFAULT_SECOND_REPLID_OFFSET);
	}
	
	@Override
	public ReplicationStoreMeta shiftReplicationId(String newReplId, Long currentOffset) throws IOException {
		
		synchronized (metaRef) {
			ReplicationStoreMeta metaDup = dupReplicationStoreMeta();

			String currentReplId = metaDup.getReplId();
			if(ObjectUtils.equals(currentReplId, newReplId)){
				logger.info("[shiftReplicationId][repidEqual]{}", newReplId);
				return metaDup;
			}
			
			metaDup.setReplId(newReplId);
			metaDup.setReplId2(currentReplId);
			metaDup.setSecondReplIdOffset(currentOffset + 1);
			
			saveMeta(metaDup);
			return metaDup;
		}
	}

	@Override
	public void releaseRdbFile(String rdbFile) throws IOException {
		synchronized (metaRef) {
			ReplicationStoreMeta currentMeta = metaRef.get();
			String currentRdbFile = currentMeta.getRdbFile();

			if (null == currentRdbFile) {
				logger.info("[releaseRdbFile][{}][already no rdb]", rdbFile);
			} else if (!currentRdbFile.equals(rdbFile)) {
				logger.warn("[releaseRdbFile][{}] current {}, skip", rdbFile, currentRdbFile);
			} else {
				ReplicationStoreMeta metaDup = dupReplicationStoreMeta();
				clearRdb(metaDup);
				saveMeta(metaDup);
			}
		}
	}

	private void clearRdb(ReplicationStoreMeta metaDup) {
		metaDup.setRdbFile(null);
		metaDup.setRdbEofMark(null);
		metaDup.setRdbFileSize(0);
		metaDup.setRdbLastOffset(null);
	}

}
