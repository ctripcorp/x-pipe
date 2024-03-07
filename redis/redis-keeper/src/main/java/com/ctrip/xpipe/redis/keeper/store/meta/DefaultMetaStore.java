package com.ctrip.xpipe.redis.keeper.store.meta;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.store.RdbStore;
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
	public ReplicationStoreMeta rdbConfirm(String replId, long beginOffset, String gtidSet, String rdbFile, RdbStore.Type type,
										 EofType eofType, String cmdFilePrefix) throws IOException {
		synchronized (metaRef) {
			ReplicationStoreMeta metaDup = dupReplicationStoreMeta();

			if (RdbStore.Type.NORMAL.equals(type)) {
				metaDup.setRdbFile(rdbFile);
				metaDup.setRdbGtidSet(null);
				setRdbFileInfo(metaDup, eofType);
				metaDup.setRdbLastOffset(beginOffset - 1);
				metaDup.setRdbGtidSet(gtidSet);
			} else if (RdbStore.Type.RORDB.equals(type)) {
				metaDup.setRordbFile(rdbFile);
				metaDup.setRordbGtidSet(null);
				setRordbFileInfo(metaDup, eofType);
				metaDup.setRordbLastOffset(beginOffset - 1);
				metaDup.setRordbGtidSet(gtidSet);
			} else {
				throw new IllegalStateException("unknown type " + (type == null?"null":type.name()));
			}

			metaDup.setReplId(replId);
			metaDup.setBeginOffset(beginOffset);
			metaDup.setCmdFilePrefix(cmdFilePrefix);
			clearReplicationId2(metaDup);

			saveMeta(metaDup);
			return metaDup;
		}

	}

	@Override
	public ReplicationStoreMeta rdbBegun(String replId, long beginOffset, String rdbFile, EofType eofType,
			String cmdFilePrefix) throws IOException {
		synchronized (metaRef) {
			ReplicationStoreMeta metaDup = dupReplicationStoreMeta();

			metaDup.setReplId(replId);
			metaDup.setBeginOffset(beginOffset);
			metaDup.setRdbFile(rdbFile);
			metaDup.setRdbGtidSet(null);
			setRdbFileInfo(metaDup, eofType);
			metaDup.setCmdFilePrefix(cmdFilePrefix);
			metaDup.setRdbLastOffset(beginOffset - 1);
			
			clearReplicationId2(metaDup);
			
			saveMeta(metaDup);
			return metaDup;
		}
	}

	@Override
	public boolean attachRdbGtidSet(String rdbFile, String gtidSet) throws IOException {
		synchronized (metaRef) {
			ReplicationStoreMeta metaDup = dupReplicationStoreMeta();
			if (rdbFile.equals(metaDup.getRdbFile())) {
				metaDup.setRdbGtidSet(gtidSet);
				saveMeta(metaDup);
				return true;
			}

			return false;
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
			String currentRordbFile = currentMeta.getRordbFile();

			if (currentRdbFile != null && currentRdbFile.equals(rdbFile)) {
				ReplicationStoreMeta metaDup = dupReplicationStoreMeta();
				clearRdb(metaDup);
				saveMeta(metaDup);
			} else if (currentRordbFile != null && currentRordbFile.equals(rdbFile)) {
				ReplicationStoreMeta metaDup = dupReplicationStoreMeta();
				clearRordb(metaDup);
				saveMeta(metaDup);
			} else {
				logger.info("[releaseRdbFile][{}] currentRdb:{} currentRordb:{}, skip", rdbFile, currentRdbFile, currentRordbFile);
			}
		}
	}

	private void clearRdb(ReplicationStoreMeta metaDup) {
		metaDup.setRdbFile(null);
		metaDup.setRdbEofMark(null);
		metaDup.setRdbFileSize(0);
		metaDup.setRdbLastOffset(null);
		metaDup.setRdbGtidSet(null);
	}

	private void clearRordb(ReplicationStoreMeta metaDup) {
		metaDup.setRordbFile(null);
		metaDup.setRordbEofMark(null);
		metaDup.setRordbFileSize(0);
		metaDup.setRordbLastOffset(null);
		metaDup.setRordbGtidSet(null);
	}

}
