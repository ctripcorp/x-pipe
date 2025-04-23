package com.ctrip.xpipe.redis.keeper.store.meta;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.store.RdbStore;
import com.ctrip.xpipe.redis.core.store.ReplStage;
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
	public ReplStage getPreReplStage() {
		return getMeta().getPrevReplStage();
	}

	@Override
	public ReplStage getCurrentReplStage() {
		return getMeta().getCurReplStage();
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
	public String getCurReplStageReplId() {
		ReplStage curReplStage = getMeta().getCurReplStage();
		if (curReplStage != null) {
			return curReplStage.getReplId();
		} else {
			return null;
		}
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

	private void clearGapAllowObseleteFields(ReplicationStoreMeta meta) {
		meta.setBeginOffset(null);
		meta.setReplId(null);
		meta.setReplId2(null);
		meta.setSecondReplIdOffset(null);
		meta.setRdbLastOffset(null);
		meta.setRordbLastOffset(null);
	}

	@Override
	public ReplicationStoreMeta rdbConfirmPsync(String replId, long beginReplOffset, long backlogOff, String rdbFile,
												RdbStore.Type type, EofType eofType, String cmdFilePrefix) throws IOException {
		synchronized (metaRef) {
			ReplicationStoreMeta metaDup = dupReplicationStoreMeta();

			metaDup.setPrevReplStage(null);
			metaDup.setCurReplStage(new ReplStage(replId, beginReplOffset, backlogOff));

			if (RdbStore.Type.NORMAL.equals(type)) {
				metaDup.setRdbFile(rdbFile);
				setRdbFileInfo(metaDup, eofType);
				metaDup.setRdbGtidSet(null);
				metaDup.setRdbBacklogOffset(backlogOff);
			} else if (RdbStore.Type.RORDB.equals(type)) {
				metaDup.setRordbFile(rdbFile);
				setRordbFileInfo(metaDup, eofType);
				metaDup.setRordbGtidSet(null);
				metaDup.setRordbBacklogOffset(backlogOff);
			} else {
				throw new IllegalStateException("unknown type " + (type == null?"null":type.name()));
			}

			metaDup.setCmdFilePrefix(cmdFilePrefix);

			clearGapAllowObseleteFields(metaDup);

			saveMeta(metaDup);
			return metaDup;
		}
	}

	@Override
	public ReplicationStoreMeta psyncContinue(String newReplId, long backlogOff) throws IOException {
		synchronized (metaRef) {
			ReplicationStoreMeta metaDup = dupReplicationStoreMeta();

			ReplStage curReplStage = metaDup.getCurReplStage();
			if (curReplStage.getProto() != ReplStage.ReplProto.PSYNC) {
				throw new IllegalStateException("continue in xsync replstage");
			}

			String currentReplId = curReplStage.getReplId();
			// backlogOff - curReplStage.beginOffsetBacklog == secondReplidOffset - replStage.beginOffsetRepl
			long secondReplidOffset = curReplStage.getBegOffsetRepl() + backlogOff - curReplStage.getBegOffsetBacklog();

			curReplStage.setReplId2(currentReplId);
			curReplStage.setSecondReplIdOffset(secondReplidOffset);
			curReplStage.updateReplId(newReplId);

			saveMeta(metaDup);
			return metaDup;
		}
	}

	@Override
	public ReplicationStoreMeta switchToPsync(String replId, long beginReplOffset, long backlogOff) throws IOException {
		synchronized (metaRef) {
			ReplicationStoreMeta metaDup = dupReplicationStoreMeta();

			ReplStage curReplStage = metaDup.getCurReplStage();
			if (curReplStage.getProto() != ReplStage.ReplProto.XSYNC) {
				throw new IllegalStateException("switchtopsync in psync replstage");
			}

			// TODO should we update xsync replid/off when switch to psync?
			// curReplStage.updateReplId(replId);
			// curReplStage.adjustBegOffsetRepl(beginReplOffset, backlogOff);

			ReplStage newReplStage = new ReplStage(replId, beginReplOffset, backlogOff);

			metaDup.setPrevReplStage(curReplStage);
			metaDup.setCurReplStage(newReplStage);

			saveMeta(metaDup);
			return metaDup;
		}
	}

	@Override
	public ReplicationStoreMeta rdbConfirmXsync(String replId, long beginReplOffset, long backlogOff, String masterUuid,
												GtidSet gtidLost, GtidSet gtidExecuted, String rdbFile,
												RdbStore.Type type, EofType eofType, String cmdFilePrefix) throws IOException {
		synchronized (metaRef) {
			ReplicationStoreMeta metaDup = dupReplicationStoreMeta();

			metaDup.setPrevReplStage(null);
			metaDup.setCurReplStage(new ReplStage(replId, beginReplOffset, backlogOff, masterUuid, gtidLost, gtidExecuted));

			if (RdbStore.Type.NORMAL.equals(type)) {
				metaDup.setRdbFile(rdbFile);
				setRdbFileInfo(metaDup, eofType);
				metaDup.setRdbGtidSet(gtidExecuted.toString());
				metaDup.setRdbBacklogOffset(backlogOff);
			} else if (RdbStore.Type.RORDB.equals(type)) {
				metaDup.setRordbFile(rdbFile);
				setRordbFileInfo(metaDup, eofType);
				metaDup.setRordbGtidSet(gtidExecuted.toString());
				metaDup.setRordbBacklogOffset(backlogOff);
			} else {
				throw new IllegalStateException("unknown type " + (type == null?"null":type.name()));
			}

			metaDup.setCmdFilePrefix(cmdFilePrefix);

			clearGapAllowObseleteFields(metaDup);

			saveMeta(metaDup);
			return metaDup;
		}
	}

	@Override
	public boolean xsyncContinue(String replId, long beginReplOffset, long backlogOff, String masterUuid,
											  GtidSet gtidCont, GtidSet gtidIndexed) throws IOException {
		boolean	updated = false;
		synchronized (metaRef) {
			ReplicationStoreMeta metaDup = dupReplicationStoreMeta();

			ReplStage curReplStage = metaDup.getCurReplStage();
			if (curReplStage.getProto() != ReplStage.ReplProto.XSYNC) {
				throw new IllegalStateException("xcontinue in psync replstage");
			}

			if (curReplStage.updateReplId(replId)) {
				updated = true;
			}

			if (curReplStage.adjustBegOffsetRepl(beginReplOffset, backlogOff)) {
				updated = true;
			}

			if (curReplStage.updateMasterUuid(masterUuid)) {
				updated = true;
			}

			GtidSet gtidSet = gtidIndexed.union(curReplStage.getBeginGtidset());
			GtidSet gtidLost = gtidCont.subtract(gtidSet);

			if (!gtidLost.equals(curReplStage.getGtidLost())) {
				curReplStage.setGtidLost(gtidLost);
				updated = true;
			}

			saveMeta(metaDup);
		}
		return updated;
	}

	@Override
	public ReplicationStoreMeta switchToXsync(String replId, long beginReplOffset, long backlogOff, String masterUuid,
											  GtidSet gtidCont) throws IOException {
		synchronized (metaRef) {
			ReplicationStoreMeta metaDup = dupReplicationStoreMeta();

			ReplStage curReplStage = metaDup.getCurReplStage();
			if (curReplStage.getProto() != ReplStage.ReplProto.PSYNC) {
				throw new IllegalStateException("switchtoxsync in xsync replstage");
			}

			ReplStage newReplStage = new ReplStage(replId, beginReplOffset, backlogOff, masterUuid,
					new GtidSet(GtidSet.EMPTY_GTIDSET), gtidCont);

			metaDup.setPrevReplStage(curReplStage);
			metaDup.setCurReplStage(newReplStage);

			saveMeta(metaDup);
			return metaDup;
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
