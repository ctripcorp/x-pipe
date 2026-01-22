package com.ctrip.xpipe.redis.keeper.store.meta;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.store.*;
import com.ctrip.xpipe.utils.ObjectUtils;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

/**
 * @author wenchao.meng
 *
 * Dec 4, 2016
 */
public class DefaultMetaStore extends AbstractMetaStore implements GtidCmdFilter {
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
	public Long backlogOffsetToReplOffset(Long backlogOffset) {
		Long replOffset;

		ReplStage curReplStage = getMeta().getCurReplStage();
		ReplStage prevReplStage = getMeta().getPrevReplStage();

		if (backlogOffset != null && curReplStage != null && curReplStage.getBegOffsetBacklog() <= backlogOffset) {
			replOffset = backlogOffset + curReplStage.getBegOffsetRepl() - curReplStage.getBegOffsetBacklog();
			logger.debug("[replOffsetToBacklogOffset] [cur] replOffset:{} backlogOffset:{}, begOffsetRepl:{}, begOffsetBacklog:{}",
					replOffset, backlogOffset, curReplStage.getBegOffsetRepl(), curReplStage.getBegOffsetBacklog());
		} else if (backlogOffset != null && prevReplStage != null && prevReplStage.getBegOffsetBacklog() <= backlogOffset) {
			replOffset = backlogOffset + prevReplStage.getBegOffsetRepl() - prevReplStage.getBegOffsetBacklog();
			logger.debug("[replOffsetToBacklogOffset] [prev] replOffset:{} backlogOffset:{}, begOffsetRepl:{}, begOffsetBacklog:{}",
					replOffset, backlogOffset, prevReplStage.getBegOffsetRepl(), prevReplStage.getBegOffsetBacklog());
		} else {
			replOffset = null;
			logger.debug("[replOffsetToBacklogOffset] [none] replOffset:{} backlogOffset:{}", replOffset, backlogOffset);
		}

		return replOffset;
	}


	@Override
	public Long replOffsetToBacklogOffset(Long replOffset) {
		Long backlogOffset;

		ReplStage curReplStage = getMeta().getCurReplStage();
		ReplStage prevReplStage = getMeta().getPrevReplStage();

		if (replOffset != null && curReplStage != null && curReplStage.getBegOffsetRepl() <= replOffset+1) {
			backlogOffset = replOffset+1 - curReplStage.getBegOffsetRepl() + curReplStage.getBegOffsetBacklog();
			logger.debug("[replOffsetToBacklogOffset] [cur] backlogOffset:{} replOffset:{}, begOffsetRepl:{}, begOffsetBacklog:{}",
					backlogOffset, replOffset, curReplStage.getBegOffsetRepl(), curReplStage.getBegOffsetBacklog());
		} else if (replOffset != null && prevReplStage != null && prevReplStage.getBegOffsetRepl() <= replOffset+1) {
			backlogOffset = replOffset+1 - prevReplStage.getBegOffsetRepl() + prevReplStage.getBegOffsetBacklog();
			logger.debug("[replOffsetToBacklogOffset] [prev] backlogOffset:{} replOffset:{}, begOffsetRepl:{}, begOffsetBacklog:{}",
					backlogOffset, replOffset, prevReplStage.getBegOffsetRepl(), prevReplStage.getBegOffsetBacklog());
		} else {
			backlogOffset = null;
			logger.debug("[replOffsetToBacklogOffset] [none] backlogOffset:{} replOffset:{}", backlogOffset, replOffset);
		}


		return backlogOffset;
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
				metaDup.setRdbContiguousBacklogOffset(backlogOff);
				metaDup.setRdbReplProto(ReplStage.ReplProto.PSYNC);
			} else if (RdbStore.Type.RORDB.equals(type)) {
				metaDup.setRordbFile(rdbFile);
				setRordbFileInfo(metaDup, eofType);
				metaDup.setRordbGtidSet(null);
				metaDup.setRordbContiguousBacklogOffset(backlogOff);
				metaDup.setRordbReplProto(ReplStage.ReplProto.PSYNC);
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
	public ReplicationStoreMeta psyncContinueFrom(String replId, long beginReplOffset, long backlogOff, String cmdFilePrefix) throws IOException {
		synchronized (metaRef) {
			ReplicationStoreMeta metaDup = dupReplicationStoreMeta();

			metaDup.setPrevReplStage(null);
			metaDup.setCurReplStage(new ReplStage(replId, beginReplOffset, backlogOff));

			metaDup.setCmdFilePrefix(cmdFilePrefix);

			clearGapAllowObseleteFields(metaDup);

			clearRdb(metaDup);
			clearRordb(metaDup);

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

			if(ObjectUtils.equals(currentReplId, newReplId)){
				logger.info("[shiftReplicationId][repidEqual]{}", newReplId);
				return metaDup;
			}

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

			ReplStage newReplStage = new ReplStage(replId, beginReplOffset, backlogOff);

			logger.info("[switchToPsync] {} -> {}", curReplStage, newReplStage);
			metaDup.setPrevReplStage(curReplStage);
			metaDup.setCurReplStage(newReplStage);

			saveMeta(metaDup);
			return metaDup;
		}
	}

	@Override
	public ReplicationStoreMeta rdbConfirmXsync(String replId, long beginReplOffset, long beginOffsetBacklog, String masterUuid,
												GtidSet gtidLost, GtidSet gtidExecuted, String rdbFile,
												RdbStore.Type type, EofType eofType, String cmdFilePrefix) throws IOException {
		synchronized (metaRef) {
			ReplicationStoreMeta metaDup = dupReplicationStoreMeta();

			metaDup.setPrevReplStage(null);
			metaDup.setCurReplStage(new ReplStage(replId, beginReplOffset, beginOffsetBacklog, masterUuid, gtidLost, gtidExecuted));

			if (RdbStore.Type.NORMAL.equals(type)) {
				metaDup.setRdbFile(rdbFile);
				setRdbFileInfo(metaDup, eofType);
				metaDup.setRdbGtidSet(gtidExecuted.toString());
				metaDup.setRdbContiguousBacklogOffset(beginOffsetBacklog);
				metaDup.setRdbReplProto(ReplStage.ReplProto.XSYNC);
			} else if (RdbStore.Type.RORDB.equals(type)) {
				metaDup.setRordbFile(rdbFile);
				setRordbFileInfo(metaDup, eofType);
				metaDup.setRordbGtidSet(gtidExecuted.toString());
				metaDup.setRordbContiguousBacklogOffset(beginOffsetBacklog);
				metaDup.setRordbReplProto(ReplStage.ReplProto.XSYNC);
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
	public boolean increaseLost(GtidSet lost) throws IOException {

		synchronized (metaRef) {
			ReplicationStoreMeta metaDup = dupReplicationStoreMeta();

			ReplStage curReplStage = metaDup.getCurReplStage();
			if (curReplStage.getProto() != ReplStage.ReplProto.XSYNC) {
				throw new IllegalStateException("xcontinue in psync replstage");
			}

			GtidSet oldLost = curReplStage.getGtidLost();
			GtidSet newLost = oldLost.union(lost);
			if (oldLost.equals(newLost)) {
				return false;
			}

			curReplStage.setGtidLost(newLost);
			saveMeta(metaDup);
			return true;
		}
	}

	@Override
	public int removeLost(GtidSet gtidSet) throws IOException {
		synchronized (metaRef) {
			ReplicationStoreMeta metaDup = dupReplicationStoreMeta();

			ReplStage curReplStage = metaDup.getCurReplStage();
			if (curReplStage.getProto() != ReplStage.ReplProto.XSYNC) {
				throw new IllegalStateException("xcontinue in psync replstage");
			}

			GtidSet oldLost = curReplStage.getGtidLost();
			GtidSet newLost = oldLost.subtract(gtidSet);
			int diffCnt = oldLost.subtract(newLost).itemCnt();
			if (diffCnt == 0) {
				return diffCnt;
			}
			curReplStage.setGtidLost(newLost);
			saveMeta(metaDup);
			return diffCnt;
		}
	}

	@Override
	public int increaseExecuted(GtidSet gtidSet) throws IOException {
		synchronized (metaRef) {
			ReplicationStoreMeta metaDup = dupReplicationStoreMeta();

			ReplStage curReplStage = metaDup.getCurReplStage();
			if (curReplStage.getProto() != ReplStage.ReplProto.XSYNC) {
				throw new IllegalStateException("xcontinue in psync replstage");
			}

			GtidSet oldFixed = curReplStage.getFixedGtidset();
			GtidSet newFixed = oldFixed.union(gtidSet);
			int diffCnt = newFixed.subtract(oldFixed).itemCnt();
			if (diffCnt == 0) {
				return diffCnt;
			}
			curReplStage.setGtidLost(newFixed);
			saveMeta(metaDup);
			return diffCnt;
		}
	}

	@Override
	public boolean gtidSetContains(String uuid, long gno) {
		synchronized (metaRef) {
			return metaRef.get().getCurReplStage().getGtidLost().contains(uuid, gno);
		}
	}

	@Override
	public boolean xsyncContinue(String replId, long beginReplOffset, long beginOffsetBacklog, String masterUuid,
								 GtidSet gtidCont, GtidSet gtidIndexed) throws IOException {
		boolean	updated = false;
		synchronized (metaRef) {
			ReplicationStoreMeta metaDup = dupReplicationStoreMeta();

			ReplStage curReplStage = metaDup.getCurReplStage();
			if (curReplStage.getProto() != ReplStage.ReplProto.XSYNC) {
				throw new IllegalStateException("xcontinue in psync replstage");
			}

			if (curReplStage.updateReplId(replId)) {
				logger.info("[xsyncUpdate][replid] replId:{}", replId);
				updated = true;
			}

			if (curReplStage.adjustBegOffsetRepl(beginReplOffset, beginOffsetBacklog)) {
				logger.info("[xsyncUpdate][begOffsetRepl] beginOffsetRepl:{}, beginOffsetBacklog:{}", beginReplOffset, beginOffsetBacklog);
				updated = true;
			}

			if (curReplStage.updateMasterUuid(masterUuid)) {
				logger.info("[xsyncUpdate][masterUuid] masterUuid:{}", masterUuid);
				updated = true;
			}

			GtidSet gtidSet = gtidIndexed.union(curReplStage.getBeginGtidset());
			GtidSet deltaLost = gtidCont.subtract(gtidSet);

			if (!deltaLost.isEmpty()) {
				GtidSet gtidLost = curReplStage.getGtidLost().union(deltaLost);
				logger.info("[xsyncUpdate][gtidLost] gtidLost:{} + deltaLost:{} => gtidLost:{}", curReplStage.getGtidLost(), deltaLost, gtidLost);
				curReplStage.setGtidLost(gtidLost);
				updated = true;
			}

			saveMeta(metaDup);
		}
		return updated;
	}

	@Override
	public ReplicationStoreMeta xsyncContinueFrom(String replId, long beginReplOffset, long beginOffsetBacklog, String masterUuid,
											  GtidSet gtidLost, GtidSet gtidExecuted, String cmdFilePrefix) throws IOException {
		synchronized (metaRef) {
			ReplicationStoreMeta metaDup = dupReplicationStoreMeta();

			metaDup.setPrevReplStage(null);
			metaDup.setCurReplStage(new ReplStage(replId, beginReplOffset, beginOffsetBacklog, masterUuid, gtidLost, gtidExecuted));

			metaDup.setCmdFilePrefix(cmdFilePrefix);

			clearGapAllowObseleteFields(metaDup);

			clearRdb(metaDup);
			clearRordb(metaDup);

			saveMeta(metaDup);

			return metaDup;
		}
	}

	@Override
	public ReplicationStoreMeta switchToXsync(String replId, long beginReplOffset, long backlogOff, String masterUuid,
											  GtidSet gtidCont, GtidSet gtidLost) throws IOException {
		synchronized (metaRef) {
			ReplicationStoreMeta metaDup = dupReplicationStoreMeta();

			ReplStage curReplStage = metaDup.getCurReplStage();
			if (curReplStage.getProto() != ReplStage.ReplProto.PSYNC) {
				throw new IllegalStateException("switchtoxsync in xsync replstage");
			}
			GtidSet gtidExecuted = gtidCont.subtract(gtidLost);
			ReplStage newReplStage = new ReplStage(replId, beginReplOffset, backlogOff, masterUuid, gtidLost, gtidExecuted);

			logger.info("[switchToXsync] {} -> {}", curReplStage, newReplStage);
			metaDup.setPrevReplStage(curReplStage);
			metaDup.setCurReplStage(newReplStage);

			saveMeta(metaDup);
			return metaDup;
		}
	}

	private UPDATE_RDB_RESULT checkBeforeUpdateRdbInfo(
			long rdbOffset, long rdbContiguousBacklogOffset,
			long backlogBeginOffset, long backlogEndOffset, ReplStage.ReplProto expectedProto,
			String expectedReplId, String expectedMasterUuid) {
		ReplStage curReplStage = getCurrentReplStage();

		if (!Objects.equals(expectedProto, curReplStage.getProto())) {
			logger.info("[checkBeforeUpdateRdbInfo]update rdb fail, proto:{}, rdbProto:{}", curReplStage.getProto(), expectedProto);
			return UPDATE_RDB_RESULT.REPLSTAGE_NOT_MATCH;
		}

		if (!Objects.equals(expectedReplId, curReplStage.getReplId())) {
			logger.info("[checkBeforeUpdateRdbInfo]update rdb fail, replid:{}, rdbReplId:{}", curReplStage.getReplId(), expectedReplId);
			return UPDATE_RDB_RESULT.REPLID_NOT_MATCH;
		}

		if (expectedProto == ReplStage.ReplProto.XSYNC && !Objects.equals(expectedMasterUuid, curReplStage.getMasterUuid())) {
			logger.info("[checkBeforeUpdateRdbInfo]update rdb fail, masterUuid:{}, rdbMasterUuid:{}", curReplStage.getMasterUuid(), expectedMasterUuid);
			return UPDATE_RDB_RESULT.MASTER_UUID_NOT_MATCH;
		}

		if (rdbOffset + 1 < curReplStage.getBegOffsetRepl()) {
			logger.info("[checkBeforeUpdateRdbInfo]update rdb fail: rdb offset not in range. beginOffsetRepl:{}, rdbOffset:{}", curReplStage.getBegOffsetRepl(), rdbOffset);
			return UPDATE_RDB_RESULT.REPLOFF_OUT_RANGE;
		}

		if (rdbContiguousBacklogOffset < backlogBeginOffset) {
			logger.info("[checkBeforeUpdateRdbInfo]update rdb fail: lack backlog. backlogBeginOffset:{}, rdbContiguousBacklogOffset:{}", backlogBeginOffset, rdbContiguousBacklogOffset);
			return UPDATE_RDB_RESULT.LACK_BACKLOG;
		}

		if (rdbContiguousBacklogOffset > backlogEndOffset) {
			logger.info("[checkBeforeUpdateRdbInfo]update rdb fail: rdb more recent. backlogEndOffset:{}, rdbContiguousBacklogOffset:{}", backlogEndOffset, rdbContiguousBacklogOffset);
			return UPDATE_RDB_RESULT.RDB_MORE_RECENT;
		}

		return UPDATE_RDB_RESULT.OK;
	}

	private void updateRdbInfo(ReplicationStoreMeta metaDup,
							   String rdbFile, RdbStore.Type type, EofType eofType,
							   long rdbContBacklogOffset, String rdbGtidExecuted, ReplStage.ReplProto replProto) {
		if (RdbStore.Type.NORMAL.equals(type)) {
			logger.info("[rdbUpdated] update rdbContBacklogOffset to {}", rdbContBacklogOffset);
			metaDup.setRdbFile(rdbFile);
			setRdbFileInfo(metaDup, eofType);
			metaDup.setRdbContiguousBacklogOffset(rdbContBacklogOffset);
			metaDup.setRdbReplProto(replProto);
			metaDup.setRdbGtidSet(rdbGtidExecuted);
		} else if (RdbStore.Type.RORDB.equals(type)) {
			logger.info("[rordbUpdated] update rordbBacklogOffset to {}", rdbContBacklogOffset);
			metaDup.setRordbFile(rdbFile);
			setRordbFileInfo(metaDup, eofType);
			metaDup.setRordbContiguousBacklogOffset(rdbContBacklogOffset);
			metaDup.setRordbReplProto(replProto);
			metaDup.setRordbGtidSet(rdbGtidExecuted);
		} else {
			throw new IllegalStateException("unknown type " + (type == null?"null":type.name()));
		}
	}

	@Override
	public UPDATE_RDB_RESULT checkReplIdAndUpdateRdbInfoPsync(
			String rdbFile, RdbStore.Type type, EofType eofType, long rdbOffset, String rdbReplId,
			long backlogBeginOffset, long backlogEndOffset) throws IOException {
		synchronized (metaRef) {

			ReplicationStoreMeta metaDup = dupReplicationStoreMeta();

			ReplStage curReplStage = getCurrentReplStage();
			long rdbContBacklogOffset =  rdbOffset + 1 - curReplStage.getBegOffsetRepl() + curReplStage.getBegOffsetBacklog();

			UPDATE_RDB_RESULT result = checkBeforeUpdateRdbInfo(rdbOffset, rdbContBacklogOffset,
					backlogBeginOffset, backlogEndOffset, ReplStage.ReplProto.PSYNC, rdbReplId, null);

			if (result != UPDATE_RDB_RESULT.OK) return result;

			updateRdbInfo(metaDup, rdbFile, type, eofType, rdbContBacklogOffset, null, ReplStage.ReplProto.PSYNC);
			saveMeta(metaDup);

			return result;
		}
	}

	@Override
	public UPDATE_RDB_RESULT checkReplIdAndUpdateRdbInfoXsync(
			String rdbFile, RdbStore.Type type, EofType eofType, long rdbOffset, String rdbReplId, String rdbMasterUuid,
			GtidSet rdbGtidExecuted, GtidSet rdbGtidLost,
			long backlogBeginOffset, long backlogEndOffset, long indexedOffsetBacklog, GtidSet indexedGtidSet) throws IOException {
		synchronized (metaRef) {

			ReplicationStoreMeta metaDup = dupReplicationStoreMeta();

			UPDATE_RDB_RESULT result =  checkBeforeUpdateRdbInfo(rdbOffset, indexedOffsetBacklog,
					backlogBeginOffset, backlogEndOffset, ReplStage.ReplProto.XSYNC, rdbReplId, rdbMasterUuid);

			if (result != UPDATE_RDB_RESULT.OK) return result;

			GtidSet beginGtidSet = getCurrentReplStage().getBeginGtidset();
			GtidSet gtidLost = getCurrentReplStage().getGtidLost();
			GtidSet gtidCont = beginGtidSet.union(indexedGtidSet).union(gtidLost);
			logger.info("[checkReplIdAndUpdateRdbInfoXsync] gtidCont:{} = beginGtidSet:{} + indexedGtidSet:{} + gtidLost:{}", gtidCont, beginGtidSet, indexedGtidSet, gtidLost);

			GtidSet rdbGtidSet = rdbGtidExecuted.union(rdbGtidLost);
			logger.info("[checkReplIdAndUpdateRdbInfoXsync] rdbGtidSet:{} = rdbGtidExecuted:{} + rdbGtidLost:{}", rdbGtidSet, rdbGtidExecuted, rdbGtidLost);

			if (gtidCont.subtract(rdbGtidSet).itemCnt() > 0) {
				logger.warn("[checkReplIdAndUpdateRdbInfoXsync][gtid.set not match] gtidCont:{} > rdbGtidSet:{}", gtidCont, rdbGtidSet);
				return UPDATE_RDB_RESULT.GTID_SET_NOT_MATCH;
			}
			if (rdbGtidSet.subtract(gtidCont).itemCnt() > 0) {
				logger.warn("[checkReplIdAndUpdateRdbInfoXsync][rdb more recent] gtidCont:{} < rdbGtidSet:{}", gtidCont, rdbGtidSet);
				return UPDATE_RDB_RESULT.RDB_MORE_RECENT;
			}

			logger.info("[checkReplIdAndUpdateRdbInfoXsync][check ok] gtidCont:{} == rdbGtidSet:{}", gtidCont, rdbGtidSet);

			updateRdbInfo(metaDup, rdbFile, type, eofType, indexedOffsetBacklog, rdbGtidExecuted.toString(), ReplStage.ReplProto.XSYNC);
			saveMeta(metaDup);

			return result;
		}
	}

	@Override
	public GtidCmdFilter generateGtidCmdFilter() {
		return this;
	}

	private void clearRdb(ReplicationStoreMeta metaDup) {
		metaDup.setRdbFile(null);
		metaDup.setRdbEofMark(null);
		metaDup.setRdbFileSize(0);
		metaDup.setRdbLastOffset(null);
		metaDup.setRdbGtidSet(null);
		metaDup.setRdbContiguousBacklogOffset(null);
		metaDup.setRdbReplProto(null);
	}

	private void clearRordb(ReplicationStoreMeta metaDup) {
		metaDup.setRordbFile(null);
		metaDup.setRordbEofMark(null);
		metaDup.setRordbFileSize(0);
		metaDup.setRordbLastOffset(null);
		metaDup.setRordbGtidSet(null);
		metaDup.setRordbContiguousBacklogOffset(null);
		metaDup.setRordbReplProto(null);
	}

}
