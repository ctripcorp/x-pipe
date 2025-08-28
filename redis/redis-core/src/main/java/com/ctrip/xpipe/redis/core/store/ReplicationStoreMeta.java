package com.ctrip.xpipe.redis.core.store;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;

//TODO extend ReplicationStoreMetaCommon when V1 obselete
@SuppressWarnings("serial")
public class ReplicationStoreMeta extends ReplicationStoreMetaV1 implements Serializable{
	private ReplStage prevReplStage;
	private ReplStage curReplStage;

	// next byte after RDB
	private Long rdbContiguousBacklogOffset;
	private Long rordbContiguousBacklogOffset;
	private ReplStage.ReplProto rdbReplProto;
	private ReplStage.ReplProto rordbReplProto;

	public ReplicationStoreMeta() {

	}

	public ReplicationStoreMeta(ReplicationStoreMeta proto) {
		this.masterAddress = proto.masterAddress;
		this.beginOffset = proto.beginOffset;
		
		this.replId = proto.replId;
		this.replId2 = proto.replId2;
		this.secondReplIdOffset = proto.secondReplIdOffset;
		
		this.rdbFile = proto.rdbFile;
		this.rdbLastOffset = proto.rdbLastOffset;
		this.rdbFileSize = proto.rdbFileSize;
		this.rdbEofMark = proto.rdbEofMark;
		this.rdbGtidSet = proto.rdbGtidSet;

		this.rordbFile = proto.rordbFile;
		this.rordbLastOffset = proto.rordbLastOffset;
		this.rordbFileSize = proto.rordbFileSize;
		this.rordbEofMark = proto.rordbEofMark;
		this.rordbGtidSet = proto.rordbGtidSet;
		
		this.cmdFilePrefix = proto.cmdFilePrefix;
		this.keeperState = proto.keeperState;
		this.keeperRunid = proto.keeperRunid;

		this.prevReplStage = proto.prevReplStage == null ? null : new ReplStage(proto.prevReplStage);
		this.curReplStage = proto.curReplStage == null ? null : new ReplStage(proto.curReplStage);

		this.rdbContiguousBacklogOffset = proto.rdbContiguousBacklogOffset;
		this.rordbContiguousBacklogOffset = proto.rordbContiguousBacklogOffset;
		this.rdbReplProto = proto.rdbReplProto;
		this.rordbReplProto = proto.rordbReplProto;
	}
	
	public ReplStage getCurReplStage() {
		return curReplStage;
	}

	public void setPrevReplStage(ReplStage prevReplStage) {
		this.prevReplStage = prevReplStage;
	}

	public ReplStage getPrevReplStage() {
		return prevReplStage;
	}

	public void setCurReplStage(ReplStage curReplStage) {
		this.curReplStage = curReplStage;
	}

	public Long getRdbContiguousBacklogOffset() {
		return rdbContiguousBacklogOffset;
	}

	public void setRdbContiguousBacklogOffset(Long rdbContiguousBacklogOffset) {
		this.rdbContiguousBacklogOffset = rdbContiguousBacklogOffset;
	}

	public Long getRordbContiguousBacklogOffset() {
		return rordbContiguousBacklogOffset;
	}

	public void setRordbContiguousBacklogOffset(Long rordbContiguousBacklogOffset) {
		this.rordbContiguousBacklogOffset = rordbContiguousBacklogOffset;
	}

	public ReplStage.ReplProto getRdbReplProto() {
		return rdbReplProto;
	}
	public void setRdbReplProto(ReplStage.ReplProto rdbReplProto) {
		this.rdbReplProto = rdbReplProto;
	}

	public ReplStage.ReplProto getRordbReplProto() {
		return rordbReplProto;
	}
	public void setRordbReplProto(ReplStage.ReplProto rordbReplProto) {
		this.rordbReplProto = rordbReplProto;
	}

	public ReplicationStoreMetaV1 toV1() {
		if (this.prevReplStage == null && (this.curReplStage == null ||
				(this.curReplStage.getProto() == ReplStage.ReplProto.PSYNC && this.curReplStage.getBegOffsetBacklog() == 0)) &&
				(this.rdbReplProto == null || this.rdbReplProto == ReplStage.ReplProto.PSYNC) &&
				(this.rordbReplProto == null || this.rordbReplProto == ReplStage.ReplProto.PSYNC)
		) {
			ReplicationStoreMetaV1 v1Meta = new ReplicationStoreMetaV1();

			v1Meta.setMasterAddress(this.masterAddress);
			v1Meta.setRdbFile(this.rdbFile);
			v1Meta.setRdbFileSize(this.rdbFileSize);
			v1Meta.setRdbEofMark(this.rdbEofMark);
			v1Meta.setRdbGtidSet(this.rdbGtidSet);
			v1Meta.setRordbFile(this.rordbFile);
			v1Meta.setRordbFileSize(this.rordbFileSize);
			v1Meta.setRordbEofMark(this.rordbEofMark);
			v1Meta.setRordbGtidSet(this.rordbGtidSet);
			v1Meta.setCmdFilePrefix(this.cmdFilePrefix);
			v1Meta.setKeeperState(this.keeperState);
			v1Meta.setKeeperRunid(this.keeperRunid);

			if (this.curReplStage != null) {
				v1Meta.setBeginOffset(this.curReplStage.getBegOffsetRepl());
				v1Meta.setReplId(this.curReplStage.getReplId());
				v1Meta.setReplId2(this.curReplStage.getReplId2());
				v1Meta.setSecondReplIdOffset(this.curReplStage.getSecondReplIdOffset());
				Long rdbLastOffset = this.rdbContiguousBacklogOffset == null ? null : this.curReplStage.backlogOffset2ReplOffset(this.rdbContiguousBacklogOffset) - 1;
				Long rordbLastOffset = this.rordbContiguousBacklogOffset == null ? null : this.curReplStage.backlogOffset2ReplOffset(this.rordbContiguousBacklogOffset) - 1;
				v1Meta.setRdbLastOffset(rdbLastOffset);
				v1Meta.setRordbLastOffset(rordbLastOffset);
			} else {
				//TODO remove when v1 obseletes
				v1Meta.setBeginOffset(this.beginOffset);
				v1Meta.setReplId(this.replId);
				v1Meta.setReplId2(this.replId2);
				v1Meta.setSecondReplIdOffset(this.secondReplIdOffset);
				v1Meta.setRdbLastOffset(this.rdbLastOffset);
				v1Meta.setRordbLastOffset(this.rordbLastOffset);
			}

			return v1Meta;
		} else {
			// Not compatible with v1 meta.
			return null;
		}
	}

	public ReplicationStoreMeta fromV1(ReplicationStoreMetaV1 v1Meta) {
		this.masterAddress = v1Meta.getMasterAddress();

		this.rdbFile = v1Meta.getRdbFile();
		this.rdbFileSize = v1Meta.getRdbFileSize();
		this.rdbEofMark = v1Meta.getRdbEofMark();
		this.rdbGtidSet = v1Meta.getRdbGtidSet();

		this.rordbFile = v1Meta.getRordbFile();
		this.rordbFileSize = v1Meta.getRordbFileSize();
		this.rordbEofMark = v1Meta.getRordbEofMark();
		this.rordbGtidSet = v1Meta.getRordbGtidSet();

		this.cmdFilePrefix = v1Meta.getCmdFilePrefix();
		this.keeperState = v1Meta.getKeeperState();
		this.keeperRunid = v1Meta.getKeeperRunid();

		if (v1Meta.getRdbLastOffset() != null) {
			this.rdbContiguousBacklogOffset = v1Meta.getRdbLastOffset() + 1 - v1Meta.getBeginOffset() + ReplicationStoreMeta.DEFAULT_END_OFFSET;
		}

		if (v1Meta.getRordbLastOffset() != null) {
			this.rordbContiguousBacklogOffset = v1Meta.getRordbLastOffset() + 1 - v1Meta.getBeginOffset() + ReplicationStoreMeta.DEFAULT_END_OFFSET;
		}

		ReplStage replStage = new ReplStage(v1Meta.getReplId(), v1Meta.getBeginOffset(), ReplicationStoreMeta.DEFAULT_END_OFFSET);
		replStage.setReplId2(v1Meta.getReplId2());
		replStage.setSecondReplIdOffset(v1Meta.getSecondReplIdOffset());

		this.prevReplStage = null;
		this.curReplStage = replStage;

		this.beginOffset = null;
		this.replId = null;
		this.replId2 = null;
		this.secondReplIdOffset = null;
		this.rdbLastOffset = null;
		this.rordbLastOffset = null;

		if (v1Meta.rdbFile != null) {
			this.rdbReplProto = ReplStage.ReplProto.PSYNC;
		}
		if (v1Meta.rordbFile != null) {
			this.rordbReplProto = ReplStage.ReplProto.PSYNC;
		}

		return this;
	}

	@Override
	public String toString() {
		return "ReplicationStoreMeta{" +
				"masterAddress=" + masterAddress +
				", beginOffset=" + beginOffset +
				", replId='" + replId + '\'' +
				", replId2='" + replId2 + '\'' +
				", secondReplIdOffset=" + secondReplIdOffset +
				", rdbFile='" + rdbFile + '\'' +
				", rdbLastOffset=" + rdbLastOffset +
				", rdbFileSize=" + rdbFileSize +
				", rdbEofMark='" + rdbEofMark + '\'' +
				", rdbGtidSet='" + rdbGtidSet + '\'' +
				", rdbContiguousBacklogOffset=" + rdbContiguousBacklogOffset +
				", rdbReplProto=" + rdbReplProto +
				", rordbFile='" + rordbFile + '\'' +
				", rordbLastOffset=" + rordbLastOffset +
				", rordbFileSize=" + rordbFileSize +
				", rordbEofMark='" + rordbEofMark + '\'' +
				", rordbGtidSet='" + rordbGtidSet + '\'' +
				", rordbContiguousBacklogOffset=" + rordbContiguousBacklogOffset +
				", rordbReplProto=" + rordbReplProto +
				", cmdFilePrefix='" + cmdFilePrefix + '\'' +
				", keeperState=" + keeperState +
				", keeperRunid='" + keeperRunid + '\'' +
				", curReplStage=" + curReplStage +
				", prevReplStage=" + prevReplStage +
				'}';
	}

	@Override
	public boolean equals(Object obj) {
		return EqualsBuilder.reflectionEquals(this, obj);
	}
	
	@Override
	public int hashCode() {
		return HashCodeBuilder.reflectionHashCode(this, false);
	}
}
