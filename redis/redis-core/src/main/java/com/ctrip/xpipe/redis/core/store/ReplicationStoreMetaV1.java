package com.ctrip.xpipe.redis.core.store;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

@SuppressWarnings("serial")
public class ReplicationStoreMetaV1 extends ReplicationStoreMetaCommon implements Serializable{

	protected Long beginOffset = DEFAULT_BEGIN_OFFSET;

	protected String replId;
	protected String replId2;
	protected Long secondReplIdOffset = DEFAULT_SECOND_REPLID_OFFSET;

	protected Long rdbLastOffset;
	protected Long rordbLastOffset;

	public ReplicationStoreMetaV1() {

	}

	public ReplicationStoreMetaV1(ReplicationStoreMetaV1 proto) {
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
	}
	
	public Long getBeginOffset() {
		return beginOffset;
	}

	public void setBeginOffset(Long beginOffset) {
		this.beginOffset = beginOffset;
	}


	public String getReplId() {
		return replId;
	}

	public void setReplId(String replId) {
		this.replId = replId;
	}

	public Long getRdbLastOffset() {
		return rdbLastOffset;
	}

	public void setRdbLastOffset(Long rdbLastOffset) {
		this.rdbLastOffset = rdbLastOffset;
	}

	public String getReplId2() {
		return replId2;
	}

	public void setReplId2(String replId2) {
		this.replId2 = replId2;
	}

	public Long getSecondReplIdOffset() {
		return secondReplIdOffset;
	}

	public void setSecondReplIdOffset(Long secondReplIdOffset) {
		this.secondReplIdOffset = secondReplIdOffset;
	}

	public Long getRordbLastOffset() {
		return rordbLastOffset;
	}

	public void setRordbLastOffset(Long rordbLastOffset) {
		this.rordbLastOffset = rordbLastOffset;
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
				", rordbFile='" + rordbFile + '\'' +
				", rordbLastOffset=" + rordbLastOffset +
				", rordbFileSize=" + rordbFileSize +
				", rordbEofMark='" + rordbEofMark + '\'' +
				", rordbGtidSet='" + rordbGtidSet + '\'' +
				", cmdFilePrefix='" + cmdFilePrefix + '\'' +
				", keeperState=" + keeperState +
				", keeperRunid='" + keeperRunid + '\'' +
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
